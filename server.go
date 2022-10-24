package raft

import (
	"context"
	"errors"
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"

	"github.com/adityeah8969/raft/config"
	"github.com/adityeah8969/raft/types"
	"github.com/adityeah8969/raft/types/constants"
	"github.com/adityeah8969/raft/types/logEntry"
	"github.com/adityeah8969/raft/types/logger"
	serverdb "github.com/adityeah8969/raft/types/serverDb"
	"github.com/adityeah8969/raft/types/stateMachine"
	"github.com/adityeah8969/raft/util"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

var serverInstance *Server
var sugar *zap.SugaredLogger
var electionContextInst *processContext
var leaderContextInst *processContext

func init() {
	serverDb, err := serverdb.GetServerDbInstance()
	if err != nil {
		log.Fatal("initializing server db: ", err)
	}
	err = serverDb.AutoMigrate(&Vote{})
	if err != nil {
		log.Fatal("auto-migrating the server db: ", err)
	}
	stateMcInst, err := stateMachine.GetStateMachine()
	if err != nil {
		log.Fatal("auto-migrating the server db: ", err)
	}
	peers := config.GetPeers()
	rpcClients := make(map[string]interface{}, len(peers))
	nextIndex := make(map[string]*logEntry.LogEntry, len(peers))
	matchIndex := make(map[string]*logEntry.LogEntry, len(peers))
	for _, peer := range peers {
		client, err := rpc.DialHTTP("tcp", peer)
		if err != nil {
			log.Fatal("dialing:", err)
		}
		rpcClients[peer] = client
		nextIndex[peer] = nil
		matchIndex[peer] = nil
	}

	serverTicker := &ServerTicker{
		ticker:         time.NewTicker(time.Second),
		done:           make(chan bool),
		tickerInterval: config.GetTickerIntervalInMillisecond(),
	}
	serverInstance = &Server{
		serverId:     config.GetServerId(),
		peers:        peers,
		state:        string(constants.Follower),
		serverDb:     serverDb,
		stateMachine: stateMcInst,
		nextIndex:    nextIndex,
		matchIndex:   matchIndex,
		logs:         make([]logEntry.LogEntry, 0),
		rpcClients:   rpcClients,
		serverTicker: serverTicker,
	}
	sugar = logger.GetLogger()
}

type ServerTicker struct {
	ticker         *time.Ticker
	done           chan bool
	tickerInterval int
}

type Vote struct {
	gorm.Model
	term     int
	votedFor string
}

// see how the individual fields are getting impacted, at every imp logical step
type Server struct {
	serverId          string
	leaderId          string
	peers             []string
	rpcClients        map[string]interface{}
	state             string
	currentTerm       int
	votedFor          string
	lastComittedIndex int
	lastAppliedIndex  int
	// next log entry to send to servers
	nextIndex map[string]*logEntry.LogEntry
	// index of the highest log entry known to be replicated on server
	matchIndex   map[string]*logEntry.LogEntry
	logs         []logEntry.LogEntry
	stateMachine stateMachine.StateMachine
	serverDb     *gorm.DB
	serverTicker *ServerTicker
}

var electionCtxMu = &sync.Mutex{}
var leaderCtxMu = &sync.Mutex{}

type processContext struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func StartServing() error {
	rpc.Register(serverInstance)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", ":8089")
	if err != nil {
		return err
	}
	go serverInstance.startTicker()
	err = http.Serve(l, nil)
	return err
}

// heartbeat (AppendEntry messages in general) stopping the leaderELection process
// heartbeat messages restarting timeout tickers
func (s *Server) startTicker() {
	defer s.serverTicker.ticker.Stop()
	for {
		select {
		case <-s.serverTicker.done:
			sugar.Infof("%s done ticking!!", s.serverId)
			return
		case t := <-s.serverTicker.ticker.C:

			// Ignore ticker in case the server itself is the leader
			if s.leaderId == s.serverId {
				s.serverTicker.ticker.Reset(util.GetRandomTickerDuration(s.serverTicker.tickerInterval))
				continue
			}

			sugar.Infof("election started by %s at %v", s.serverId, t)
			// separate updating electionContextInst into a method
			electionCtxMu.Lock()
			ctx, cancel := context.WithCancel(context.Background())
			electionContextInst = &processContext{
				ctx:    ctx,
				cancel: cancel,
			}
			electionCtxMu.Unlock()

			// start election here
			// context can be used to stop the ongoing election if need be
			go s.LeaderElection()
			s.serverTicker.ticker.Reset(util.GetRandomTickerDuration(s.serverTicker.tickerInterval))

			// separate it out
			electionCtxMu.Lock()
			electionContextInst = nil
			electionCtxMu.Unlock()
		}
	}
}

// Sends out RequestVote RPCs to other servers. Requests may timeout here, keep retrying. On failure go back to previous step and start all over again.
// On receving majority, the candidate becomes a leader.
// On receving heartbeat from some other newly elected leader, the candidate becomes a follower.
func (s *Server) LeaderElection() error {
	for {
		select {
		case <-electionContextInst.ctx.Done():
			// log here
			return nil
		default:

			// # Candidate incerements its term.
			// # Votes for itself. (persists)
			s.currentTerm++
			vote := &Vote{
				votedFor: s.serverId,
				term:     s.currentTerm,
			}
			err := s.serverDb.Model(&Vote{}).Save(vote).Error
			if err != nil {
				return err
			}
			s.votedFor = s.serverId

			var wg sync.WaitGroup
			wg.Add(len(s.peers))

			responseChan := make(chan *types.ResponseVoteRPC, len(s.peers))

			for i := range s.rpcClients {
				go func() {
					defer wg.Done()
					client := s.rpcClients[i].(*rpc.Client)
					request := &types.RequestVoteRPC{
						Term:        s.currentTerm,
						CandidateId: s.serverId,
					}
					response := &types.ResponseVoteRPC{}
					err = util.RPCWithRetry(client, "Server.RequestVoteRPC", request, response, config.GetRetryRPCLimit())
					if err != nil {
						sugar.Warnw("request vote RPC failed after retries", "rpcClient", client, "request", request, "response", response)
						response = &types.ResponseVoteRPC{
							VotedGranted: false,
						}
					}
					responseChan <- response
				}()
			}
			wg.Wait()

			voteCnt := 0
			isTermOutdated := false
			for resp := range responseChan {
				if resp.VotedGranted {
					voteCnt++
					continue
				}
				if resp.OutdatedTerm {
					s.state = string(constants.Follower)
					s.leaderId = resp.CurrentLeader
					isTermOutdated = true
					break
				}
			}
			close(responseChan)
			if isTermOutdated {
				sugar.Infof("%s server had an outdated term as a candidate", s.serverId)
				return nil
			}

			if voteCnt >= int(math.Ceil(1.0*float64(len(s.peers)/2))) {
				s.leaderId = s.serverId
				s.state = string(constants.Leader)

				leaderCtxMu.Lock()
				ctx, cancel := context.WithCancel(context.Background())
				leaderContextInst = &processContext{
					ctx:    ctx,
					cancel: cancel,
				}
				leaderCtxMu.Unlock()

				go s.startLeading()
			}

		}
	}
}

func (s *Server) startLeading() error {
	for {
		select {
		case <-leaderContextInst.ctx.Done():
			// log here
			return nil
		default:

			// periodic heartbeat
			// AppendEntryCalls
			// Receiver for relayed appendEntry calls
			// Receiver for relayed getEntry calls

		}
	}
}

func (s *Server) RequestVoteRPC(req *types.RequestVoteRPC, res *types.ResponseVoteRPC) {
	// 	Notify that the requesting candidate should step back.
	if s.currentTerm > req.Term {
		res = &types.ResponseVoteRPC{
			VotedGranted:  false,
			OutdatedTerm:  true,
			CurrentLeader: s.leaderId,
		}
		return
	}
	// 	Do not vote for the requesting candidate, if already voted
	if s.currentTerm == req.Term && s.votedFor != "" {
		res = &types.ResponseVoteRPC{
			VotedGranted: false,
		}
		return
	}
	var lastServerLog logEntry.LogEntry
	if len(s.logs) > 0 {
		lastServerLog = s.logs[len(s.logs)-1]
		// Deny, if the candidates logs are not updated enough
		if lastServerLog.Term > req.LastLogTerm || (lastServerLog.Term == req.LastLogTerm && lastServerLog.Index > req.LastLogIndex) {
			res = &types.ResponseVoteRPC{
				VotedGranted: false,
			}
			return
		}
	}

	// Vote for the requesting candidate
	// Add persistence logic [TODO]
	res = &types.ResponseVoteRPC{
		VotedGranted: true,
	}
}

func (s *Server) AppendEntryRPC(req *types.RequestAppendEntryRPC, res *types.ResponseAppendEntryRPC) {

	// report outdated term in the request
	if s.currentTerm > req.LeaderTerm {
		res = &types.ResponseAppendEntryRPC{
			ServerId:      s.serverId,
			Success:       false,
			OutdatedTerm:  true,
			CurrentLeader: s.leaderId,
		}
		return
	}

	if s.currentTerm < req.LeaderTerm {
		// revert to being a follower
		// consider locking here
		if s.state != string(constants.Follower) {
			s.state = string(constants.Follower)
		}
		s.currentTerm = req.LeaderTerm
		s.leaderId = req.LeaderId
		s.votedFor = ""
	}

	if len(req.Entries) == 0 {
		go s.heartBeatTimerReset()
		return
	}

	// return failure
	ok := s.isPreviousEntryPresent(&req.PrevEntry)
	if !ok {
		res = &types.ResponseAppendEntryRPC{
			ServerId:      s.serverId,
			Success:       false,
			OutdatedTerm:  false,
			CurrentLeader: s.leaderId,
		}
		return
	}

	// commit
	for _, entry := range req.Entries {
		serverLog := logEntry.LogEntry{
			Term:  entry.Term,
			Index: entry.Index,
			Entry: entry.Index,
		}
		s.logs = append(s.logs, serverLog)
	}

	s.lastComittedIndex = int(math.Min(float64(req.LastCommittedEntryInLeader.Index), float64(len(s.logs))))

	// apply entries
	err := s.applyEntriesToStateMC(&req.LastCommittedEntryInLeader)
	if err != nil {
		// log here
		res = &types.ResponseAppendEntryRPC{
			ServerId:      s.serverId,
			Success:       false,
			OutdatedTerm:  false,
			CurrentLeader: s.leaderId,
		}
	}

	// return success
	res = &types.ResponseAppendEntryRPC{
		ServerId:      s.serverId,
		Success:       true,
		OutdatedTerm:  false,
		CurrentLeader: s.leaderId,
	}
}

func (s *Server) heartBeatTimerReset() {
	electionCtxMu.Lock()
	defer electionCtxMu.Unlock()
	if electionContextInst != nil {
		electionContextInst.cancel()
		electionContextInst = nil
	}
	s.serverTicker.ticker.Reset(util.GetRandomTickerDuration(s.serverTicker.tickerInterval))
}

func (s *Server) isPreviousEntryPresent(prevEntry *logEntry.LogEntry) bool {

	entryFound := false
	index := -1

	for i := len(s.logs) - 1; i >= 0; i-- {
		if s.logs[i].Term < prevEntry.Term {
			return false
		}
		if s.logs[i] == *prevEntry {
			entryFound = true
			index = i
		}
	}

	if entryFound {
		s.logs = s.logs[:index+1]
	}

	return false
}

func (s *Server) applyEntriesToStateMC(lastComittedEntryInLeader *logEntry.LogEntry) error {

	// can make this a separate method
	batchEntries := make([]logEntry.LogEntry, 0)

	index := -1
	for i := len(s.logs) - 1; i >= 0; i-- {
		if s.logs[i] == *lastComittedEntryInLeader {
			index = i
			break
		}
	}

	if index == -1 {
		return errors.New("last comitted entry in leader not found in the follower")
	}

	for ; index >= 0; index-- {
		if s.logs[index].Index == s.lastAppliedIndex {
			break
		}
		batchEntries = append(batchEntries, s.logs[index])
	}

	batchEntries = util.GetReversedSlice(batchEntries)
	return s.stateMachine.Apply(batchEntries)
}

/*

IMP

redirect to leader calls

*/

// server
// lastApplied .... lastComitted
// nextIndex[], matchIndex[]
// [lastApplied, lastComittedInLeader]

/*

lastComittedEntryLeader

----

Server

	lastComittedIndex int

	lastAppliedIndex  int

	nextIndex 	 []int // next log entry to send to servers
	matchIndex   []int // index of the highest log entry known to be replicated on server

----

Create a log of entries to be comitted,

[server.lastAppliedIndex+1, lastComittedEntryInLeader]

*/
