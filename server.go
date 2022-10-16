package raft

import (
	"context"
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

var sqliteDB *gorm.DB
var serverInstance *Server
var sugar *zap.SugaredLogger

// user sugar logger
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
	rpcClients := make(map[string]interface{}, 0)
	for _, peer := range peers {
		client, err := rpc.DialHTTP("tcp", peer)
		if err != nil {
			log.Fatal("dialing:", err)
		}
		rpcClients[peer] = client
	}
	nextIndex := make([]int, len(peers))
	matchIndex := make([]int, len(peers))
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
		logs:         make([]logEntry.Entry, 0),
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

type Server struct {
	serverId        string
	leaderId        string
	peers           []string
	rpcClients      map[string]interface{}
	state           string
	currentTerm     int
	votedFor        string
	lastCommitIndex int
	lastApplied     int
	// next log entry to send to servers
	nextIndex []int
	// index of the highest log entry known to be replicated on server
	matchIndex   []int
	logs         []logEntry.Entry
	stateMachine stateMachine.StateMachine
	serverDb     *gorm.DB
	serverTicker *ServerTicker
}

var mu = &sync.Mutex{}

type electionContext struct {
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
			sugar.Infof("election started by %s at %v", s.serverId, t)
			mu.Lock()
			ctx, cancel := context.WithCancel(context.Background())
			mu.Unlock()

			electionContextInst := &electionContext{
				ctx:    ctx,
				cancel: cancel,
			}

			// start election here
			// context can be used to stop the ongoiong election if need be
			go s.LeaderElection(electionContextInst)
			interval := s.serverTicker.tickerInterval
			s.serverTicker.ticker.Reset(time.Duration(util.GetRandomInt(interval, 2*interval) * int(time.Millisecond)))

		}
	}
}

// Sends out RequestVote RPCs to other servers. Requests may timeout here, keep retrying. On failure go back to previous step and start all over again.
// On receving majority, the candidate becomes a leader.
// On receving heartbeat from some other newly elected leader, the candidate becomes a follower.
func (s *Server) LeaderElection(electionContextInst *electionContext) error {
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
						Term:     s.currentTerm,
						ServerId: s.serverId,
					}
					response := &types.ResponseVoteRPC{}
					err = util.RPCWithRetry(client, "Server.RequestVoteRPC", request, response, config.GetRetryRPCLimit())
					if err != nil {
						sugar.Warnw("request vote RPC failed after retries", "rpcClient", client, "request", request, "response", response)
						response = &types.ResponseVoteRPC{
							Voted: false,
						}
					}
					responseChan <- response
				}()

			}
			wg.Wait()

			voteCnt := 0
			isTermOutdated := false
			for resp := range responseChan {
				if resp.Voted {
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
			}

		}
	}
}

func (s *Server) RequestVoteRPC(request *types.RequestVoteRPC, response *types.ResponseVoteRPC) {
	// 	Notify that the requesting candidate should step back.
	if s.currentTerm > request.Term {
		response = &types.ResponseVoteRPC{
			Voted:         false,
			OutdatedTerm:  true,
			CurrentLeader: s.leaderId,
		}
		return
	}
	// 	do not vote for the requesting candidate
	if s.currentTerm == request.Term {
		response = &types.ResponseVoteRPC{
			Voted: false,
		}
		return
	}
	// 	vote for the requesting candidate
	if s.currentTerm < request.Term {
		response = &types.ResponseVoteRPC{
			Voted: true,
		}
		return
	}
}

func (s *Server) AppendEntryRPC(request *types.RequestAppendEntryRPC, response *types.ResponseAppendEntryRPC) {

	/// report outdated term in the request
	if s.currentTerm > request.Term {
		response = &types.ResponseAppendEntryRPC{
			ServerId:      s.serverId,
			Success:       true,
			CurrentLeader: s.leaderId,
			OutdatedTerm:  true,
		}
	}

	if s.currentTerm == request.Term {
		if len(request.Entries) == 0 {

		}
	}

}

func (s *Server) heartBeatTimerReset() {

}
