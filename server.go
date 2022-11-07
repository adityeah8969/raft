package raft

import (
	"context"
	"errors"
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

type ServerTicker struct {
	ticker *time.Ticker
	done   chan bool
}

type Vote struct {
	gorm.Model
	term     int
	votedFor string
}

type processContext struct {
	ctx    context.Context
	cancel context.CancelFunc
}

// see how the individual fields are getting impacted, at every imp logical step. TODO
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
	nextIndex map[string]int
	// index of the highest log entry known to be replicated on server
	matchIndex   map[string]int
	logs         []logEntry.LogEntry
	stateMachine stateMachine.StateMachine
	serverDb     *gorm.DB
	serverTicker *ServerTicker
}

var serverInstance *Server
var sugar *zap.SugaredLogger

var followerContextInst *processContext
var candidateContextInst *processContext
var leaderContextInst *processContext

var candidateCtxMu = &sync.Mutex{}
var leaderCtxMu = &sync.Mutex{}
var followerCtxMu = &sync.Mutex{}

func init() {
	serverDb, err := serverdb.GetServerDbInstance()
	if err != nil {
		sugar.Fatalf("initializing server db: ", err)
	}
	err = serverDb.AutoMigrate(&Vote{})
	if err != nil {
		sugar.Fatalf("auto-migrating the server db: ", err)
	}
	stateMcInst, err := stateMachine.GetStateMachine()
	if err != nil {
		sugar.Fatalf("auto-migrating the server db: ", err)
	}
	// initializing the logs with a dummy entry, index > 0 will be considered as valid logs
	logs := []logEntry.LogEntry{}
	peers := config.GetPeers()
	rpcClients := make(map[string]interface{}, len(peers))
	nextIndex := make(map[string]int, len(peers))
	matchIndex := make(map[string]int, len(peers))
	for _, peer := range peers {
		client, err := rpc.DialHTTP("tcp", peer)
		if err != nil {
			sugar.Fatalf("dialing:", err)
		}
		rpcClients[peer] = client
		nextIndex[peer] = 1
		matchIndex[peer] = 0
	}
	serverTicker := &ServerTicker{
		ticker: time.NewTicker(time.Duration(config.GetTickerIntervalInMillisecond()) * time.Millisecond),
		done:   make(chan bool),
	}
	serverInstance = &Server{
		serverId:          config.GetServerId(),
		peers:             peers,
		state:             string(constants.Follower),
		serverDb:          serverDb,
		stateMachine:      stateMcInst,
		nextIndex:         nextIndex,
		matchIndex:        matchIndex,
		logs:              logs,
		rpcClients:        rpcClients,
		serverTicker:      serverTicker,
		currentTerm:       0,
		votedFor:          "",
		lastComittedIndex: 0,
		lastAppliedIndex:  0,
	}
	sugar = logger.GetLogger()
	followerContextInst = &processContext{}
	candidateContextInst = &processContext{}
	leaderContextInst = &processContext{}
}

func StartServing() error {
	rpc.Register(serverInstance)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", ":8089")
	if err != nil {
		return err
	}
	go serverInstance.startFollowing()
	err = http.Serve(l, nil)
	return err
}

func updateProcessContext(procCtx *processContext, updatedCtx *processContext, ctxMu *sync.Mutex) {
	ctxMu.Lock()
	defer ctxMu.Unlock()
	if procCtx.cancel != nil {
		procCtx.cancel()
	}
	procCtx.ctx = updatedCtx.ctx
	procCtx.cancel = updatedCtx.cancel
}

func (s *Server) startFollowing() {
	s.state = string(constants.Follower)
	// create a util for the following
	ctx, cancel := context.WithCancel(context.Background())
	updatedCtx := &processContext{
		ctx:    ctx,
		cancel: cancel,
	}
	updateProcessContext(followerContextInst, updatedCtx, followerCtxMu)
	ctx, _ = context.WithCancel(followerContextInst.ctx)
	go s.startTicker(ctx)
}

func (s *Server) stopFollowing() {
	updateProcessContext(followerContextInst, &processContext{}, followerCtxMu)
}

func (s *Server) startTicker(ctx context.Context) {
	defer s.serverTicker.ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			sugar.Infof("stopping ticker for server %s", s.serverId)
			return
		case t := <-s.serverTicker.ticker.C:
			sugar.Infof("election contest started by %s at %v", s.serverId, t)
			go s.startContesting()
			s.stopFollowing()
		}
	}
}

func (s *Server) resetTicker() {
	// consider locking here
	s.serverTicker.ticker.Reset(util.GetRandomTickerDuration(config.GetTickerIntervalInMillisecond()))
}

func (s *Server) stopContesting() {
	updateProcessContext(candidateContextInst, &processContext{}, followerCtxMu)
}

// TODO: check if the election timer is needed
// TODO: If yes, start new election whenever there is a timout and do not forget to reset everytime there is an election.
func (s *Server) startContesting() error {
	for {
		select {
		case <-candidateContextInst.ctx.Done():
			sugar.Infof("stopping election contest for server %s", s.serverId)
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

			for k, _ := range s.rpcClients {
				go func() {
					defer wg.Done()
					client := s.rpcClients[k].(*rpc.Client)
					request := &types.RequestVoteRPC{
						Term:        s.currentTerm,
						CandidateId: s.serverId,
					}
					response := &types.ResponseVoteRPC{}
					// TODO. Get time from config
					resp, err := util.RPCWithRetry(client, "Server.RequestVoteRPC", request, response, config.GetRetryRPCLimit(), 2)
					if err != nil {
						sugar.Warnw("request vote RPC failed after retries", "candidate", s.serverId, "rpcClient", client, "request", request, "response", response)
						response = &types.ResponseVoteRPC{
							VoteGranted: false,
						}
					}
					response, ok := resp.(*types.ResponseVoteRPC)
					if !ok {
						sugar.Debugw("Unable to cast response into ResponseVoteRPC", "response", resp)
						// TODO. Check if there is any need of populating fields
						response = &types.ResponseVoteRPC{}
					}
					responseChan <- response
				}()
			}
			wg.Wait()

			voteCnt := 0
			isTermOutdated := false
			for resp := range responseChan {
				if resp.VoteGranted {
					voteCnt++
					continue
				}
				if resp.OutdatedTerm {
					// check if locking is required here. TODO
					s.state = string(constants.Follower)
					s.leaderId = resp.CurrentLeader
					s.currentTerm = resp.Term
					isTermOutdated = true
					break
				}
			}
			close(responseChan)
			if isTermOutdated {
				sugar.Infof("%s server had an outdated term as a candidate", s.serverId)
				go s.startFollowing()
				s.stopContesting()
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
				s.stopContesting()
				go s.startLeading()
				return nil
			}

		}
	}
}

func (s *Server) stopLeading() {
	updateProcessContext(leaderContextInst, &processContext{}, leaderCtxMu)
}

func (s *Server) sendHeartBeats(ctx context.Context) {
	heartBeatTicker := ServerTicker{
		ticker: time.NewTicker(time.Duration(config.GetTickerIntervalInMillisecond()) * time.Millisecond),
		done:   make(chan bool),
	}
	defer heartBeatTicker.ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			sugar.Infof("%s started leading", s.serverId)
			return
		case <-heartBeatTicker.ticker.C:

			var wg sync.WaitGroup
			wg.Add(len(s.peers))

			responseChan := make(chan *types.ResponseAppendEntryRPC, len(s.peers))
			for i := range s.rpcClients {
				go func() {
					defer wg.Done()
					client := s.rpcClients[i].(*rpc.Client)
					request := &types.RequestAppendEntryRPC{
						Term:     s.currentTerm,
						LeaderId: s.serverId,
						// Fill the last comiitted entry here. TODO
					}
					response := &types.ResponseAppendEntryRPC{}
					// TODO, fix this response
					_, err := util.RPCWithRetry(client, "Server.AppendEntryRPC", request, response, config.GetRetryRPCLimit(), 2)
					if err != nil {
						sugar.Warnw("append entry RPC failed after retries", "leader", s.serverId, "rpcClient", client, "request", request, "response", response)
						response = &types.ResponseAppendEntryRPC{}
					}
					responseChan <- response
				}()
			}
			wg.Done()

			outdatedTerm := false
			updatedTerm := s.currentTerm
			updatedLeader := s.leaderId

			for resp := range responseChan {
				if !resp.Success && resp.OutdatedTerm {
					outdatedTerm = true
					updatedTerm = resp.Term
					updatedLeader = resp.CurrentLeader
					break
				}
			}

			if outdatedTerm {
				// check if locking is required here. TODO
				s.currentTerm = updatedTerm
				s.leaderId = updatedLeader
				go s.startFollowing()
				s.stopLeading()
			}

			// Check what should be the interval here. Put the smae in config. TODO
			heartBeatTicker.ticker.Reset(time.Duration(config.GetTickerIntervalInMillisecond()) * time.Millisecond)
		}
	}
}

func (s *Server) startLeading() error {
	sugar.Infof("%s started leading", s.serverId)

	// Re-initialize nextIndex, matchIndex

	for _, v := range s.peers {
		s.nextIndex[v] = len(s.logs)
		s.matchIndex[v] = 0
	}

	for {
		select {
		case <-leaderContextInst.ctx.Done():
			sugar.Infof("%s stopped leading", s.serverId)
			return nil
		default:
			// periodic heartbeat
			ctx, _ := context.WithCancel(leaderContextInst.ctx)
			go s.makeAppendEntryCallsConcurrently(ctx)
			s.sendHeartBeats(ctx)
		}
	}
}

func (s *Server) makeAppendEntryCallsConcurrently(ctx context.Context) {

	for {
		select {
		case <-ctx.Done():
			sugar.Infof("%s stopped making append entry calls", s.serverId)
			return
		default:
			responseChan := make(chan *types.ResponseAppendEntryRPC)
			wg := &sync.WaitGroup{}
			for k := range s.rpcClients {
				client := s.rpcClients[k].(*rpc.Client)
				nextIndex := s.nextIndex[k]
				// check for the correctness / edge cases. TODO
				prevEntryIndex := nextIndex - 1
				prevEntry := s.logs[prevEntryIndex]
				// TODO Have a lock here.
				// last comitted entry may change, also check if the locking is needed to get bulk entries
				lastComittedEntry := s.logs[s.lastComittedIndex]
				bulkEntries := make([]logEntry.LogEntry, len(s.logs)-nextIndex)
				for i := len(s.logs) - 1; i >= nextIndex; i-- {
					bulkEntries = append(bulkEntries, s.logs[i])
				}
				bulkEntries = util.GetReversedSlice(bulkEntries)
				var request *types.RequestAppendEntryRPC
				if len(s.logs)-1 >= s.nextIndex[k] {
					request = &types.RequestAppendEntryRPC{
						Term:                       s.currentTerm,
						LeaderId:                   s.serverId,
						PrevEntry:                  &prevEntry,
						Entries:                    bulkEntries,
						LastCommittedEntryInLeader: lastComittedEntry,
					}
				}
				ctx, _ = context.WithCancel(ctx)
				wg.Add(1)
				go s.makeAppendEntryCall(ctx, client, prevEntryIndex, request, responseChan, wg)
			}
			wg.Wait()
			for resp := range responseChan {
				// If successful: update nextIndex and matchIndex for the follower
				if resp.Success {
					serverId := resp.ServerId
					s.nextIndex[serverId] = int(math.Min(float64(resp.LastCommittedIndexInFollower+1), float64(len(s.logs)-1)))
					s.matchIndex[serverId] = resp.LastCommittedIndexInFollower
				}
			}
			s.setCommitIndex()
		}
	}
}

func (s *Server) setCommitIndex() {

	lastCommitIndex := s.lastComittedIndex
	finalCommitIndex := lastCommitIndex
	lastCommitIndex++

	for ; lastCommitIndex < len(s.logs)-1; lastCommitIndex++ {
		cnt := 0
		for _, v := range s.matchIndex {
			if lastCommitIndex <= v {
				cnt++
			}
		}
		if cnt > len(s.peers)/2 && s.logs[lastCommitIndex].Term == s.currentTerm {
			finalCommitIndex = lastCommitIndex
		}
	}

	s.lastComittedIndex = finalCommitIndex
}

func (s *Server) makeAppendEntryCall(ctx context.Context, client *rpc.Client, prevEntryIndex int, request *types.RequestAppendEntryRPC, responseChan chan *types.ResponseAppendEntryRPC, wg *sync.WaitGroup) error {
	defer wg.Done()
	response := &types.ResponseAppendEntryRPC{}
	// TODO: Get this from config
	timer := time.NewTimer(5 * time.Second)
	for {
		select {
		case <-ctx.Done():
			sugar.Debugw("append entry call stopped by context", "leaderId", s.serverId)
			return errors.New("append entry call stopped by context")
		case <-timer.C:
			sugar.Debugw("append entry call timed out", "leaderId", s.serverId, "client", client)
			return errors.New("append entry call timed out")
		default:
			// TODO: get this from config, see last param
			resp, err := util.RPCWithRetry(client, "Server.AppendEntryRPC", request, response, config.GetRetryRPCLimit(), 2)
			if err != nil {
				sugar.Warnw("append entry RPC calls failed", "Error", err, "leaderId", s.serverId, "rpcClient", client)
				// check if we need to populate the following. TODO
				response = &types.ResponseAppendEntryRPC{}
				responseChan <- response
				timer.Stop()
				return err
			}
			response, ok := resp.(*types.ResponseAppendEntryRPC)
			if !ok {
				sugar.Errorw("unable to cast response into ResponseAppendEntryRPC", "client", client, "request", request, "response", response)
				return errors.New("unable to cast response into ResponseAppendEntryRPC")
			}
			if !response.Success {
				if response.OutdatedTerm {
					// revert to being a follower
					// TODO. Have a lock here
					s.state = string(constants.Follower)
					s.leaderId = response.CurrentLeader
					s.currentTerm = response.Term
					go s.startFollowing()
					s.stopLeading()
					return nil
				}
				if response.PreviousEntryAbsent {
					// update prevEntry and try again
					prevEntryIndex--
					if prevEntryIndex < 0 {
						request.PrevEntry = nil
						continue
					}
					prevEntry := s.logs[prevEntryIndex]
					request.PrevEntry = &prevEntry
					continue
				}
			}
			responseChan <- response
			timer.Stop()
			return nil
		}
	}
}

func (s *Server) Set(request *types.RequestEntry) *types.ResponseEntry {

	// re-direct to leader
	if s.leaderId != s.serverId {
		client := s.rpcClients[s.leaderId].(*rpc.Client)
		response := &types.ResponseEntry{}
		err := util.RPCWithRetry(client, "Server.Set", request, response, config.GetRetryRPCLimit(), 2)
		if err != nil {
			sugar.Warnw("set entry failed after retries", "serverId", s.serverId, "leaderId", s.leaderId, "rpcClient", client, "request", request, "response", response)
			response = &types.ResponseEntry{
				Success: false,
				Err:     err,
			}
		}
		return response
	}

	newIndex := len(s.logs)

	log := logEntry.LogEntry{
		Term:  s.currentTerm,
		Index: newIndex,
		Entry: request,
	}

	// Might have to take a lock here. TODO
	s.logs = append(s.logs, log)

	// We need a way to asynchronously send the response back to the client after the entry is applied to the state machine ??
	// Just check if the entry has been applied, let it be a blocking call.
	// Get this from a config. TODO
	timer := time.NewTimer(5 * time.Second)

	for {
		select {
		case <-timer.C:
			sugar.Debugf("timing out while applying the entry", "serverId", s.serverId, "leaderId", s.leaderId, "logEntry", log)
			return &types.ResponseEntry{
				Success: false,
				Err:     errors.New("timing out while applying the entry"),
			}
		default:
			if s.lastAppliedIndex < newIndex {
				continue
			}
			timer.Stop()
			return &types.ResponseEntry{
				Success: true,
			}
		}
	}
}

func (s *Server) Get(request *types.RequestEntry) *types.ResponseEntry {
	// re-direct to leader
	if s.leaderId != s.serverId {
		client := s.rpcClients[s.leaderId].(*rpc.Client)
		response := &types.ResponseEntry{}
		err := util.RPCWithRetry(client, "Server.Get", request, response, config.GetRetryRPCLimit())
		if err != nil {
			sugar.Warnw("get entry failed after retries", "serverId", s.serverId, "leaderId", s.leaderId, "rpcClient", client, "request", request, "response", response)
			response = &types.ResponseEntry{
				Success: false,
				Err:     err,
			}
		}
		return response
	}

	log := &logEntry.LogEntry{Entry: request}
	logEntry, err := s.stateMachine.GetEntry(log)

	if err != nil {
		return &types.ResponseEntry{
			Success: false,
			Err:     err,
		}
	}

	return &types.ResponseEntry{
		Success: false,
		Err:     nil,
		Data:    logEntry,
	}
}

func (s *Server) RequestVoteRPC(req *types.RequestVoteRPC, res *types.ResponseVoteRPC) {
	// 	Notify that the requesting candidate should step back.
	if s.currentTerm > req.Term {
		res = &types.ResponseVoteRPC{
			VoteGranted:   false,
			OutdatedTerm:  true,
			CurrentLeader: s.leaderId,
		}
		return
	}
	// 	Do not vote for the requesting candidate, if already voted
	if s.currentTerm == req.Term && s.votedFor != "" {
		res = &types.ResponseVoteRPC{
			VoteGranted: false,
		}
		return
	}
	var lastServerLog logEntry.LogEntry
	if len(s.logs) > 0 {
		lastServerLog = s.logs[len(s.logs)-1]
		// Deny, if the candidates logs are not updated enough
		if lastServerLog.Term > req.LastLogTerm || (lastServerLog.Term == req.LastLogTerm && lastServerLog.Index > req.LastLogIndex) {
			res = &types.ResponseVoteRPC{
				VoteGranted: false,
			}
			return
		}
	}

	// Vote for the requesting candidate
	err := s.serverDb.Model(&Vote{}).Save(Vote{term: req.Term, votedFor: req.CandidateId}).Error
	if err != nil {
		res = &types.ResponseVoteRPC{
			VoteGranted: false,
			Err:         errors.New("db error while persisting vote"),
		}
		return
	}

	s.currentTerm = req.Term
	s.votedFor = req.CandidateId

	res = &types.ResponseVoteRPC{
		VoteGranted: true,
	}
}

func (s *Server) AppendEntryRPC(req *types.RequestAppendEntryRPC, res *types.ResponseAppendEntryRPC) {

	// report outdated term in the request
	if s.currentTerm > req.Term {
		res = &types.ResponseAppendEntryRPC{
			ServerId:      s.serverId,
			Success:       false,
			OutdatedTerm:  true,
			CurrentLeader: s.leaderId,
		}
		return
	}

	if s.currentTerm < req.Term {
		res = &types.ResponseAppendEntryRPC{
			ServerId: s.serverId,
			Success:  false,
		}
		// revert to being a follower
		// TODO: consider locking here
		s.currentTerm = req.Term
		s.leaderId = req.LeaderId
		s.votedFor = ""
		if s.state != string(constants.Follower) {
			s.state = string(constants.Follower)
			if s.state == string(constants.Candidate) {
				s.stopContesting()
			}
			if s.state == string(constants.Leader) {
				s.stopLeading()
			}
			go s.startFollowing()
		}
		return
	}

	if len(req.Entries) == 0 {
		s.resetTicker()
		return
	}

	// return failure
	ok := s.isPreviousEntryPresent(req.PrevEntry)
	if !ok {
		res = &types.ResponseAppendEntryRPC{
			ServerId:            s.serverId,
			Success:             false,
			OutdatedTerm:        false,
			CurrentLeader:       s.leaderId,
			PreviousEntryAbsent: true,
		}
		return
	}

	// TODO: Take a final call on commit. Currently consiodering appending as commit.
	for _, entry := range req.Entries {
		serverLog := logEntry.LogEntry{
			Term:  entry.Term,
			Index: entry.Index,
			Entry: entry.Index,
		}
		s.logs = append(s.logs, serverLog)
	}

	// TODO: As per the call on commit, this may change
	// TODO: consider locking here
	s.lastComittedIndex = len(s.logs) - 1

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

	// TODO: consider locking here
	s.lastAppliedIndex = req.LastCommittedEntryInLeader.Index

	// return success
	res = &types.ResponseAppendEntryRPC{
		ServerId:      s.serverId,
		Success:       true,
		OutdatedTerm:  false,
		CurrentLeader: s.leaderId,
	}
}

func (s *Server) isPreviousEntryPresent(prevEntry *logEntry.LogEntry) bool {

	if prevEntry == nil {
		s.logs = s.logs[:0]
		return true
	}

	index := -1
	entryFound := false
	for i := len(s.logs) - 1; i >= 0; i-- {
		if s.logs[i].Term < prevEntry.Term {
			return false
		}
		if s.logs[i] == *prevEntry {
			entryFound = true
			index = i
			break
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

func (s *Server) applyEntriesToStateMachineIfNeeded() error {

	if s.lastComittedIndex <= s.lastAppliedIndex {
		return nil
	}

	index := -1
	for i := len(s.logs) - 1; i >= 0; i-- {
		if s.logs[i].Index == s.lastComittedIndex {
			index = i
			break
		}
	}

	if index == -1 {
		sugar.Debugw("Invalid lastComittedIndex", "serverId", s.serverId)
		return errors.New("last comitted entry in leader not found in the follower")
	}

	batchEntries := make([]logEntry.LogEntry, 0)

	for ; index >= 0; index-- {
		if s.logs[index].Index == s.lastAppliedIndex {
			break
		}
		batchEntries = append(batchEntries, s.logs[index])
	}

	batchEntries = util.GetReversedSlice(batchEntries)
	err := s.stateMachine.Apply(batchEntries)
	if err != nil {
		return err
	}

	// TODO: consider locking here
	s.lastAppliedIndex = s.lastComittedIndex
	return nil
}
