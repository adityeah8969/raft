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
	"github.com/huandu/go-clone"
	"github.com/imdario/mergo"
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

type Server struct {
	serverId     string
	peers        []string
	rpcClients   map[string]interface{}
	stateMachine stateMachine.StateMachine
	serverDb     *gorm.DB

	LeaderId          string
	State             string
	CurrentTerm       int
	VotedFor          string
	LastComittedIndex int
	LastAppliedIndex  int
	// next log entry to send to servers
	NextIndex map[string]int
	// index of the highest log entry known to be replicated on server
	MatchIndex   map[string]int
	Logs         []logEntry.LogEntry
	ServerTicker *ServerTicker
}

var serverInstance *Server
var sugar *zap.SugaredLogger

var followerContextInst *processContext
var candidateContextInst *processContext
var leaderContextInst *processContext

var candidateCtxMu = &sync.Mutex{}
var leaderCtxMu = &sync.Mutex{}
var followerCtxMu = &sync.Mutex{}
var serverMu = &sync.RWMutex{}

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
		State:             string(constants.Follower),
		serverDb:          serverDb,
		stateMachine:      stateMcInst,
		NextIndex:         nextIndex,
		MatchIndex:        matchIndex,
		Logs:              logs,
		rpcClients:        rpcClients,
		ServerTicker:      serverTicker,
		CurrentTerm:       0,
		VotedFor:          "",
		LastComittedIndex: 0,
		LastAppliedIndex:  0,
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
	ctx, cancel := context.WithCancel(context.Background())
	go serverInstance.applyEntries(ctx)
	err = http.Serve(l, nil)
	cancel()
	return err
}

func (s *Server) RequestVoteRPC(req *types.RequestVoteRPC, res *types.ResponseVoteRPC) {

	clonedInst := s.getClonedInst()

	// 	Notify that the requesting candidate should step back.
	if clonedInst.CurrentTerm > req.Term {
		res = &types.ResponseVoteRPC{
			VoteGranted:   false,
			OutdatedTerm:  true,
			CurrentLeader: clonedInst.LeaderId,
		}
		return
	}
	// 	Do not vote for the requesting candidate, if already voted
	if clonedInst.CurrentTerm == req.Term && clonedInst.VotedFor != "" {
		res = &types.ResponseVoteRPC{
			VoteGranted: false,
		}
		return
	}
	var lastServerLog logEntry.LogEntry
	if len(clonedInst.Logs) > 0 {
		lastServerLog = clonedInst.Logs[len(clonedInst.Logs)-1]
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

	s.CurrentTerm = req.Term
	s.VotedFor = req.CandidateId

	updatedAttrs := map[string]interface{}{
		"CurrentTerm": req.Term,
		"VotedFor":    req.CandidateId,
	}
	s.update(updatedAttrs)

	res = &types.ResponseVoteRPC{
		VoteGranted: true,
	}
}

func (s *Server) AppendEntryRPC(req *types.RequestAppendEntryRPC, res *types.ResponseAppendEntryRPC) {

	clonedInst := s.getClonedInst()

	// report outdated term in the request
	if clonedInst.CurrentTerm > req.Term {
		res = &types.ResponseAppendEntryRPC{
			ServerId:      clonedInst.serverId,
			Success:       false,
			OutdatedTerm:  true,
			CurrentLeader: clonedInst.LeaderId,
		}
		return
	}

	if clonedInst.CurrentTerm < req.Term {
		res = &types.ResponseAppendEntryRPC{
			ServerId: clonedInst.serverId,
			Success:  false,
		}

		updatedAttrs := map[string]interface{}{
			"CurrentTerm": req.Term,
			"LeaderId":    req.LeaderId,
			"VotedFor":    "",
		}

		// revert to being a follower
		if clonedInst.State != string(constants.Follower) {
			if clonedInst.State == string(constants.Candidate) {
				s.stopContesting()
			}
			if clonedInst.State == string(constants.Leader) {
				s.stopLeading()
			}
			updatedAttrs["State"] = string(constants.Follower)
			s.update(updatedAttrs)
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
			ServerId:            clonedInst.serverId,
			Success:             false,
			OutdatedTerm:        false,
			CurrentLeader:       clonedInst.LeaderId,
			PreviousEntryAbsent: true,
		}
		return
	}

	updatedAttrs := make(map[string]interface{})
	updatedLogs := make([]logEntry.LogEntry, 0)

	// TODO: Take a final call on commit. Currently considering appending as commit.
	for _, entry := range req.Entries {
		serverLog := logEntry.LogEntry{
			Term:  entry.Term,
			Index: entry.Index,
			Entry: entry.Entry,
		}
		updatedLogs = append(clonedInst.Logs, serverLog)
	}
	updatedAttrs["Logs"] = updatedLogs
	// TODO: As per the call on commit, this may change
	updatedAttrs["LastComittedIndex"] = len(s.Logs) - 1
	// apply entries
	err := s.applyEntriesToStateMachine(req.LastCommittedEntryInLeader.Index)
	if err != nil {
		// log here
		res = &types.ResponseAppendEntryRPC{
			ServerId:      s.serverId,
			Success:       false,
			OutdatedTerm:  false,
			CurrentLeader: s.LeaderId,
		}
	}

	updatedAttrs["LastAppliedIndex"] = req.LastCommittedEntryInLeader.Index
	s.update(updatedAttrs)

	// return success
	res = &types.ResponseAppendEntryRPC{
		ServerId:      s.serverId,
		Success:       true,
		OutdatedTerm:  false,
		CurrentLeader: s.LeaderId,
	}
}

func (s *Server) Set(request *types.RequestEntry) *types.ResponseEntry {

	clonedInst := s.getClonedInst()

	// re-direct to leader
	if clonedInst.LeaderId != clonedInst.serverId {
		client := clonedInst.rpcClients[clonedInst.LeaderId].(*rpc.Client)
		response := &types.ResponseEntry{}
		err := util.RPCWithRetry(client, "Server.Set", request, response, config.GetRetryRPCLimit(), config.GetRPCTimeoutInSeconds())
		if err != nil {
			sugar.Warnw("set entry failed after retries", "serverId", s.serverId, "leaderId", s.LeaderId, "rpcClient", client, "request", request, "response", response)
			response = &types.ResponseEntry{
				Success: false,
				Err:     err,
			}
		}
		return response
	}

	newIndex := len(clonedInst.Logs)
	log := logEntry.LogEntry{
		Term:  clonedInst.CurrentTerm,
		Index: newIndex,
		Entry: request,
	}

	updatedLogs := append(clonedInst.Logs, log)
	updatedAttrs := map[string]interface{}{
		"Logs": updatedLogs,
	}
	s.update(updatedAttrs)

	// We need a way to synchronously send the response back to the client after the entry is applied to the state machine.
	// Just check if the entry has been applied, let it be a blocking call.
	timer := time.NewTimer(time.Duration(config.GetClientRequestInSeconds()) * time.Second)

	for {
		select {
		case <-timer.C:
			sugar.Debugf("timing out while applying the entry", "serverId", s.serverId, "leaderId", s.LeaderId, "logEntry", log)
			return &types.ResponseEntry{
				Success: false,
				Err:     errors.New("timing out while applying the entry"),
			}
		default:
			clonedInst = s.getClonedInst()
			if clonedInst.LastAppliedIndex < newIndex {
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

	clonedInst := s.getClonedInst()

	// re-direct to leader
	if clonedInst.LeaderId != clonedInst.serverId {
		client := clonedInst.rpcClients[clonedInst.LeaderId].(*rpc.Client)
		response := &types.ResponseEntry{}
		err := util.RPCWithRetry(client, "Server.Get", request, response, config.GetRetryRPCLimit(), config.GetRPCTimeoutInSeconds())
		if err != nil {
			sugar.Warnw("get entry failed after retries", "serverId", s.serverId, "leaderId", s.LeaderId, "rpcClient", client, "request", request, "response", response)
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

func (s *Server) startTicker(ctx context.Context) {
	defer s.ServerTicker.ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			sugar.Infof("stopping ticker for server %s", s.serverId)
			return
		case t := <-s.ServerTicker.ticker.C:
			sugar.Infof("election contest started by %s at %v", s.serverId, t)
			go s.startContesting()
			s.stopFollowing()
		}
	}
}

func (s *Server) resetTicker() {
	serverMu.Lock()
	defer serverMu.Unlock()
	s.ServerTicker.ticker.Reset(util.GetRandomTickerDuration(config.GetTickerIntervalInMillisecond()))
}

func (s *Server) startFollowing() {
	s.State = string(constants.Follower)
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
	serverMu.Lock()
	defer serverMu.Unlock()
	updateProcessContext(followerContextInst, &processContext{}, followerCtxMu)
}

func (s *Server) update(updatedAttrs map[string]interface{}) error {
	serverMu.Lock()
	defer serverMu.Unlock()
	if err := mergo.Map(s, updatedAttrs); err != nil {
		return err
	}
	return nil
}

func (s *Server) getClonedInst() *Server {
	serverMu.RLock()
	defer serverMu.RUnlock()
	clonedInst := clone.Clone(serverInstance).(*Server)
	return clonedInst
}

func (s *Server) stopContesting() {
	serverMu.Lock()
	defer serverMu.Unlock()
	updateProcessContext(candidateContextInst, &processContext{}, followerCtxMu)
}

func (s *Server) startContesting() error {
	for {
		select {
		case <-candidateContextInst.ctx.Done():
			sugar.Infof("stopping election contest for server %s", s.serverId)
			return nil
		default:

			err := s.voteForItself()
			if err != nil {
				return err
			}

			var wg sync.WaitGroup
			wg.Add(len(s.peers))

			responseChan := make(chan *types.ResponseVoteRPC, len(s.peers))

			for k := range s.rpcClients {
				go func() {
					defer wg.Done()
					client := s.rpcClients[k].(*rpc.Client)
					request := &types.RequestVoteRPC{
						Term:        s.CurrentTerm,
						CandidateId: s.serverId,
					}
					response := &types.ResponseVoteRPC{}
					err := util.RPCWithRetry(client, "Server.RequestVoteRPC", request, response, config.GetRetryRPCLimit(), config.GetRPCTimeoutInSeconds())
					if err != nil {
						sugar.Warnw("request vote RPC failed after retries", "candidate", s.serverId, "rpcClient", client, "request", request, "response", response)
						response = &types.ResponseVoteRPC{
							VoteGranted: false,
						}
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
					updatedAttrs := map[string]interface{}{
						"State":       string(constants.Follower),
						"LeaderId":    resp.CurrentLeader,
						"CurrentTerm": resp.Term,
					}
					s.update(updatedAttrs)
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

				updatedAttrs := map[string]interface{}{
					"LeaderId": s.serverId,
					"State":    string(constants.Leader),
				}
				s.update(updatedAttrs)

				ctx, cancel := context.WithCancel(context.Background())
				updatedCtx := &processContext{
					ctx:    ctx,
					cancel: cancel,
				}
				updateProcessContext(leaderContextInst, updatedCtx, leaderCtxMu)

				s.stopContesting()
				go s.startLeading()
				return nil
			}

		}
	}
}

func (s *Server) voteForItself() error {
	clonedInst := s.getClonedInst()
	vote := &Vote{
		votedFor: clonedInst.serverId,
		term:     clonedInst.CurrentTerm + 1,
	}
	err := s.serverDb.Model(&Vote{}).Save(vote).Error
	if err != nil {
		return err
	}
	updatedAttrs := map[string]interface{}{
		"CurrentTerm": clonedInst.CurrentTerm + 1,
		"VotedFor":    clonedInst.serverId,
	}
	s.update(updatedAttrs)
	return nil
}

func (s *Server) startLeading() error {
	sugar.Infof("%s started leading", s.serverId)

	clonedInst := s.getClonedInst()
	updatedNextIndexMap := make(map[string]int, len(s.peers))
	updatedMatchIndexMap := make(map[string]int, len(s.peers))
	for _, v := range s.peers {
		updatedNextIndexMap[v] = len(clonedInst.Logs)
		updatedMatchIndexMap[v] = 0
	}

	updatedAttrs := map[string]interface{}{
		"NextIndex":  updatedNextIndexMap,
		"MatchIndex": updatedMatchIndexMap,
	}
	s.update(updatedAttrs)

	for {
		select {
		case <-leaderContextInst.ctx.Done():
			sugar.Infof("%s stopped leading", clonedInst.serverId)
			return nil
		default:
			// periodic heartbeat
			ctx, _ := context.WithCancel(leaderContextInst.ctx)
			go s.makeAppendEntryCallsConcurrently(ctx)
			s.sendHeartBeats(ctx)
		}
	}
}

func (s *Server) stopLeading() {
	serverMu.Lock()
	defer serverMu.Unlock()
	updateProcessContext(leaderContextInst, &processContext{}, leaderCtxMu)
}

func (s *Server) applyEntries(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			sugar.Debugf("server %s stopped applying entries on context cancellation", s.serverId)
			return
		default:
			clonedInst := s.getClonedInst()
			s.applyEntriesToStateMachine(clonedInst.LastComittedIndex)
		}
	}
}

func (s *Server) applyEntriesToStateMachine(lastComittedIndex int) error {

	clonedInst := s.getClonedInst()

	if lastComittedIndex <= clonedInst.LastAppliedIndex {
		return nil
	}

	index := -1
	for i := len(clonedInst.Logs) - 1; i >= 0; i-- {
		if i == lastComittedIndex {
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
		if index == clonedInst.LastAppliedIndex {
			break
		}
		batchEntries = append(batchEntries, s.Logs[index])
	}
	batchEntries = util.GetReversedSlice(batchEntries)

	err := s.stateMachine.Apply(batchEntries)
	if err != nil {
		return err
	}

	updatedAttrs := map[string]interface{}{
		"LastAppliedIndex": lastComittedIndex,
	}
	s.update(updatedAttrs)

	return nil
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

			clonedInst := s.getClonedInst()
			responseChan := make(chan *types.ResponseAppendEntryRPC, len(s.peers))
			for i := range s.rpcClients {
				go func() {
					defer wg.Done()
					client := s.rpcClients[i].(*rpc.Client)
					request := &types.RequestAppendEntryRPC{
						Term:                       clonedInst.CurrentTerm,
						LeaderId:                   clonedInst.serverId,
						LastCommittedEntryInLeader: clonedInst.Logs[clonedInst.LastComittedIndex],
					}
					response := &types.ResponseAppendEntryRPC{}
					err := util.RPCWithRetry(client, "Server.AppendEntryRPC", request, response, config.GetRetryRPCLimit(), config.GetRPCTimeoutInSeconds())
					if err != nil {
						sugar.Warnw("append entry RPC failed after retries", "leader", s.serverId, "rpcClient", client, "request", request, "response", response)
						response = &types.ResponseAppendEntryRPC{}
					}
					responseChan <- response
				}()
			}
			wg.Wait()

			outdatedTerm := false
			updatedTerm := clonedInst.CurrentTerm
			updatedLeader := clonedInst.LeaderId

			for resp := range responseChan {
				if !resp.Success && resp.OutdatedTerm {
					outdatedTerm = true
					updatedTerm = resp.Term
					updatedLeader = resp.CurrentLeader
					break
				}
			}

			if outdatedTerm {
				updatedAttrs := map[string]interface{}{
					"CurrentTerm": updatedTerm,
					"LeaderId":    updatedLeader,
				}
				s.update(updatedAttrs)
				s.stopLeading()
				go s.startFollowing()
			}

			heartBeatTicker.ticker.Reset(time.Duration(config.GetTickerIntervalInMillisecond()) * time.Millisecond)
		}
	}
}

func (s *Server) makeAppendEntryCallsConcurrently(ctx context.Context) {

	clonedInst := s.getClonedInst()

	for {
		select {
		case <-ctx.Done():
			sugar.Infof("%s stopped making append entry calls", clonedInst.serverId)
			return
		default:
			responseChan := make(chan *types.ResponseAppendEntryRPC)
			wg := &sync.WaitGroup{}
			for k := range clonedInst.rpcClients {
				client := clonedInst.rpcClients[k].(*rpc.Client)
				nextIndex := clonedInst.NextIndex[k]
				prevEntryIndex := nextIndex - 1
				prevEntry := clonedInst.Logs[prevEntryIndex]
				lastComittedEntry := clonedInst.Logs[clonedInst.LastComittedIndex]
				bulkEntries := make([]logEntry.LogEntry, len(clonedInst.Logs)-nextIndex)
				for i := len(clonedInst.Logs) - 1; i >= nextIndex; i-- {
					bulkEntries = append(bulkEntries, clonedInst.Logs[i])
				}
				bulkEntries = util.GetReversedSlice(bulkEntries)
				var request *types.RequestAppendEntryRPC
				if len(s.Logs)-1 >= clonedInst.NextIndex[k] {
					request = &types.RequestAppendEntryRPC{
						Term:                       clonedInst.CurrentTerm,
						LeaderId:                   clonedInst.serverId,
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
			updatedNextIndex := clonedInst.NextIndex
			updatedMatchIndex := clonedInst.MatchIndex
			for resp := range responseChan {
				// If successful: update nextIndex and matchIndex for the follower
				if resp.Success {
					serverId := resp.ServerId
					updatedNextIndex[serverId] = int(math.Min(float64(resp.LastCommittedIndexInFollower+1), float64(len(clonedInst.Logs)-1)))
					updatedMatchIndex[serverId] = resp.LastCommittedIndexInFollower
				}
			}
			updatedAttrs := map[string]interface{}{
				"NextIndex":  updatedNextIndex,
				"MatchIndex": updatedMatchIndex,
			}
			s.update(updatedAttrs)
			s.setCommitIndex()
		}
	}
}

func (s *Server) makeAppendEntryCall(ctx context.Context, client *rpc.Client, prevEntryIndex int, request *types.RequestAppendEntryRPC, responseChan chan *types.ResponseAppendEntryRPC, wg *sync.WaitGroup) error {
	defer wg.Done()
	response := &types.ResponseAppendEntryRPC{}
	clonedInst := s.getClonedInst()
	for {
		select {
		case <-ctx.Done():
			sugar.Debugw("append entry call stopped by context", "leaderId", s.serverId)
			return errors.New("append entry call stopped by context")
		default:
			err := util.RPCWithRetry(client, "Server.AppendEntryRPC", request, response, config.GetRetryRPCLimit(), config.GetRPCTimeoutInSeconds())
			if err != nil {
				sugar.Warnw("append entry RPC calls failed", "Error", err, "leaderId", s.serverId, "rpcClient", client)
				response = &types.ResponseAppendEntryRPC{}
				responseChan <- response
				return err
			}
			if !response.Success {
				if response.OutdatedTerm {
					// revert to being a follower
					updatedAttrs := map[string]interface{}{
						"State":       string(constants.Follower),
						"LeaderId":    response.CurrentLeader,
						"CurrentTerm": response.Term,
					}
					s.update(updatedAttrs)
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
					prevEntry := clonedInst.Logs[prevEntryIndex]
					request.PrevEntry = &prevEntry
					continue
				}
			}
			responseChan <- response
			return nil
		}
	}
}

func (s *Server) setCommitIndex() {

	clonedInst := s.getClonedInst()

	lastCommitIndex := clonedInst.LastComittedIndex
	finalCommitIndex := lastCommitIndex
	lastCommitIndex++

	for ; lastCommitIndex < len(clonedInst.Logs)-1; lastCommitIndex++ {
		cnt := 0
		for _, v := range clonedInst.MatchIndex {
			if lastCommitIndex <= v {
				cnt++
			}
		}
		if cnt > len(clonedInst.peers)/2 && clonedInst.Logs[lastCommitIndex].Term == clonedInst.CurrentTerm {
			finalCommitIndex = lastCommitIndex
		}
	}

	updatedAttrs := map[string]interface{}{
		"LastComittedIndex": finalCommitIndex,
	}
	s.update(updatedAttrs)
}

func (s *Server) isPreviousEntryPresent(prevEntry *logEntry.LogEntry) bool {

	clonedInst := s.getClonedInst()
	updatedLogs := make([]logEntry.LogEntry, 0)
	updatedAttrs := make(map[string]interface{}, 0)

	if prevEntry == nil {
		updatedLogs = clonedInst.Logs[:0]
		updatedAttrs["Logs"] = updatedLogs
		s.update(updatedAttrs)
		return true
	}

	index := -1
	entryFound := false
	for i := len(clonedInst.Logs) - 1; i >= 0; i-- {
		if clonedInst.Logs[i].Term < prevEntry.Term {
			return false
		}
		if clonedInst.Logs[i] == *prevEntry {
			entryFound = true
			index = i
			break
		}
	}

	if entryFound {
		updatedLogs = clonedInst.Logs[:index+1]
		updatedAttrs["Logs"] = updatedLogs
		s.update(updatedAttrs)
	}

	return false
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
