package raft

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/adityeah8969/raft/config"
	"github.com/adityeah8969/raft/types"
	"github.com/adityeah8969/raft/types/constants"
	"github.com/adityeah8969/raft/types/logEntry"
	"github.com/adityeah8969/raft/types/logger"
	"github.com/adityeah8969/raft/types/peer"
	"github.com/adityeah8969/raft/types/rpcClient"
	serverdb "github.com/adityeah8969/raft/types/serverDb"
	"github.com/adityeah8969/raft/types/stateMachine"
	"github.com/adityeah8969/raft/util"

	"github.com/imdario/mergo"
	"github.com/keegancsmith/rpc"
	"go.uber.org/zap"
)

type Server struct {
	// Change this to serverID
	serverId          string
	peers             []peer.Peer
	rpcClients        []rpcClient.RpcClientI
	stateMachine      stateMachine.StateMachine
	serverDb          serverdb.DAO
	stateStartFunc    map[constants.ServerState]func(context.Context)
	statePrepareFunc  map[constants.ServerState]func() (map[string]interface{}, error)
	serverMu          *sync.RWMutex
	logger            *zap.SugaredLogger
	currentCancelFunc context.CancelFunc
	followerTicker    *time.Ticker
	leaderTicker      *time.Ticker
	LeaderId          string
	State             constants.ServerState
	CurrentTerm       int
	VotedFor          string
	LastComittedIndex int
	LastAppliedIndex  int
	NextIndex         []int
	MatchIndex        []int
	Logs              []logEntry.LogEntry
}

var serverInstance *Server

func init() {
	sugar := logger.GetLogger()

	respChannel := make(chan interface{})
	go startRPCServer(&respChannel)
	go func() {
		err := <-respChannel
		sugar.Panic(err)
	}()

	dbInst, err := serverdb.GetServerDbInstance()
	if err != nil {
		sugar.Fatalf("initializing server db: ", err)
	}

	err = serverdb.AutoMigrateModels(dbInst)
	if err != nil {
		sugar.Fatalf("migrating db models: ", err)
	}

	stateMcInst, err := stateMachine.GetStateMachine()
	if err != nil {
		sugar.Fatalf("auto-migrating the server db: ", err)
	}
	// initializing the logs with a dummy entry, index > 0 will be considered as valid logs. What is this ?
	logs := make([]logEntry.LogEntry, 0)

	peers, err := GetServerPeers()
	if err != nil {
		sugar.Fatalf("Fetching the server peers: ", err)
	}

	serverId := config.GetServerId()

	serverInstance = &Server{
		serverId:          serverId,
		peers:             peers,
		logger:            sugar,
		State:             constants.Follower,
		serverDb:          dbInst,
		stateMachine:      stateMcInst,
		followerTicker:    time.NewTicker(1 * time.Millisecond),
		leaderTicker:      time.NewTicker(1 * time.Millisecond),
		serverMu:          &sync.RWMutex{},
		Logs:              logs,
		CurrentTerm:       0,
		VotedFor:          "",
		LastComittedIndex: 0,
		LastAppliedIndex:  0,
	}

	sugar.Debugf("registering rpc for %v", serverId)
	rpc.Register(serverInstance)

	stateStartFunc := map[constants.ServerState]func(context.Context){
		constants.Follower:  serverInstance.startFollowing,
		constants.Candidate: serverInstance.startContesting,
		constants.Leader:    serverInstance.startLeading,
	}

	statePrepareFunc := map[constants.ServerState]func() (map[string]interface{}, error){
		constants.Follower:  serverInstance.prepareFollowerState,
		constants.Candidate: serverInstance.prepareCandidateState,
		constants.Leader:    serverInstance.prepareLeaderState,
	}

	serverInstance.stateStartFunc = stateStartFunc
	serverInstance.statePrepareFunc = statePrepareFunc

	rpcClients := make([]rpcClient.RpcClientI, len(peers)+1)
	nextIndex := make([]int, len(peers)+1)
	matchIndex := make([]int, len(peers)+1)

	for _, peer := range peers {
		if serverId == peer.Address {
			continue
		}
		client, err := rpcClient.GetRpcClient("tcp", fmt.Sprintf("%s:%d", peer.Address, config.GetAppPort()), 10)
		if err != nil {
			panic(fmt.Sprintf("rpc client initialization failed for serverId: %v with error: %v", serverId, err))
		}
		index := util.GetServerIndex(peer.Hostname)
		rpcClients[index] = client
		nextIndex[index] = len(logs)
		matchIndex[index] = 0
	}

	serverInstance.NextIndex = nextIndex
	serverInstance.MatchIndex = matchIndex
	serverInstance.rpcClients = rpcClients

	serverInstance.logger.Debugw("all rpc clients for server initialized successfully", "server", serverInstance.serverId)
}

func GetServerPeers() ([]peer.Peer, error) {
	var peers []peer.Peer
	bytes, err := config.GetPeers()
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(bytes, &peers)
	if err != nil {
		return nil, err
	}
	return peers, nil
}

func startRPCServer(respChannel *chan interface{}) {
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", config.GetAppPort()))
	if err != nil {
		*respChannel <- fmt.Errorf("unable to create rpc listener: %v", err)
	}
	rpc.Accept(l)
}

func StartServing() {

	ctx, cancel := context.WithCancel(context.Background())
	serverInstance.update(nil, cancel, true)
	go serverInstance.startFollowing(ctx)

	applyEntryCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serverInstance.applyEntries(applyEntryCtx)
}

func (s *Server) RequestVoteRPC(ctx context.Context, req *types.RequestVoteRPC, res *types.ResponseVoteRPC) error {

	s.logger.Infow("vote requested", "candidate ", req.CandidateId, "requested from", s.serverId, "election term", req.Term)
	_ = ctx

	s.serverMu.Lock()
	defer s.serverMu.Unlock()

	// notify that the requesting candidate should step back.
	// Race Condition: There are cases where server becomes candidate with an incremented term right before receiving first heartbeat of the leader of previous term.
	// The below check fails the heartbeat request of the concerned leader
	if s.CurrentTerm > req.Term {
		s.logger.Infow("denying vote because of outdated term", "voter: ", s.serverId, "voter term: ", s.CurrentTerm, "candidate: ", req.CandidateId, "current leader: ", s.LeaderId)
		*res = types.ResponseVoteRPC{
			VoteGranted:   false,
			OutdatedTerm:  true,
			CurrentLeader: s.LeaderId,
			Term:          s.CurrentTerm,
			Voter:         s.serverId,
		}
		return nil
	}

	if s.CurrentTerm == req.Term && s.VotedFor != "" {
		if s.VotedFor != req.CandidateId {
			s.logger.Infow("denying vote because already voted", "voter: ", s.serverId, "voter term: ", s.CurrentTerm, "candidate: ", req.CandidateId, "voted for", s.VotedFor)
			*res = types.ResponseVoteRPC{
				VoteGranted: false,
				Voter:       s.serverId,
			}
			return nil
		}
		// For idempotency
		// Is there a possibility of double vote count on the candidate side ??
		if s.VotedFor == req.CandidateId {
			s.logger.Infow("already voted the candidate", "voter: ", s.serverId, "voter term: ", s.CurrentTerm, "candidate: ", req.CandidateId, "voted for", s.VotedFor)
			*res = types.ResponseVoteRPC{
				VoteGranted: true,
				Voter:       s.serverId,
			}
			return nil
		}
	}

	if len(s.Logs) > 0 {
		lastServerLog := s.Logs[len(s.Logs)-1]
		if lastServerLog.Term > req.LastLogTerm || (lastServerLog.Term == req.LastLogTerm && lastServerLog.Index > req.LastLogIndex) {
			s.logger.Infow("denying vote because candidate logs are not updated enough", "voter: ", s.serverId, "voter term: ", s.CurrentTerm, "candidate: ", req.CandidateId)
			*res = types.ResponseVoteRPC{
				VoteGranted: false,
				Voter:       s.serverId,
			}
			return nil
		}
	}

	// Vote for the requesting candidate
	err := s.serverDb.SaveVote(&types.Vote{Term: req.Term, VotedFor: req.CandidateId})
	if err != nil {
		*res = types.ResponseVoteRPC{
			VoteGranted: false,
			Err:         errors.New("db error while persisting vote"),
			Voter:       s.serverId,
		}
		s.logger.Debugf("err saving vote in db: %v", err)
		return err
	}

	updatedAttrs := map[string]interface{}{
		"CurrentTerm": req.Term,
		"VotedFor":    req.CandidateId,
	}
	s.update(updatedAttrs, nil, false)

	*res = types.ResponseVoteRPC{
		VoteGranted: true,
		Voter:       s.serverId,
	}
	s.logger.Infow("granting vote", "voter: ", s.serverId, "candidate: ", req.CandidateId, "term: ", req.Term)
	if s.State == constants.Follower {
		s.resetFollowerTicker(false)
	}
	return nil
}

func (s *Server) AppendEntryRPC(ctx context.Context, req *types.RequestAppendEntryRPC, res *types.ResponseAppendEntryRPC) error {

	_ = res

	s.serverMu.Lock()
	defer s.serverMu.Unlock()

	// report outdated term in the request
	if s.CurrentTerm > req.Term {
		s.logger.Debugw("denying append entry call because of outdated request term", "server", s.serverId, "request", *req)
		*res = types.ResponseAppendEntryRPC{
			ServerId:      s.serverId,
			Success:       false,
			OutdatedTerm:  true,
			CurrentLeader: s.LeaderId,
		}
		return nil
	}

	// revert to follower in case the current term is less than the one in the request
	if s.CurrentTerm < req.Term {
		s.logger.Debugw("reverting to follower as the server term is less than request term", "server", s.serverId, "request", *req)
		*res = types.ResponseAppendEntryRPC{
			ServerId: s.serverId,
			Success:  false,
		}
		go s.revertToFollower(req.Term, req.LeaderId)
		return nil
	}

	// heartbeat
	if len(req.Entries) == 0 {
		s.logger.Debugw("heartbeat received", "sender", req.LeaderId, "receiver", s.serverId)
		if s.State == constants.Candidate {
			go s.revertToFollower(req.Term, req.LeaderId)
		} else {
			go s.resetFollowerTicker(true)
		}
		*res = types.ResponseAppendEntryRPC{
			ServerId: s.serverId,
			Success:  true,
		}
		return nil
	}

	removeDupicateEntriesFromRequest(req.Entries, s.Logs, req)

	// Achieving Idempotency
	if len(req.Entries) == 0 {
		*res = types.ResponseAppendEntryRPC{
			ServerId:                     s.serverId,
			Success:                      true,
			CurrentLeader:                s.LeaderId,
			Term:                         s.CurrentTerm,
			LastCommittedIndexInFollower: s.LastComittedIndex,
		}
		s.logger.Debugw("no unique entry to append", "server", s.serverId)
		return nil
	}

	// return failure
	ok := s.trimInconsistentLogs(req.PrevEntryIndex, req.PrevEntryTerm, false)
	if !ok {
		s.logger.Debugw("failed to trim inconsistent logs", "server", s.serverId)
		*res = types.ResponseAppendEntryRPC{
			ServerId:            s.serverId,
			Success:             false,
			OutdatedTerm:        false,
			CurrentLeader:       s.LeaderId,
			PreviousEntryAbsent: true,
		}
		return nil
	}

	// Append Entries
	s.appendRPCEntriesToLogs(req.Entries, false)
	updatedLastCommitIndex := req.LeaderLastCommitIndex
	updatedAttrs := map[string]interface{}{
		"LastComittedIndex": updatedLastCommitIndex,
	}
	s.update(updatedAttrs, nil, false)

	// return success
	*res = types.ResponseAppendEntryRPC{
		ServerId:                     s.serverId,
		Success:                      true,
		CurrentLeader:                s.LeaderId,
		Term:                         s.CurrentTerm,
		LastCommittedIndexInFollower: updatedLastCommitIndex,
	}

	s.logger.Debugw("successfully appended entry", "server", s.serverId, "entries", req.Entries)
	return nil
}

func (s *Server) update(updateAttrs map[string]interface{}, cancel context.CancelFunc, withLock bool) error {
	s.logger.Debugw("updating server attributes", "server", s.serverId, "updateAttrs", updateAttrs)

	if withLock {
		s.serverMu.Lock()
		defer s.serverMu.Unlock()
	}

	if updateAttrs != nil {
		if err := mergo.MapWithOverwrite(s, updateAttrs); err != nil {
			return err
		}
	}

	if cancel != nil {
		s.currentCancelFunc = cancel
	}
	return nil
}

func (s *Server) applyEntries(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			s.logger.Debugf("server %s stopped applying entries on context cancellation", s.serverId)
			return
		default:
			err := s.applyEntriesToStateMachine()
			if err != nil {
				s.logger.Debugf("applying entries to server %s : %v", s.serverId, err)
			}
		}
	}
}

func (s *Server) applyEntriesToStateMachine() error {

	s.serverMu.RLock()
	lastComittedIndex := s.LastComittedIndex
	lastAppliedIndex := s.LastAppliedIndex
	s.serverMu.RUnlock()

	if lastComittedIndex <= lastAppliedIndex {
		return nil
	}

	batchEntries := s.Logs[lastAppliedIndex+1 : lastComittedIndex+1]

	if len(batchEntries) == 0 {
		return nil
	}

	err := s.stateMachine.Apply(batchEntries)
	if err != nil {
		return err
	}

	updatedAttrs := map[string]interface{}{
		"LastAppliedIndex": lastComittedIndex,
	}
	s.update(updatedAttrs, nil, true)
	return nil
}

func (s *Server) updateCommitIndex() error {

	s.serverMu.RLock()
	logs := s.Logs
	lastCommitIndex := s.LastComittedIndex
	currTerm := s.CurrentTerm
	peers := s.peers
	matchIndex := s.MatchIndex
	s.serverMu.RUnlock()

	var updatedCommitIndex int
	for i := len(logs) - 1; i > lastCommitIndex; i-- {
		cnt := 0
		for _, matchIndex := range matchIndex {
			if matchIndex >= i {
				cnt++
			}
		}
		if cnt > len(peers)/2 && logs[lastCommitIndex].Term == currTerm {
			updatedCommitIndex = i
			break
		}
	}

	if updatedCommitIndex == 0 {
		return nil
	}

	err := s.serverDb.SaveLogs(logs[lastCommitIndex+1 : updatedCommitIndex+1])
	if err != nil {
		return err
	}

	updatedAttrs := map[string]interface{}{
		"LastComittedIndex": updatedCommitIndex,
	}
	s.update(updatedAttrs, nil, true)
	return nil
}

func (s *Server) trimInconsistentLogs(prevEntryIndex int, prevEntryTerm int, withReadLock bool) bool {

	var logs []logEntry.LogEntry

	if withReadLock {
		s.serverMu.RLock()
		logs = s.Logs
		s.serverMu.RUnlock()
	} else {
		logs = s.Logs
	}

	updatedAttrs := make(map[string]interface{}, 0)

	if prevEntryIndex == -1 {
		updatedAttrs["Logs"] = logs[:0]
		s.update(updatedAttrs, nil, true)
		return true
	}

	for i := len(logs) - 1; i >= 0; i-- {
		if i == prevEntryIndex && logs[i].Term == prevEntryTerm {
			updatedAttrs["Logs"] = logs[:i+1]
			s.update(updatedAttrs, nil, true)
			return true
		}
	}

	return false
}

func (s *Server) updateState(to constants.ServerState, updateAttrs map[string]interface{}) error {
	statePrepareAttrs, err := s.statePrepareFunc[to]()
	if err != nil {
		return err
	}
	updateAttrs = util.MergeMaps(statePrepareAttrs, updateAttrs)
	s.cleanupCurrentstate()
	ctx, cancel := context.WithCancel(context.Background())
	s.update(updateAttrs, cancel, true)
	go s.stateStartFunc[to](ctx)
	return nil
}

// For all the revert methods should we return the errors

func (s *Server) revertToFollower(updatedTerm int, updatedLeader string) error {
	s.logger.Debugw("reverting to follower", "server", s.serverId)
	updateAttrs := map[string]interface{}{
		"LeaderId":    updatedLeader,
		"CurrentTerm": updatedTerm,
	}
	return s.updateState(constants.Follower, updateAttrs)
}

func (s *Server) revertToLeader() error {
	s.logger.Debugw("reverting to leader", "server", s.serverId)
	updateAttrs := map[string]interface{}{
		"LeaderId": s.serverId,
	}
	return s.updateState(constants.Leader, updateAttrs)
}

func (s *Server) revertToCandidate() error {
	s.logger.Debugw("reverting to candidate", "server", s.serverId)
	updateAttrs := map[string]interface{}{
		"LeaderId": s.serverId,
	}
	return s.updateState(constants.Candidate, updateAttrs)
}

func (s *Server) cleanupCurrentstate() {
	s.logger.Debugw("cleaning up before state transition", "server", s.serverId)
	s.serverMu.RLock()
	if s.currentCancelFunc != nil {
		s.currentCancelFunc()
	}
	s.serverMu.RUnlock()
}

func removeDupicateEntriesFromRequest(entries []logEntry.LogEntry, logs []logEntry.LogEntry, req *types.RequestAppendEntryRPC) {
	for _, entry := range entries {
		if entry.Index > len(logs)-1 || logs[entry.Index] != entry {
			break
		}
		if logs[entry.Index] == entry {
			req.PrevEntryIndex++
			req.PrevEntryTerm++
		}
	}
	req.Entries = req.Entries[req.PrevEntryIndex+1:]
}
