package raft

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"sync"

	"github.com/adityeah8969/raft/config"
	"github.com/adityeah8969/raft/types"
	"github.com/adityeah8969/raft/types/constants"
	"github.com/adityeah8969/raft/types/logEntry"
	"github.com/adityeah8969/raft/types/logger"
	"github.com/adityeah8969/raft/types/rpcClient"
	serverdb "github.com/adityeah8969/raft/types/serverDb"
	"github.com/adityeah8969/raft/types/stateMachine"
	"github.com/adityeah8969/raft/util"

	"github.com/imdario/mergo"
	"github.com/keegancsmith/rpc"
	"go.uber.org/zap"
)

type processContext struct {
	ctxMu  *sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
}

type Peer struct {
	Hostname string
	Address  string
}

type Server struct {
	serverId string
	// Tight Coupling
	peers             []Peer
	rpcClients        []rpcClient.RpcClientI
	stateMachine      stateMachine.StateMachine
	serverDb          serverdb.DAO
	LeaderId          string
	State             constants.ServerState
	CurrentTerm       int
	VotedFor          string
	LastComittedIndex int
	LastAppliedIndex  int
	NextIndex         []int
	MatchIndex        []int
	// Tight Coupling
	Logs []logEntry.LogEntry
}

var serverInstance *Server
var sugar *zap.SugaredLogger

var serverCtx *processContext
var serverMu sync.RWMutex

var stateStartFunc map[constants.ServerState]func(context.Context)

func init() {

	sugar = logger.GetLogger()

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
	// initializing the logs with a dummy entry, index > 0 will be considered as valid logs
	logs := make([]logEntry.LogEntry, 0)

	peers, err := GetServerPeers()
	if err != nil {
		sugar.Fatalf("Fetching the server peers: ", err)
	}

	rpcClients := make([]rpcClient.RpcClientI, len(peers))
	nextIndex := make([]int, len(peers))
	matchIndex := make([]int, len(peers))

	for _, peer := range peers {
		client, err := rpcClient.GetRpcClient("tcp", peer.Address)
		if err != nil {
			sugar.Fatalw("initializing rpc client: ", "error", err)
		}
		index := util.GetServerIndex(peer.Hostname)
		rpcClients[index] = client
		nextIndex[index] = 1
		matchIndex[index] = 0
	}

	serverInstance = &Server{
		serverId:          config.GetServerId(),
		peers:             peers,
		State:             constants.Follower,
		serverDb:          dbInst,
		stateMachine:      stateMcInst,
		NextIndex:         nextIndex,
		MatchIndex:        matchIndex,
		Logs:              logs,
		rpcClients:        rpcClients,
		CurrentTerm:       0,
		VotedFor:          "",
		LastComittedIndex: 0,
		LastAppliedIndex:  0,
	}

	serverCtx = &processContext{
		ctxMu: &sync.Mutex{},
	}

	stateStartFunc = map[constants.ServerState]func(context.Context){
		constants.Follower:  serverInstance.startFollowing,
		constants.Candidate: serverInstance.startContesting,
		constants.Leader:    serverInstance.startLeading,
	}
}

func GetServerPeers() ([]Peer, error) {
	var peers []Peer
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

func StartServing() error {
	rpc.Register(serverInstance)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", ":8089")
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())

	serverCtx.ctxMu.Lock()
	serverCtx.ctx = ctx
	serverCtx.cancel = cancel
	serverCtx.ctxMu.Unlock()
	go serverInstance.startFollowing(ctx)

	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	go serverInstance.applyEntries(ctx)
	err = http.Serve(l, nil)
	return err
}

func (s *Server) RequestVoteRPC(req *types.RequestVoteRPC, res *types.ResponseVoteRPC) {

	serverMu.RLock()

	leaderId := s.LeaderId
	currTerm := s.CurrentTerm
	votedFor := s.VotedFor
	logs := s.Logs

	serverMu.RUnlock()

	// 	Notify that the requesting candidate should step back.
	if currTerm > req.Term {
		res = &types.ResponseVoteRPC{
			VoteGranted:   false,
			OutdatedTerm:  true,
			CurrentLeader: leaderId,
			Term:          currTerm,
		}
		return
	}

	if currTerm == req.Term && votedFor != "" {
		// Deny vote
		if votedFor != req.CandidateId {
			res = &types.ResponseVoteRPC{
				VoteGranted: false,
			}
			return
		}
		// For idempotency
		if votedFor == req.CandidateId {
			res = &types.ResponseVoteRPC{
				VoteGranted: true,
			}
			return
		}
	}

	// Deny if the candidates logs are not updated enough
	if len(logs) > 0 {
		lastServerLog := logs[len(logs)-1]
		if lastServerLog.Term > req.LastLogTerm || (lastServerLog.Term == req.LastLogTerm && lastServerLog.Index > req.LastLogIndex) {
			res = &types.ResponseVoteRPC{
				VoteGranted: false,
			}
			return
		}
	}

	// Vote for the requesting candidate
	err := s.serverDb.SaveVote(&types.Vote{Term: req.Term, VotedFor: req.CandidateId})
	if err != nil {
		res = &types.ResponseVoteRPC{
			VoteGranted: false,
			Err:         errors.New("db error while persisting vote"),
		}
		return
	}

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

	serverMu.RLock()
	currTerm := s.CurrentTerm
	leaderId := s.LeaderId
	logs := s.Logs
	lastComittedIndex := s.LastComittedIndex
	serverMu.RUnlock()

	// report outdated term in the request
	if currTerm > req.Term {
		res = &types.ResponseAppendEntryRPC{
			ServerId:      s.serverId,
			Success:       false,
			OutdatedTerm:  true,
			CurrentLeader: leaderId,
		}
		return
	}

	// revert to follower in case the current term is less than the one in the request
	if currTerm < req.Term {
		res = &types.ResponseAppendEntryRPC{
			ServerId: s.serverId,
			Success:  false,
		}
		s.revertToFollower(req.Term, req.LeaderId)
		return
	}

	// heartbeat
	if len(req.Entries) == 0 {
		s.resetFollowerTicker()
		return
	}

	removeDupicateEntriesFromRequest(req.Entries, logs, req)

	// Achieving Idempotency
	if len(req.Entries) == 0 {
		res = &types.ResponseAppendEntryRPC{
			ServerId:                     s.serverId,
			Success:                      true,
			CurrentLeader:                leaderId,
			Term:                         currTerm,
			LastCommittedIndexInFollower: lastComittedIndex,
		}
		return
	}

	// return failure
	ok := s.trimInconsistentLogs(req.PrevEntryIndex, req.PrevEntryTerm)
	if !ok {
		res = &types.ResponseAppendEntryRPC{
			ServerId:            s.serverId,
			Success:             false,
			OutdatedTerm:        false,
			CurrentLeader:       leaderId,
			PreviousEntryAbsent: true,
		}
		return
	}

	// Append Entries
	s.appendRPCEntriesToLogs(req.Entries)
	updatedLastCommitIndex := req.LeaderLastCommitIndex
	updatedAttrs := map[string]interface{}{
		"LastComittedIndex": updatedLastCommitIndex,
	}
	s.update(updatedAttrs)

	// return success
	res = &types.ResponseAppendEntryRPC{
		ServerId:                     s.serverId,
		Success:                      true,
		CurrentLeader:                leaderId,
		Term:                         currTerm,
		LastCommittedIndexInFollower: updatedLastCommitIndex,
	}
}

func (s *Server) update(updatedAttrs map[string]interface{}) error {
	serverMu.Lock()
	defer serverMu.Unlock()
	if err := mergo.Map(s, updatedAttrs); err != nil {
		return err
	}
	return nil
}

func (s *Server) applyEntries(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			sugar.Debugf("server %s stopped applying entries on context cancellation", s.serverId)
			return
		default:
			err := s.applyEntriesToStateMachine()
			if err != nil {
				sugar.Debugf("applying entries to server %s : %v", s.serverId, err)
			}
		}
	}
}

func (s *Server) applyEntriesToStateMachine() error {

	serverMu.RLock()
	lastComittedIndex := s.LastComittedIndex
	lastAppliedIndex := s.LastAppliedIndex
	serverMu.RUnlock()

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
	s.update(updatedAttrs)

	return nil
}

func (s *Server) updateCommitIndex() error {

	serverMu.RLock()
	logs := s.Logs
	lastCommitIndex := s.LastComittedIndex
	currTerm := s.CurrentTerm
	peers := s.peers
	matchIndex := s.MatchIndex
	serverMu.RUnlock()

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
	s.update(updatedAttrs)
	return nil
}

func (s *Server) trimInconsistentLogs(prevEntryIndex int, prevEntryTerm int) bool {

	serverMu.RLock()
	logs := s.Logs
	serverMu.RUnlock()

	updatedAttrs := make(map[string]interface{}, 0)

	if prevEntryIndex == -1 {
		updatedAttrs["Logs"] = logs[:0]
		s.update(updatedAttrs)
		return true
	}

	for i := len(logs) - 1; i >= 0; i-- {
		if i == prevEntryIndex && logs[i].Term == prevEntryTerm {
			updatedAttrs["Logs"] = logs[:i+1]
			s.update(updatedAttrs)
			return true
		}
	}

	return false
}

func (s *Server) updateState(to constants.ServerState, updateAttrs map[string]interface{}) error {

	if updateAttrs != nil {
		s.update(updateAttrs)
	}

	serverCtx.ctxMu.Lock()

	if serverCtx.cancel != nil {
		serverCtx.cancel()
	}

	ctx, cancel := context.WithCancel(context.Background())

	serverCtx.ctx = ctx
	serverCtx.cancel = cancel

	serverCtx.ctxMu.Unlock()

	go stateStartFunc[to](ctx)

	return nil
}

func (s *Server) revertToFollower(updatedTerm int, updatedLeader string) {
	updateAttrs := map[string]interface{}{
		"LeaderId":    updatedLeader,
		"CurrentTerm": updatedTerm,
	}
	s.updateState(constants.Follower, updateAttrs)
}

func (s *Server) revertToLeader() {
	updateAttrs := map[string]interface{}{
		"LeaderId": s.serverId,
	}
	s.updateState(constants.Candidate, updateAttrs)
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
