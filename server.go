package raft

import (
	"context"
	"errors"
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
	serverId string
	// While working with peers we should ignore the current server
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
	NextIndex []int
	// index of the highest log entry known to be replicated on server
	MatchIndex   []int
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

var serverMu = &sync.Mutex{}

func init() {

	serverDb, err := serverdb.GetServerDbInstance()
	if err != nil {
		sugar.Fatalf("initializing server db: ", err)
	}

	err = migrateServerModels(serverDb)
	if err != nil {
		sugar.Fatalf("migrating db models: ", err)
	}

	stateMcInst, err := stateMachine.GetStateMachine()
	if err != nil {
		sugar.Fatalf("auto-migrating the server db: ", err)
	}
	// initializing the logs with a dummy entry, index > 0 will be considered as valid logs
	logs := make([]logEntry.LogEntry, 0)
	peers := config.GetPeers()
	rpcClients := make(map[string]interface{}, len(peers))
	nextIndex := make([]int, len(peers))
	matchIndex := make([]int, len(peers))
	for index, peer := range peers {
		client, err := rpc.DialHTTP("tcp", peer)
		if err != nil {
			sugar.Fatalf("dialing:", err)
		}
		rpcClients[peer] = client
		nextIndex[index] = 1
		matchIndex[index] = 0
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

	// Check if we can make do with just one context
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
	defer cancel()
	go serverInstance.applyEntries(ctx)
	err = http.Serve(l, nil)
	return err
}

func migrateServerModels(db *gorm.DB) error {
	err := db.AutoMigrate(&Vote{}, &logEntry.LogEntry{})
	if err != nil {
		return err
	}
	return nil
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

func (s *Server) RequestVoteRPC(req *types.RequestVoteRPC, res *types.ResponseVoteRPC) {

	clonedInst := s.getClonedInst()

	// 	Notify that the requesting candidate should step back.
	if clonedInst.CurrentTerm > req.Term {
		res = &types.ResponseVoteRPC{
			VoteGranted:   false,
			OutdatedTerm:  true,
			CurrentLeader: clonedInst.LeaderId,
			Term:          clonedInst.CurrentTerm,
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

	if len(clonedInst.Logs) > 0 {
		lastServerLog := clonedInst.Logs[len(clonedInst.Logs)-1]
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
			"State":       constants.Follower,
			"VotedFor":    "",
		}
		s.update(updatedAttrs)
	}

	switch clonedInst.State {
	case string(constants.Candidate):
		go s.stopContesting()
	case string(constants.Leader):
		go s.stopLeading()
	}

	if clonedInst.State != string(constants.Follower) {
		s.startFollowing()
	}

	if len(req.Entries) == 0 {
		s.resetTicker()
		return
	}

	// return failure
	ok := s.trimInconsistentLogs(req.PrevEntryIndex, req.PrevEntryTerm)
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
	updatedLogs := append(clonedInst.Logs, req.Entries...)
	updatedAttrs["Logs"] = updatedLogs
	updatedAttrs["LastComittedIndex"] = len(updatedLogs) - 1
	s.update(updatedAttrs)

	// return success
	res = &types.ResponseAppendEntryRPC{
		ServerId:                     s.serverId,
		Success:                      true,
		CurrentLeader:                s.LeaderId,
		Term:                         s.CurrentTerm,
		LastCommittedIndexInFollower: len(updatedLogs) - 1,
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

func (s *Server) getClonedInst() *Server {
	serverMu.Lock()
	defer serverMu.Unlock()
	clonedInst := clone.Clone(serverInstance).(*Server)
	return clonedInst
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

	clonedInst := s.getClonedInst()

	if clonedInst.LastComittedIndex <= clonedInst.LastAppliedIndex {
		return nil
	}

	batchEntries := s.Logs[clonedInst.LastAppliedIndex+1 : clonedInst.LastComittedIndex+1]

	if len(batchEntries) == 0 {
		return nil
	}

	err := s.stateMachine.Apply(batchEntries)
	if err != nil {
		return err
	}

	updatedAttrs := map[string]interface{}{
		"LastAppliedIndex": clonedInst.LastComittedIndex,
	}
	s.update(updatedAttrs)

	return nil
}

func (s *Server) updateCommitIndex() error {

	clonedInst := s.getClonedInst()

	logs := clonedInst.Logs
	lastCommitIndex := clonedInst.LastComittedIndex
	currTerm := clonedInst.CurrentTerm
	peers := clonedInst.peers

	var updatedCommitIndex int
	for i := len(logs) - 1; i > lastCommitIndex; i-- {
		cnt := 0
		for _, matchIndex := range clonedInst.MatchIndex {
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

	err := s.serverDb.Model(&logEntry.LogEntry{}).Save(logs[lastCommitIndex+1 : updatedCommitIndex+1]).Error
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

	logs := s.getClonedInst().Logs
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

func (s *Server) resetTicker() {
	serverMu.Lock()
	defer serverMu.Unlock()
	s.ServerTicker.ticker.Reset(util.GetRandomTickerDuration(config.GetTickerIntervalInMillisecond()))
}
