package raft

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/adityeah8969/raft/config"
	"github.com/adityeah8969/raft/types"
	"github.com/adityeah8969/raft/types/logEntry"
	"github.com/adityeah8969/raft/types/rpcClient"
	"github.com/adityeah8969/raft/util"
)

const (
	leaderRpcMethods = 2
)

func (s *Server) prepareLeaderState() (map[string]interface{}, error) {
	s.serverMu.RLock()
	logs := s.Logs
	s.serverMu.RUnlock()
	// make peer length a constant outside
	updatedNextIndexSlice := make([]int, len(s.peers)+1)
	updatedMatchIndexSlice := make([]int, len(s.peers)+1)
	for i := range s.peers {
		updatedNextIndexSlice[i] = len(logs)
		updatedMatchIndexSlice[i] = 0
	}
	updatedAttrs := map[string]interface{}{
		"NextIndex":  updatedNextIndexSlice,
		"MatchIndex": updatedMatchIndexSlice,
	}
	return updatedAttrs, nil
}

func (s *Server) startLeading(ctx context.Context) {
	s.logger.Infof("%s started leading", s.serverId)
	leaderWg := sync.WaitGroup{}
	leaderWg.Add(leaderRpcMethods)
	go s.sendPeriodicHeartBeats(ctx, &leaderWg)
	duration := time.Duration(config.GetHeartBeatTickerIntervalInMilliseconds()) * time.Millisecond
	s.resetLeaderTicker(duration, true)
	go s.makeAppendEntryCalls(ctx, &leaderWg)
	leaderWg.Wait()
}

func (s *Server) sendPeriodicHeartBeats(ctx context.Context, leaderWg *sync.WaitGroup) {
	defer s.leaderTicker.Stop()
	defer leaderWg.Done()
	for {
		select {
		case <-ctx.Done():
			s.logger.Infof("%s done sending periodic heartbeat", s.serverId)
			return
		case <-s.leaderTicker.C:
			s.serverMu.RLock()
			request := &types.RequestAppendEntryRPC{
				Term:                  s.CurrentTerm,
				LeaderId:              s.serverId,
				LeaderLastCommitIndex: s.LastComittedIndex,
			}
			s.serverMu.RUnlock()

			responseChan := make(chan *types.ResponseAppendEntryRPC, len(s.peers)-1)
			resp := s.sendHeartBeatsToPeers(ctx, request, responseChan)
			if resp.IsOutdatedTerm {
				return
			}
			duration := time.Duration(config.GetHeartBeatTickerIntervalInMilliseconds()) * time.Millisecond
			s.resetLeaderTicker(duration, true)
		}
	}
}

func (s *Server) sendHeartBeatsToPeers(ctx context.Context, req *types.RequestAppendEntryRPC, responseChan chan *types.ResponseAppendEntryRPC) *types.ResonseProcessRPC {
	startTime := time.Now()

	s.logger.Debugw("sending heartbeats to peers", "leader", s.serverId)
	wg := sync.WaitGroup{}

	for ind, client := range s.rpcClients {
		peerID := util.GetServerId(ind)
		if s.serverId == peerID {
			continue
		}
		wg.Add(1)
		go func(client rpcClient.RpcClientI, peerID string) {
			defer wg.Done()
			childCtx, cancel := context.WithTimeout(ctx, time.Duration(config.GetRpcTimeoutInSeconds()*int(time.Second)))
			defer cancel()
			resp := &types.ResponseAppendEntryRPC{}
			s.logger.Debugw("sending heartbeat", "sender", s.serverId, "receiver", peerID)
			err := client.MakeRPC(childCtx, "Server.AppendEntryRPC", req, resp, config.GetRpcRetryLimit(), nil)
			if err != nil {
				s.logger.Errorw("rpc error", "rpcClient", client, "leaderId", req.LeaderId)
				return
			}
			responseChan <- resp
		}(client, peerID)
	}
	go func() {
		wg.Wait()
		close(responseChan)
	}()
	for resp := range responseChan {
		s.logger.Debugf("heartbeat response: %v", resp)
		if !resp.Success && resp.OutdatedTerm {
			s.logger.Debugw("outdated term response received, reverting to follower", "existing leader", s.serverId, "new leader", resp.CurrentLeader, "new term", resp.Term, "sent by", resp.ServerId)
			go s.revertToFollower(resp.Term, resp.CurrentLeader)
			return &types.ResonseProcessRPC{
				IsOutdatedTerm: true,
			}
		}
	}
	s.logger.Debug("gone through all the heart beat responses")
	heartBeatsDispatchTime := time.Since(startTime)
	s.logger.Debugf("total heartBeatsDispatchTime: %v", heartBeatsDispatchTime)
	return &types.ResonseProcessRPC{
		IsOutdatedTerm: false,
	}
}

// should makeAppendEntryCalls be interval based ? Or how about using this exact flow form heartbeats ?
func (s *Server) makeAppendEntryCalls(ctx context.Context, leaderWg *sync.WaitGroup) {
	leaderWg.Done()

	for {
		select {
		case <-ctx.Done():
			s.logger.Infof("%s stopped making append entry calls", s.serverId)
			return
		default:
			s.logger.Debugw("making append entry calls to peers", "server", s.serverId)

			resp, err := s.makeAppendEntryCallToPeers(ctx)
			if err != nil {
				s.logger.Errorw("making append entry calls to peers", "server", s.serverId, "error", err)
			}

			if resp.IsOutdatedTerm {
				s.logger.Debugw("outdated term received while making append entry calls to peers", "server", s.serverId)
				return
			}

			err = s.updateLeaderIndexes(resp, true)
			if err != nil {
				s.logger.Debugf("Updating commit index in %s: %v", s.serverId, err)
			}

		}
	}
}

func (s *Server) makeAppendEntryCallToPeers(ctx context.Context) (*types.ResonseProcessRPC, error) {
	responseChan := make(chan *types.ResponseAppendEntryRPC, len(s.rpcClients))

	s.serverMu.RLock()
	nextIndex := s.NextIndex
	matchIndex := s.MatchIndex
	logs := s.Logs
	currTerm := s.CurrentTerm
	lastComittedIndex := s.LastComittedIndex
	s.serverMu.RUnlock()

	wg := sync.WaitGroup{}
	wg.Add(len(s.rpcClients))

	go func() {
		wg.Wait()
		close(responseChan)
	}()

	for clientIndex := range s.rpcClients {

		if s.serverId == util.GetServerId(clientIndex) {
			continue
		}

		if nextIndex[clientIndex] > len(logs)-1 {
			continue
		}

		bulkEntries := logs[nextIndex[clientIndex]:]
		if len(bulkEntries) == 0 {
			continue
		}

		prevEntry := getPreviousEntry(logs, nextIndex[clientIndex])

		request := &types.RequestAppendEntryRPC{
			Term:                  currTerm,
			LeaderId:              s.serverId,
			PrevEntryIndex:        prevEntry.Index,
			PrevEntryTerm:         prevEntry.Term,
			Entries:               bulkEntries,
			LeaderLastCommitIndex: lastComittedIndex,
		}

		go s.makeAppendEntryCall(ctx, clientIndex, logs, request, responseChan, &wg)
	}

	for resp := range responseChan {
		if !resp.Success && resp.OutdatedTerm {
			go s.revertToFollower(resp.Term, resp.CurrentLeader)
			return &types.ResonseProcessRPC{
				IsOutdatedTerm: true,
			}, nil
		}
		// If successful: update nextIndex and matchIndex for the follower
		serverIndex := util.GetServerIndex(resp.ServerId)
		nextIndex[serverIndex] = int(math.Min(float64(resp.LastCommittedIndexInFollower+1), float64(len(logs)-1)))
		matchIndex[serverIndex] = resp.LastCommittedIndexInFollower
	}

	return &types.ResonseProcessRPC{
		NextIndex:  nextIndex,
		MatchIndex: matchIndex,
	}, nil
}

func (s *Server) makeAppendEntryCall(ctx context.Context, clientIndex int, logs []logEntry.LogEntry, request *types.RequestAppendEntryRPC, responseChan chan *types.ResponseAppendEntryRPC, wg *sync.WaitGroup) {
	defer wg.Done()
	response := &types.ResponseAppendEntryRPC{}
	for {
		client := s.rpcClients[clientIndex]
		childCtx, cancel := context.WithTimeout(ctx, time.Duration(config.GetRpcTimeoutInSeconds()*int(time.Second)))
		defer cancel()
		err := client.MakeRPC(childCtx, "Server.AppendEntryRPC", request, response, config.GetRpcRetryLimit(), nil)
		if err != nil {
			s.logger.Warnw("append entry RPC calls failed", "Error", err, "leaderId", s.serverId, "rpcClient", client)
			response = &types.ResponseAppendEntryRPC{}
			responseChan <- response
			return
		}
		if !response.PreviousEntryPresent {
			s.logger.Infow("missing previous entry", "leader", request.LeaderId, "rpcClient", client)
			prevEntry := getPreviousEntry(logs, request.PrevEntryIndex)
			// no previous entry
			if prevEntry == nil {
				request.PrevEntryIndex = -1
				request.PrevEntryTerm = -1
				response = &types.ResponseAppendEntryRPC{}
				continue
			}
			request.PrevEntryIndex = prevEntry.Index
			request.PrevEntryTerm = prevEntry.Term
			request.Entries = append([]logEntry.LogEntry{*prevEntry}, request.Entries...)
			response = &types.ResponseAppendEntryRPC{}
			continue
		}
		responseChan <- response
		return
	}
}

func (s *Server) Set(ctx context.Context, req *types.RequestEntry, resp *types.ResponseEntry) error {

	s.logger.Debugw("server received request", "server", s.serverId, "req", req)
	_ = ctx
	s.serverMu.RLock()
	leaderId := s.LeaderId
	logs := s.Logs
	lastAppliedIndex := s.LastAppliedIndex
	s.serverMu.RUnlock()

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.GetClientRequestTimeoutInSeconds())*time.Second)
	defer cancel()

	if leaderId != s.serverId {
		s.logger.Debugw("server redirecting request to leader", "server", s.serverId, "leader", s.LeaderId)
		return s.redirectRequestToLeader(ctx, leaderId, "Server.Set", req, resp)
	}

	lastLogIndex := len(logs) - 1
	s.appendReqToLogs(req)

	// We need a way to synchronously send the response back to the client after the entry is applied to the state machine.
	// Just check if the entry has been applied, let it be a blocking call (As per the raft paper).

	s.logger.Debugw("server waiting for log to be applied by state machine", "server", s.serverId)
	err := waitTillApply(ctx, lastAppliedIndex, lastLogIndex)
	if err != nil {
		s.logger.Debugw("wait till log gets applied to state machine", "error", err, "leaderId", leaderId, "log entry", req)
		resp = &types.ResponseEntry{
			Success: false,
			Err:     err,
		}
		return err
	}
	resp = &types.ResponseEntry{
		Success: true,
	}
	return nil
}

func (s *Server) Get(ctx context.Context, req *types.RequestEntry, resp *types.ResponseEntry) error {

	_ = ctx

	s.serverMu.RLock()
	leaderId := s.LeaderId
	s.serverMu.RUnlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// re-direct to leader
	if leaderId != s.serverId {
		return s.redirectRequestToLeader(ctx, leaderId, "Server.Get", req, resp)
	}

	log := &logEntry.LogEntry{Entry: req}
	logEntry, err := s.stateMachine.GetEntry(log)

	if err != nil {
		resp = &types.ResponseEntry{
			Success: false,
			Err:     err,
		}
		return err
	}

	resp = &types.ResponseEntry{
		Success: false,
		Err:     nil,
		Data:    logEntry,
	}

	return nil
}

func (s *Server) redirectRequestToLeader(ctx context.Context, leaderId string, method string, req *types.RequestEntry, resp *types.ResponseEntry) error {

	childCtx, cancel := context.WithTimeout(ctx, time.Duration(config.GetRpcTimeoutInSeconds()*int(time.Second)))
	defer cancel()

	client := s.rpcClients[util.GetServerIndex(leaderId)]
	response := &types.ResponseEntry{}
	err := client.MakeRPC(childCtx, method, req, response, config.GetRpcRetryLimit(), nil)
	if err != nil {
		s.logger.Warnw("set entry failed after retries", "serverId", s.serverId, "leaderId", s.LeaderId, "rpcClient", client, "request", req, "response", response)
		response = &types.ResponseEntry{
			Success: false,
			Err:     err,
		}
		return err
	}
	return nil
}

func (s *Server) appendReqToLogs(req *types.RequestEntry) {
	s.serverMu.Lock()
	defer s.serverMu.Unlock()
	log := logEntry.LogEntry{
		Term:  s.CurrentTerm,
		Index: len(s.Logs),
		Entry: req,
	}
	s.Logs = append(s.Logs, log)
}

func (s *Server) appendRPCEntriesToLogs(logs []logEntry.LogEntry, withLock bool) {
	if withLock {
		s.serverMu.Lock()
		defer s.serverMu.Unlock()
	}
	s.Logs = append(s.Logs, logs...)
	s.logger.Debugw("appended log entries", "server", s.serverId, "logs", s.Logs)
}

func waitTillApply(ctx context.Context, lastAppliedIndex int, lastLogIndex int) error {
	// [TODO] take this from config
	tickerDuration := 10 * time.Millisecond
	waitTillApplyTicker := time.NewTicker(tickerDuration)
	for {
		select {
		case <-ctx.Done():
			return errors.New("context done called")
		case <-waitTillApplyTicker.C:
			if lastAppliedIndex < lastLogIndex {
				waitTillApplyTicker.Reset(tickerDuration)
				continue
			}
			return nil
		}
	}
}

func getPreviousEntry(logs []logEntry.LogEntry, currEntryIndex int) *logEntry.LogEntry {
	prevEntryIndex := currEntryIndex - 1
	if prevEntryIndex < 0 {
		return nil
	}
	return &logs[prevEntryIndex]
}

func (s *Server) resetLeaderTicker(d time.Duration, withLock bool) {
	if withLock {
		s.serverMu.Lock()
		defer s.serverMu.Unlock()
	}
	s.leaderTicker.Reset(d)
	s.logger.Debugf("leader ticker reset, next tick duration %#v ms", d.Milliseconds())
}

func (s *Server) updateLeaderIndexes(resp *types.ResonseProcessRPC, withLock bool) error {

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

	updateAttrs := map[string]interface{}{
		"LastComittedIndex": updatedCommitIndex,
		"NextIndex":         resp.NextIndex,
		"MatchIndex":        resp.MatchIndex,
	}
	s.update(updateAttrs, nil, withLock)

	s.logger.Debugw("updated leader indexes", "updateAttrs", updateAttrs)

	return nil
}
