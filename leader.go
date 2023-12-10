package raft

import (
	"context"
	"errors"
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
	// Is this re-initialization correct ?
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
			childCtx, cancel := context.WithTimeout(ctx, time.Duration(config.GetRpcTimeoutInSeconds())*time.Second)
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

// how about using this exact flow for heartbeats as well ? should makeAppendEntryCalls be interval based ?
func (s *Server) makeAppendEntryCalls(ctx context.Context, leaderWg *sync.WaitGroup) {
	leaderWg.Done()

	for {
		select {
		case <-ctx.Done():
			s.logger.Infof("%s stopped making append entry calls", s.serverId)
			return
		default:
			s.logger.Debugw("make append entry calls to peers started", "serverID", s.serverId)

			resp, err := s.makeAppendEntryCallToPeers(ctx)
			if err != nil {
				s.logger.Errorw("making append entry calls to peers", "serverID", s.serverId, "error", err)
			}

			if resp.IsOutdatedTerm {
				s.logger.Debugw("outdated term received while making append entry calls to peers", "serverID", s.serverId)
				continue
			}

			if resp.RPCsFired == 0 {
				s.logger.Debugw("no RPCs fired as the followers are up to date", "serverID", s.serverId)
				continue
			}

			err = s.updateLeaderIndexes(resp, true)
			if err != nil {
				s.logger.Debugf("Updating commit index in %s: %v", s.serverId, err)
			}

			s.logger.Debugw("make append entry calls to peers stopped", "server", s.serverId)
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

	for clientIndex := range s.rpcClients {

		if s.serverId == util.GetServerId(clientIndex) {
			continue
		}

		if nextIndex[clientIndex] == len(logs) {
			s.logger.Debugw("no new entries to append", "serverID", s.serverId, "clientIndex", clientIndex, "nextIndex[clientIndex]", nextIndex[clientIndex], "len(logs)-1", len(logs)-1)
			continue
		}

		bulkEntries := logs[nextIndex[clientIndex]:]

		// Is previous entry working fine with bulk entries ?
		prevEntry := getPreviousEntry(logs, nextIndex[clientIndex])

		request := &types.RequestAppendEntryRPC{
			Term:                  currTerm,
			LeaderId:              s.serverId,
			Entries:               bulkEntries,
			LeaderLastCommitIndex: lastComittedIndex,
		}

		if prevEntry == nil {
			request.PrevEntryIndex = -1
			request.PrevEntryTerm = -1
		} else {
			request.PrevEntryIndex = prevEntry.Index
			request.PrevEntryTerm = prevEntry.Term
		}

		wg.Add(1)
		go s.makeAppendEntryCall(ctx, clientIndex, logs, request, responseChan, &wg)
	}

	go func() {
		wg.Wait()
		close(responseChan)
	}()

	processResp := &types.ResonseProcessRPC{}
	for resp := range responseChan {
		if !resp.Success {
			if resp.OutdatedTerm {
				go s.revertToFollower(resp.Term, resp.CurrentLeader)
				return &types.ResonseProcessRPC{
					IsOutdatedTerm: true,
				}, nil
			}
			// To handle future inconsistencies
			continue
		}
		processResp.RPCsFired++
		// If successful: update nextIndex and matchIndex for the follower
		serverIndex := util.GetServerIndex(resp.ServerId)
		matchIndex[serverIndex] = resp.LastAppendedIndexInFollower
		// next index could potentially equal len(logs)
		nextIndex[serverIndex] = util.Min(resp.LastAppendedIndexInFollower+1, len(logs))
	}

	processResp.NextIndex = nextIndex
	processResp.MatchIndex = matchIndex

	return processResp, nil
}

func (s *Server) makeAppendEntryCall(ctx context.Context, clientIndex int, logs []logEntry.LogEntry, request *types.RequestAppendEntryRPC, responseChan chan *types.ResponseAppendEntryRPC, wg *sync.WaitGroup) {

	defer wg.Done()
	response := &types.ResponseAppendEntryRPC{}
	for {
		client := s.rpcClients[clientIndex]
		childCtx, cancel := context.WithTimeout(ctx, time.Duration(config.GetRpcTimeoutInSeconds())*time.Second)
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

func (s *Server) Set(ctx context.Context, req *types.RequestAppendEntryRPC, resp *types.ResponseAppendEntryRPC) error {

	s.logger.Debugw("server received request", "server", s.serverId, "req", req)
	_ = ctx
	s.serverMu.RLock()
	leaderId := s.LeaderId
	s.serverMu.RUnlock()

	// ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.GetClientRequestTimeoutInSeconds())*time.Second)
	// defer cancel()

	if leaderId != s.serverId {
		s.logger.Debugw("server redirecting request to leader", "server", s.serverId, "leader", s.LeaderId)
		// Using context of  the client does not make sense in case of 'Set' RPC as it ties down the subsequent internal RPCs to client. So using a new context here.
		return s.redirectRequestToLeader(context.Background(), leaderId, "Server.Set", req, resp)
	}

	appendedLogs := s.appendRPCEntriesToLogs(req, true)

	// We need a way to synchronously send the response back to the client after the entry is applied to the state machine.
	// Just check if the entry has been applied, let it be a blocking call (As per raft paper).
	s.logger.Debugw("server waiting for logs to be applied by state machine", "server", s.serverId)
	err := s.waitTillApply(ctx, appendedLogs)
	if err != nil {
		s.logger.Debugw("wait till log gets applied to state machine", "error", err, "leaderId", leaderId, "log entry", req)
		resp = &types.ResponseAppendEntryRPC{
			Success: false,
		}
		return err
	}
	s.logger.Debugw("successfully set entries", "server", s.serverId)
	resp.Success = true
	return nil
}

// Fix this method -> params + return type
func (s *Server) Get(ctx context.Context, req *types.RequestAppendEntryRPC, resp *types.ResponseAppendEntryRPC) error {

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

	_ = logEntry

	if err != nil {
		resp = &types.ResponseAppendEntryRPC{
			Success: false,
		}
		return err
	}

	resp = &types.ResponseAppendEntryRPC{
		Success: false,
	}

	return nil
}

func (s *Server) redirectRequestToLeader(ctx context.Context, leaderId string, method string, req *types.RequestAppendEntryRPC, resp *types.ResponseAppendEntryRPC) error {

	childCtx, cancel := context.WithTimeout(ctx, time.Duration(config.GetRpcTimeoutInSeconds())*time.Second)
	defer cancel()

	client := s.rpcClients[util.GetServerIndex(leaderId)]
	err := client.MakeRPC(childCtx, method, req, resp, config.GetRpcRetryLimit(), nil)
	if err != nil {
		s.logger.Warnw("RPC failed after retries", "err", err, "method", method, "serverID", s.serverId, "leaderID", s.LeaderId, "request", req)
		resp.Success = false
		return err
	}
	return nil
}

func (s *Server) appendRPCEntriesToLogs(req *types.RequestAppendEntryRPC, withLock bool) []logEntry.LogEntry {
	if withLock {
		s.logger.Debugw("acquired lock successfully", "serverID", s.serverId)
		s.serverMu.Lock()
		defer s.serverMu.Unlock()
	}

	appendedLogs := make([]logEntry.LogEntry, 0)
	for _, entry := range req.Entries {
		log := logEntry.LogEntry{
			Term:  s.CurrentTerm,
			Index: len(s.Logs),
			Entry: entry.Entry,
		}
		s.Logs = append(s.Logs, log)
		appendedLogs = append(appendedLogs, log)
	}
	// s.LastComittedIndex = len(s.Logs) - 1
	s.logger.Debugw("appended logs successfully", "serverID", s.serverId, "returning last appended log index", len(s.Logs)-1, "current logs", s.Logs)
	return appendedLogs
}

func (s *Server) waitTillApply(ctx context.Context, appendedLogs []logEntry.LogEntry) error {
	// TODO: take this from config
	tickerDuration := 5 * time.Millisecond
	waitTillApplyTicker := time.NewTicker(tickerDuration)
	for {
		select {
		case <-ctx.Done():
			return errors.New("context done called")
		case <-waitTillApplyTicker.C:

			s.serverMu.RLock()
			logs := s.Logs
			lastAppliedIndex := s.LastAppliedIndex
			s.serverMu.RUnlock()

			lastAppendedLogIndex := appendedLogs[len(appendedLogs)-1].Index
			if lastAppliedIndex < lastAppendedLogIndex {
				s.logger.Debugw("waiting till log gets applied", "serverID", s.serverId, "lastAppliedIndex", lastAppliedIndex, "lastAppendedLogIndex", lastAppendedLogIndex)
				waitTillApplyTicker.Reset(tickerDuration)
				continue
			}

			for _, log := range appendedLogs {
				index := log.Index
				if index > len(logs)-1 || index < 0 {
					return errors.New("appended logs with invalid index found")
				}
				same, err := s.areEntriesSame(&log, &logs[index])
				if err != nil {
					return err
				}
				if !same {
					s.logger.Infow("entries do not match", "appendedEntry", log, "appliedEntry", logs[index])
					return errors.New("appended log entry does not match with applied log entry")
				}
			}
			return nil
		}
	}
}

func (s *Server) areEntriesSame(appendedLogEntry, appliedLogEntry *logEntry.LogEntry) (bool, error) {

	s.logger.Debugf("appendedLogEntry: %#v\n", appendedLogEntry)
	s.logger.Debugf("appliedLogEntry: %#v\n", appliedLogEntry)

	appendedEntry, err := s.stateMachine.GetStateMcLogFromLogEntry(appendedLogEntry)
	if err != nil {
		s.logger.Debugw("unable to cast into appendedEntry", "err", err, "serverID", s.serverId, "appendedEntry", appendedEntry)
		return false, errors.New("unable to cast into appendedEntry")
	}
	appliedEntry, err := s.stateMachine.GetStateMcLogFromLogEntry(appliedLogEntry)
	if err != nil {
		s.logger.Debugw("unable to cast into appliedEntry", "serverID", s.serverId, "appliedEntry", appliedEntry)
		return false, errors.New("unable to cast into appliedEntry")
	}
	if appliedEntry != appendedEntry {
		return false, nil
	}
	return true, nil
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

	s.logger.Debugw("updating leader indexes", "serverID", s.serverId)
	updateAttrs := map[string]interface{}{
		"NextIndex":  resp.NextIndex,
		"MatchIndex": resp.MatchIndex,
	}

	s.serverMu.RLock()
	logs := s.Logs
	lastCommitIndex := s.LastComittedIndex
	currTerm := s.CurrentTerm
	peers := s.peers
	s.serverMu.RUnlock()

	var updatedCommitIndex *int
	for i := len(logs) - 1; i > lastCommitIndex; i-- {
		cnt := 0
		for _, ind := range resp.MatchIndex {
			if ind >= i {
				cnt++
			}
		}
		if cnt > len(peers)/2 && logs[i].Term == currTerm {
			updatedCommitIndex = &i
			break
		}
	}

	if updatedCommitIndex != nil {
		updateAttrs["LastComittedIndex"] = *updatedCommitIndex
		s.logger.Debugw("comitting logs", "serverID", s.serverId, "logs", logs[lastCommitIndex+1:*updatedCommitIndex+1])
		err := s.serverDb.SaveLogs(logs[lastCommitIndex+1 : *updatedCommitIndex+1])
		if err != nil {
			return err
		}
	}

	s.update(updateAttrs, nil, withLock)
	s.logger.Debugw("updated leader indexes", "updateAttrs", updateAttrs)
	return nil
}
