package raft

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/adityeah8969/raft/config"
	"github.com/adityeah8969/raft/types"
	"github.com/adityeah8969/raft/types/constants"
	"github.com/adityeah8969/raft/types/logEntry"
	"github.com/adityeah8969/raft/types/rpcClient"
	"github.com/adityeah8969/raft/util"
)

const (
	leaderRpcMethods = 2
)

var heartBeatRPCTicker *time.Ticker

func (s *Server) prepareLeaderState() {
	serverMu.RLock()
	logs := s.Logs
	serverMu.RUnlock()
	updatedNextIndexSlice := make([]int, len(s.peers))
	updatedMatchIndexSlice := make([]int, len(s.peers))
	for i := range s.peers {
		updatedNextIndexSlice[i] = len(logs)
		updatedMatchIndexSlice[i] = 0
	}
	updatedAttrs := map[string]interface{}{
		"NextIndex":  updatedNextIndexSlice,
		"MatchIndex": updatedMatchIndexSlice,
		"State":      constants.Leader,
	}
	heartBeatRPCTicker = time.NewTicker(time.Duration(config.GetTickerIntervalInMillisecond()) * time.Millisecond)
	s.update(updatedAttrs)
}

func (s *Server) startLeading(ctx context.Context) {
	sugar.Infof("%s started leading", s.serverId)
	s.prepareLeaderState()
	leaderWg := sync.WaitGroup{}
	leaderWg.Add(leaderRpcMethods)
	go s.sendPeriodicHeartBeats(ctx, &leaderWg)
	go s.makeAppendEntryCalls(ctx, &leaderWg)
	leaderWg.Wait()
}

func (s *Server) sendPeriodicHeartBeats(ctx context.Context, leaderWg *sync.WaitGroup) {
	defer heartBeatRPCTicker.Stop()
	defer leaderWg.Done()

	for {
		select {
		case <-ctx.Done():
			sugar.Infof("%s done sending periodic heartbeat", s.serverId)
			return
		case <-heartBeatRPCTicker.C:

			serverMu.RLock()
			request := &types.RequestAppendEntryRPC{
				Term:                  s.CurrentTerm,
				LeaderId:              s.serverId,
				LeaderLastCommitIndex: s.LastComittedIndex,
			}
			serverMu.RUnlock()

			responseChan := make(chan *types.ResponseAppendEntryRPC, len(s.peers)-1)
			s.sendHeartBeatsToPeers(ctx, request, responseChan)

			for resp := range responseChan {
				if !resp.Success && resp.OutdatedTerm {
					s.revertToFollower(resp.Term, resp.CurrentLeader)
					return
				}
			}

			heartBeatRPCTicker.Reset(time.Duration(config.GetTickerIntervalInMillisecond()) * time.Millisecond)
		}
	}
}

func (s *Server) sendHeartBeatsToPeers(ctx context.Context, req *types.RequestAppendEntryRPC, responseChan chan *types.ResponseAppendEntryRPC) {
	defer close(responseChan)

	wg := sync.WaitGroup{}
	wg.Add(len(s.rpcClients))

	for ind, client := range s.rpcClients {
		if s.serverId == util.GetServerId(ind) {
			continue
		}
		go func(client rpcClient.RpcClientI) {
			defer wg.Done()
			resp := &types.ResponseAppendEntryRPC{}
			err := client.MakeRPC(ctx, "Server.AppendEntryRPC", req, resp, config.GetRpcRetryLimit(), config.GetRPCTimeoutInSeconds())
			if err != nil {
				sugar.Warnw("RPC resulted in error after retries", "rpcClient", client, "leaderId", req.LeaderId)
				return
			}
			responseChan <- resp
		}(client)
	}
	wg.Wait()
}

func (s *Server) makeAppendEntryCalls(ctx context.Context, leaderWg *sync.WaitGroup) {
	leaderWg.Done()

	for {
		select {
		case <-ctx.Done():
			sugar.Infof("%s stopped making append entry calls", s.serverId)
			return
		default:

			serverMu.Lock()
			nextIndex := s.NextIndex
			matchIndex := s.MatchIndex
			logs := s.Logs
			serverMu.RUnlock()

			responseChan := make(chan *types.ResponseAppendEntryRPC, len(s.rpcClients))
			s.makeAppendEntryCallToPeers(ctx, responseChan)
			for resp := range responseChan {
				if !resp.Success && resp.OutdatedTerm {
					s.revertToFollower(resp.Term, resp.CurrentLeader)
					return
				}
				// If successful: update nextIndex and matchIndex for the follower
				serverIndex := util.GetServerIndex(resp.ServerId)
				nextIndex[serverIndex] = int(math.Min(float64(resp.LastCommittedIndexInFollower+1), float64(len(logs)-1)))
				matchIndex[serverIndex] = resp.LastCommittedIndexInFollower
			}
			err := s.updateCommitIndex()
			if err != nil {
				sugar.Debugf("Updating commit index in %s: %v", s.serverId, err)
			}
			updatedAttrs := map[string]interface{}{
				"NextIndex":  nextIndex,
				"MatchIndex": matchIndex,
			}
			s.update(updatedAttrs)
		}
	}
}

func (s *Server) makeAppendEntryCallToPeers(ctx context.Context, responseChan chan *types.ResponseAppendEntryRPC) {
	defer close(responseChan)

	serverMu.RLock()
	nextIndex := s.NextIndex
	logs := s.Logs
	currTerm := s.CurrentTerm
	lastComittedIndex := s.LastComittedIndex
	serverMu.RUnlock()

	wg := sync.WaitGroup{}
	wg.Add(len(s.rpcClients))

	for clientIndex := range s.rpcClients {

		if s.serverId == util.GetServerId(clientIndex) {
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
	wg.Wait()
}

/*
If followers crash or run slowly, or if network packets are lost, the leader retries Append-
Entries RPCs indefinitely (even after it has responded to the client)

So increasing the retry count will help here but there is always a high finite limit to it??
Should we drop the retry and keep trying indefinitely?
*/

func (s *Server) makeAppendEntryCall(ctx context.Context, clientIndex int, logs []logEntry.LogEntry, request *types.RequestAppendEntryRPC, responseChan chan *types.ResponseAppendEntryRPC, wg *sync.WaitGroup) {
	defer wg.Done()
	response := &types.ResponseAppendEntryRPC{}
	for {
		client := s.rpcClients[clientIndex]
		err := client.MakeRPC(ctx, "Server.AppendEntryRPC", request, response, config.GetRpcRetryLimit(), config.GetRPCTimeoutInSeconds())
		if err != nil {
			sugar.Warnw("append entry RPC calls failed", "Error", err, "leaderId", s.serverId, "rpcClient", client)
			response = &types.ResponseAppendEntryRPC{}
			responseChan <- response
			return
		}
		if response.PreviousEntryAbsent {
			sugar.Infow("missing prvious entry", "leader", request.LeaderId, "rpcClient", client)
			prevEntry := getPreviousEntry(logs, request.PrevEntryIndex)
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

func (s *Server) Set(req *types.RequestEntry, resp *types.ResponseEntry) {

	serverMu.RLock()
	leaderId := s.LeaderId
	logs := s.Logs
	lastAppliedIndex := s.LastAppliedIndex
	serverMu.RUnlock()

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.GetClientRequestTimeoutInSeconds())*time.Second)
	defer cancel()

	if leaderId != s.serverId {
		s.redirectRequestToLeader(ctx, "Server.Set", req, resp)
		return
	}

	logIndex := len(logs)
	s.appendReqToLogs(req)

	// We need a way to synchronously send the response back to the client after the entry is applied to the state machine.
	// Just check if the entry has been applied, let it be a blocking call (As per the raft paper).

	err := waitTillApply(ctx, lastAppliedIndex, logIndex)
	if err != nil {
		sugar.Debugw("wait till log gets applied to state machine", "error", err, "leaderId", leaderId, "log entry", req)
		resp = &types.ResponseEntry{
			Success: false,
			Err:     err,
		}
		return
	}
	resp = &types.ResponseEntry{
		Success: true,
	}
}

func (s *Server) Get(req *types.RequestEntry, resp *types.ResponseEntry) {

	serverMu.RLock()
	leaderId := s.LeaderId
	serverMu.RUnlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// re-direct to leader
	if leaderId != s.serverId {
		s.redirectRequestToLeader(ctx, "Server.Get", req, resp)
		return
	}

	log := &logEntry.LogEntry{Entry: req}
	logEntry, err := s.stateMachine.GetEntry(log)

	if err != nil {
		resp = &types.ResponseEntry{
			Success: false,
			Err:     err,
		}
	}

	resp = &types.ResponseEntry{
		Success: false,
		Err:     nil,
		Data:    logEntry,
	}
}

func (s *Server) redirectRequestToLeader(ctx context.Context, method string, req *types.RequestEntry, resp *types.ResponseEntry) {

	serverMu.RLock()
	leaderId := s.LeaderId
	serverMu.RUnlock()

	// re-direct to leader
	if leaderId != s.serverId {
		client := s.rpcClients[util.GetServerIndex(leaderId)]
		response := &types.ResponseEntry{}
		err := client.MakeRPC(ctx, method, req, response, config.GetRpcRetryLimit(), config.GetRPCTimeoutInSeconds())
		if err != nil {
			sugar.Warnw("set entry failed after retries", "serverId", s.serverId, "leaderId", s.LeaderId, "rpcClient", client, "request", req, "response", response)
			response = &types.ResponseEntry{
				Success: false,
				Err:     err,
			}
		}
	}
}

func (s *Server) appendReqToLogs(req *types.RequestEntry) {
	serverMu.Lock()
	defer serverMu.Unlock()
	log := logEntry.LogEntry{
		Term:  s.CurrentTerm,
		Index: len(s.Logs),
		Entry: req,
	}
	s.Logs = append(s.Logs, log)
}

func (s *Server) appendRPCEntriesToLogs(logs []logEntry.LogEntry) {
	serverMu.Lock()
	defer serverMu.Unlock()
	s.Logs = append(s.Logs, logs...)
}

func waitTillApply(ctx context.Context, lastAppliedIndex int, logIndex int) error {
	for {
		select {
		case <-ctx.Done():
			return errors.New("context done called")
		default:
			if lastAppliedIndex < logIndex {
				continue
			}
			return nil
		}
	}
}

func getPreviousEntry(logs []logEntry.LogEntry, currEntryIndex int) *logEntry.LogEntry {
	prevEntryIndex := currEntryIndex - 1
	if prevEntryIndex < 0 {
		return &logEntry.LogEntry{
			Term:  -1,
			Index: -1,
		}
	}
	return &logs[prevEntryIndex]
}
