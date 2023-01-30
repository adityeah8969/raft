package raft

import (
	"context"
	"errors"
	"math"
	"net/rpc"
	"sync"
	"time"

	"github.com/adityeah8969/raft/config"
	"github.com/adityeah8969/raft/types"
	"github.com/adityeah8969/raft/types/constants"
	"github.com/adityeah8969/raft/types/logEntry"
	"github.com/adityeah8969/raft/util"
)

func (s *Server) startLeading() error {
	sugar.Infof("%s started leading", s.serverId)

	clonedInst := s.getClonedInst()
	updatedNextIndexSlice := make([]int, len(s.peers))
	updatedMatchIndexSlice := make([]int, len(s.peers))
	for i := range s.peers {
		updatedNextIndexSlice[i] = len(clonedInst.Logs)
		updatedMatchIndexSlice[i] = 0
	}

	updatedAttrs := map[string]interface{}{
		"NextIndex":  updatedNextIndexSlice,
		"MatchIndex": updatedMatchIndexSlice,
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
			s.sendPeriodicHeartBeats(ctx)
		}
	}
}

func (s *Server) stopLeading() {
	serverMu.Lock()
	defer serverMu.Unlock()
	updateProcessContext(leaderContextInst, &processContext{}, leaderCtxMu)
}

func (s *Server) sendPeriodicHeartBeats(ctx context.Context) {
	heartBeatTicker := ServerTicker{
		ticker: time.NewTicker(time.Duration(config.GetTickerIntervalInMillisecond()) * time.Millisecond),
		done:   make(chan bool),
	}
	defer heartBeatTicker.ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			sugar.Infof("%s done sending periodic heartbeat", s.serverId)
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
						Term:                  clonedInst.CurrentTerm,
						LeaderId:              clonedInst.serverId,
						LeaderLastCommitIndex: clonedInst.LastComittedIndex,
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
			for serverId := range clonedInst.rpcClients {
				client := clonedInst.rpcClients[serverId].(*rpc.Client)

				serverIndex, err := util.GetServerIndex(serverId)
				if err != nil {
					sugar.Debugf("Unable to get server index in peers list for : %s", serverId)
				}

				prevEntryIndex := clonedInst.NextIndex[serverIndex] - 1
				// TODO: may have the check prevEntryIndex > 0
				prevEntryTerm := clonedInst.Logs[prevEntryIndex].Term
				bulkEntries := clonedInst.Logs[clonedInst.NextIndex[serverIndex]:]

				if len(bulkEntries) == 0 {
					continue
				}

				request := &types.RequestAppendEntryRPC{
					Term:                  clonedInst.CurrentTerm,
					LeaderId:              clonedInst.serverId,
					PrevEntryIndex:        prevEntryIndex,
					PrevEntryTerm:         prevEntryTerm,
					Entries:               bulkEntries,
					LeaderLastCommitIndex: clonedInst.LastComittedIndex,
				}

				ctx, _ = context.WithCancel(ctx)
				wg.Add(1)
				go s.makeAppendEntryCall(ctx, client, prevEntryIndex, clonedInst.Logs, request, responseChan, wg)
			}
			wg.Wait()
			updatedNextIndex := clonedInst.NextIndex
			updatedMatchIndex := clonedInst.MatchIndex

			for resp := range responseChan {
				if !resp.Success && resp.OutdatedTerm {
					// revert to being a follower
					updatedAttrs := map[string]interface{}{
						"State":       string(constants.Follower),
						"LeaderId":    resp.CurrentLeader,
						"CurrentTerm": resp.Term,
					}
					s.update(updatedAttrs)
					go s.startFollowing()
					s.stopLeading()
					return
				}
				// If successful: update nextIndex and matchIndex for the follower
				serverId := resp.ServerId
				serverIndex, err := util.GetServerIndex(serverId)
				if err != nil {
					sugar.Debugf("Unable to get serverIndex for %s", serverId)
					continue
				}
				updatedNextIndex[serverIndex] = int(math.Min(float64(resp.LastCommittedIndexInFollower+1), float64(len(clonedInst.Logs)-1)))
				updatedMatchIndex[serverIndex] = resp.LastCommittedIndexInFollower
			}
			err := s.updateCommitIndex()
			if err != nil {
				sugar.Debugf("Updating commit index in %s: %v", s.serverId, err)
			}
			updatedAttrs := map[string]interface{}{
				"NextIndex":  updatedNextIndex,
				"MatchIndex": updatedMatchIndex,
			}
			s.update(updatedAttrs)
		}
	}
}

func (s *Server) makeAppendEntryCall(ctx context.Context, client *rpc.Client, prevEntryIndex int, logs []logEntry.LogEntry, request *types.RequestAppendEntryRPC, responseChan chan *types.ResponseAppendEntryRPC, wg *sync.WaitGroup) {
	defer wg.Done()
	response := &types.ResponseAppendEntryRPC{}
	for {
		select {
		case <-ctx.Done():
			sugar.Debugw("append entry call stopped by context", "leaderId", s.serverId)
			return
		default:
			err := util.RPCWithRetry(client, "Server.AppendEntryRPC", request, response, config.GetRetryRPCLimit(), config.GetRPCTimeoutInSeconds())
			if err != nil {
				sugar.Warnw("append entry RPC calls failed", "Error", err, "leaderId", s.serverId, "rpcClient", client)
				response = &types.ResponseAppendEntryRPC{}
				responseChan <- response
				return
			}
			if response.PreviousEntryAbsent {
				request.Entries = logs[prevEntryIndex:]
				updatedPrevEntryIndex := prevEntryIndex - 1
				var updatedPrevEntryTerm int
				if updatedPrevEntryIndex < 0 {
					updatedPrevEntryTerm = -1
				} else {
					updatedPrevEntryTerm = logs[updatedPrevEntryIndex].Term
				}
				request.PrevEntryIndex = updatedPrevEntryIndex
				request.PrevEntryTerm = updatedPrevEntryTerm
				continue
			}
			responseChan <- response
			return
		}
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
	// Just check if the entry has been applied, let it be a blocking call (As per the raft paper).

	// TODO: In case there is a timeout and the client sends the same request back. Take care of that.
	timer := time.NewTimer(time.Duration(config.GetClientRequestTimeoutInSeconds()) * time.Second)

	for {
		select {
		case <-timer.C:
			sugar.Debugf("timing out while applying the entry", "serverId", s.serverId, "logEntry", log)
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
