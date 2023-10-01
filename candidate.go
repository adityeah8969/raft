package raft

import (
	"context"
	"fmt"
	"math"
	"time"

	"sync"

	"github.com/adityeah8969/raft/config"
	"github.com/adityeah8969/raft/types"
	"github.com/adityeah8969/raft/types/rpcClient"
	"github.com/adityeah8969/raft/util"
)

const (
	ElectionInProgresState = "in progress"
	ElectionCompletedState = "completed"

	ElectionResultWon  = "won"
	ElectionResultLost = "lost"
)

func (s *Server) prepareCandidateState() (map[string]interface{}, error) {
	duration := time.Duration(config.GetInitialElectionTickerInMilliseconds()) * time.Millisecond
	s.resetCandidateTicker(duration, true)
	updateAttrs := map[string]interface{}{
		"LeaderId": "",
	}
	return updateAttrs, nil
}

func (s *Server) startContesting(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			s.logger.Infof("server %s stopped contesting election", s.serverId)
			return
		case <-s.candidateTicker.C:
			startTime := time.Now()
			s.logger.Infow("contesting election", "candidate", s.serverId, "term", s.CurrentTerm+1)
			ctx, cancel := context.WithTimeout(ctx, time.Duration(config.GetMaxElectionTimeOutInSec()*int(time.Second)))
			defer cancel()

			err := s.voteForItself()
			if err != nil {
				s.logger.Debugw("contesting election", "candidateID", s.serverId, "error voting for itself", err)
				continue
			}
			voteCnt := 1
			ch := make(chan *types.ResponseVoteRPC, len(s.peers))
			wg := sync.WaitGroup{}
			go s.requestVoteFromPeers(ctx, ch, &wg)
			resp := s.processVoteResponse(ch, &voteCnt)
			if resp.HasWon || resp.IsOutdatedTerm {
				return
			}
			elapsedDuration := time.Since(startTime)
			s.logger.Debugw("current election contest over, waiting for next election", "time elapsed", elapsedDuration, "candidateID", s.serverId, "term", s.CurrentTerm, "election status", ElectionCompletedState, "result", ElectionResultLost)
			duration := time.Duration(config.GetElectionTickerInMilliseconds()) * time.Millisecond
			s.resetCandidateTicker(duration, true)
		}
	}
}

func (s *Server) processVoteResponse(ch chan *types.ResponseVoteRPC, voteCnt *int) *types.ResonseProcessRPC {
	minVoteCount := int(math.Ceil(1.0 * float64((len(s.peers) + 1)) / 2))
	for resp := range ch {
		if resp.VoteGranted {
			*voteCnt++
			if *voteCnt >= minVoteCount {
				s.logger.Debugw("processed vote responses", "election status", ElectionCompletedState, "result", ElectionResultWon, "term", s.CurrentTerm, "vote count", fmt.Sprintf("%v / %v", *voteCnt, len(s.peers)+1))
				go s.revertToLeader()
				return &types.ResonseProcessRPC{
					HasWon:         true,
					IsOutdatedTerm: false,
				}
			}
		}
		if resp.OutdatedTerm {
			s.logger.Debugw("processed vote responses", "election status", ElectionCompletedState, "result", ElectionResultLost, "term", s.CurrentTerm, "encountered outdated term", resp)
			go s.revertToFollower(resp.Term, resp.CurrentLeader)
			return &types.ResonseProcessRPC{
				HasWon:         false,
				IsOutdatedTerm: true,
			}
		}
		s.logger.Debugw("processing vote responses", "election status", ElectionInProgresState, "vote count", fmt.Sprintf("%v / %v", *voteCnt, len(s.peers)+1))
	}
	return &types.ResonseProcessRPC{
		HasWon:         false,
		IsOutdatedTerm: false,
	}
}

func (s *Server) voteForItself() error {
	s.serverMu.Lock()
	defer s.serverMu.Unlock()

	vote := &types.Vote{
		VotedFor: s.serverId,
		Term:     s.CurrentTerm + 1,
	}
	err := s.serverDb.SaveVote(vote)
	if err != nil {
		return err
	}

	updateAttrs := map[string]interface{}{
		"CurrentTerm": s.CurrentTerm + 1,
		"VotedFor":    s.serverId,
	}
	s.update(updateAttrs, nil, false)
	s.logger.Debugf("%v voted for itself", s.serverId)
	return nil
}

func (s *Server) requestVoteFromPeers(ctx context.Context, ch chan *types.ResponseVoteRPC, wg *sync.WaitGroup) {

	go func() {
		wg.Wait()
		close(ch)
	}()

	s.serverMu.RLock()
	currTerm := s.CurrentTerm
	s.serverMu.RUnlock()

	rpcRetryInterval := time.Duration(config.GetRpcRetryIntervalInMilliSeconds()) * time.Millisecond
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
			request := &types.RequestVoteRPC{
				Term:        currTerm,
				CandidateId: s.serverId,
			}
			var response types.ResponseVoteRPC
			s.logger.Debugw("requesting vote", "candidateID", s.serverId, "voterID", peerID)
			err := client.MakeRPC(childCtx, "Server.RequestVoteRPC", request, &response, config.GetRpcRetryLimit(), &rpcRetryInterval)
			if err != nil {
				s.logger.Errorw("request vote RPC", "err", err, "candidateID", s.serverId, "voterID", peerID, "request", request, "response", response)
				response.VoteGranted = false
			}
			// push to closed channel observed here
			ch <- &response
		}(client, peerID)
	}
}

func (s *Server) resetCandidateTicker(d time.Duration, withLock bool) {
	if withLock {
		s.serverMu.Lock()
		defer s.serverMu.Unlock()
	}
	s.candidateTicker.Reset(d)
	s.logger.Debugf("candidate ticker reset, next tick duration %#v ms", d.Milliseconds())
}
