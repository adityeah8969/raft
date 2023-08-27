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
	updateAttrs := make(map[string]interface{}, 0)
	return updateAttrs, nil
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
			request := &types.RequestVoteRPC{
				Term:        currTerm,
				CandidateId: s.serverId,
			}
			var response types.ResponseVoteRPC
			s.logger.Debugw("requesting vote", "candidateID", s.serverId, "voterID", peerID)
			err := client.MakeRPC(childCtx, "Server.RequestVoteRPC", request, &response, config.GetRpcRetryLimit())
			if err != nil {
				s.logger.Errorw("request vote RPC failed after retries", "err", err, "candidateID", s.serverId, "voterID", peerID, "request", request, "response", response)
				response.VoteGranted = false
			}
			// Is there a chance of pushing to a closed channel here ?
			ch <- &response
		}(client, peerID)
	}
}

func (s *Server) startContesting(ctx context.Context) {

	for {
		select {
		case <-ctx.Done():
			s.logger.Infof("server %s stopped contesting election", s.serverId)
			return
		default:

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
			s.processVoteResponse(ch, &voteCnt)

			elapsedDuration := time.Since(startTime)
			s.logger.Debugw("current election contest over", "time elapsed", elapsedDuration, "candidateID", s.serverId)

			timeRemainingForNextElection := time.Duration(config.GetMaxElectionTimeOutInSec())*time.Second - elapsedDuration
			if timeRemainingForNextElection > 0 {
				s.logger.Debugf("time remaining for next election %v", timeRemainingForNextElection)
				time.Sleep(timeRemainingForNextElection)
			}
		}
	}
}

func (s *Server) processVoteResponse(ch chan *types.ResponseVoteRPC, voteCnt *int) {
	minVoteCount := int(math.Ceil(1.0 * float64((len(s.peers) + 1)) / 2))
	for resp := range ch {
		if resp.VoteGranted {
			*voteCnt++
			if *voteCnt >= minVoteCount {
				s.logger.Debugw("processed vote responses", "election status", ElectionCompletedState, "result", ElectionResultWon, "vote count", fmt.Sprintf("%v / %v", *voteCnt, len(s.peers)+1))
				s.revertToLeader()
			}
		}
		if resp.OutdatedTerm {
			s.logger.Debugw("processed vote responses", "election status", ElectionCompletedState, "result", ElectionResultLost, "encountered outdated term", resp)
			s.revertToFollower(resp.Term, resp.CurrentLeader)
		}
		s.logger.Debugw("processing vote responses", "election status", ElectionInProgresState, "vote count", fmt.Sprintf("%v / %v", *voteCnt, len(s.peers)+1))
	}
}
