package raft

import (
	"context"
	"math"
	"net/rpc"
	"sync"
	"time"

	"github.com/adityeah8969/raft/config"
	"github.com/adityeah8969/raft/types"
	"github.com/adityeah8969/raft/types/constants"
	"github.com/adityeah8969/raft/util"
)

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

func (s *Server) startServerTicker(ctx context.Context) {
	defer s.ServerTicker.ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			sugar.Infof("stopping ticker for server %s", s.serverId)
			return
		case t := <-s.ServerTicker.ticker.C:
			sugar.Infof("election contest started by %s at %v", s.serverId, t)
			go s.stopFollowing()
			s.startContesting()
		}
	}
}

func (s *Server) stopContesting() {
	serverMu.Lock()
	defer serverMu.Unlock()
	updateProcessContext(candidateContextInst, &processContext{}, followerCtxMu)
}

func (s *Server) startContesting() error {

	electionTimer := time.NewTimer(time.Duration(config.GetElectionTimerDurationInSec()) * time.Second)
	for {
		select {
		case <-candidateContextInst.ctx.Done():
			sugar.Infof("server %s stopped contesting election", s.serverId)
			return nil
			// Should be server ticker
		case <-electionTimer.C:

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
			close(responseChan)

			voteCnt := 0
			for resp := range responseChan {
				if resp.VoteGranted {
					voteCnt++
					continue
				}
				if resp.OutdatedTerm {
					electionTimer.Stop()
					updatedAttrs := map[string]interface{}{
						"State":       string(constants.Follower),
						"LeaderId":    resp.CurrentLeader,
						"CurrentTerm": resp.Term,
					}
					s.update(updatedAttrs)
					sugar.Infof("%s server had an outdated term as a candidate", s.serverId)
					go s.startFollowing()
					s.stopContesting()
					return nil
				}
			}

			if voteCnt >= int(math.Ceil(1.0*float64(len(s.peers)/2))) {
				electionTimer.Stop()
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
			electionTimer.Reset(time.Duration(config.GetElectionTimerDurationInSec()) * time.Second)
		}
	}
}
