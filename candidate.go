package raft

import (
	"context"
	"math"

	"sync"
	"time"

	"github.com/adityeah8969/raft/config"
	"github.com/adityeah8969/raft/types"
	"github.com/adityeah8969/raft/types/rpcClient"
	"github.com/adityeah8969/raft/util"
)

func (s *Server) voteForItself() error {

	serverMu.RLock()
	serverId := s.serverId
	currTerm := s.CurrentTerm
	serverMu.RUnlock()

	vote := &types.Vote{
		VotedFor: serverId,
		Term:     currTerm + 1,
	}
	err := s.serverDb.SaveVote(vote)
	if err != nil {
		return err
	}
	updatedAttrs := map[string]interface{}{
		"CurrentTerm": currTerm + 1,
		"VotedFor":    serverId,
	}
	s.update(updatedAttrs)
	return nil
}

func (s *Server) requestVoteFromPeers(ctx context.Context, responseChan chan *types.ResponseVoteRPC) {
	defer close(responseChan)

	serverMu.RLock()
	currTerm := s.CurrentTerm
	serverMu.RUnlock()

	var wg sync.WaitGroup
	wg.Add(len(s.peers))

	for ind, client := range s.rpcClients {
		if s.serverId == util.GetServerId(ind) {
			continue
		}
		go func(client rpcClient.RpcClientI) {
			defer wg.Done()
			request := &types.RequestVoteRPC{
				Term:        currTerm,
				CandidateId: s.serverId,
			}
			response := &types.ResponseVoteRPC{}
			err := client.MakeRPC(ctx, "Server.RequestVoteRPC", request, response, config.GetRpcRetryLimit(), config.GetRPCTimeoutInSeconds())
			if err != nil {
				sugar.Warnw("request vote RPC failed after retries", "candidate", s.serverId, "rpcClient", client, "request", request, "response", response)
				response = &types.ResponseVoteRPC{
					VoteGranted: false,
				}
			}
			responseChan <- response
		}(client)
	}
	wg.Wait()
}

func (s *Server) startContesting(ctx context.Context) {

	electionTimer := time.NewTimer(time.Duration(util.GetRandomInt(config.GetMaxElectionTimeOutInSec(), config.GetMinElectionTimeOutInSec())) * time.Second)

	for {
		select {
		case <-ctx.Done():
			sugar.Infof("server %s stopped contesting election", s.serverId)
			return
		case <-electionTimer.C:

			voteCnt := 0
			err := s.voteForItself()
			if err != nil {
				sugar.Debugw("server %s voting for itsefl", "candidate id", s.serverId, "err", err)
				continue
			}
			voteCnt++

			responseChan := make(chan *types.ResponseVoteRPC, len(s.peers)-1)
			go s.requestVoteFromPeers(ctx, responseChan)

			for resp := range responseChan {
				if resp.VoteGranted {
					voteCnt++
					if voteCnt >= int(math.Ceil(1.0*float64(len(s.peers)/2))) {
						electionTimer.Stop()
						s.revertToLeader()
						return
					}
					continue
				}
				if resp.OutdatedTerm {
					electionTimer.Stop()
					s.revertToFollower(resp.Term, resp.CurrentLeader)
					return
				}
			}

			electionTimer.Reset(time.Duration(util.GetRandomInt(config.GetMaxElectionTimeOutInSec(), config.GetMinElectionTimeOutInSec())) * time.Second)
		}
	}
}
