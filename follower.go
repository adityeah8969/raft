package raft

import (
	"context"

	"github.com/adityeah8969/raft/types/constants"
)

func (s *Server) startServerTicker(ctx context.Context) {
	defer s.ServerTicker.ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			sugar.Infof("stopping ticker for server %s", s.serverId)
			return
		case t := <-s.ServerTicker.ticker.C:
			sugar.Infof("election contest started by %s at %v", s.serverId, t)
			s.updateState(constants.Follower, constants.Candidate)
		}
	}
}

func (s *Server) startFollowing(ctx context.Context) {
	updateAttrs := make(map[string]interface{})
	updateAttrs["State"] = constants.Follower
	s.update(updateAttrs)
	go s.startServerTicker(ctx)
}
