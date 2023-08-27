package raft

import (
	"context"

	"github.com/adityeah8969/raft/config"
	"github.com/adityeah8969/raft/types/constants"
	"github.com/adityeah8969/raft/util"
)

func (s *Server) prepareFollowerState() (map[string]interface{}, error) {
	updateAttrs := map[string]interface{}{
		"State": constants.Follower,
	}
	return updateAttrs, nil
}

func (s *Server) startFollowing(ctx context.Context) {
	s.logger.Debugf("%v started following", s.serverId)
	s.resetFollowerTicker(true)
	s.startServerTicker(ctx)
}

func (s *Server) startServerTicker(ctx context.Context) {
	defer s.followerTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			s.logger.Debugw("ctx done on follower ticker", "server", s.serverId)
			return
		case <-s.followerTicker.C:
			s.revertToCandidate()
			return
		}
	}
}

func (s *Server) resetFollowerTicker(withLock bool) {
	if withLock {
		s.serverMu.Lock()
		defer s.serverMu.Unlock()
	}
	
	nextTickDuration := 1000 * util.GetRandomTickerDuration(config.GetMinTickerIntervalInMillisecond(), config.GetMaxTickerIntervalInMillisecond())
	s.followerTicker.Reset(nextTickDuration)
	s.logger.Debugf("nextTickDuration: %#v", nextTickDuration)
	s.logger.Debugw("follower ticker reset", "followerID", s.serverId, "nextTickDuration", nextTickDuration)
}
