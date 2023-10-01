package raft

import (
	"context"
	"time"

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
	duration := time.Duration(util.GetRandomInt(config.GetMinTickerIntervalInMillisecond(), config.GetMaxTickerIntervalInMillisecond())) * time.Millisecond
	s.resetFollowerTicker(duration, true)
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

func (s *Server) resetFollowerTicker(d time.Duration, withLock bool) {
	if withLock {
		s.serverMu.Lock()
		defer s.serverMu.Unlock()
	}
	s.followerTicker.Reset(d)
	s.logger.Debugf("follower ticker reset, next tick duration %#v ms", d.Milliseconds())
}
