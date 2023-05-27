package raft

import (
	"context"
	"time"

	"github.com/adityeah8969/raft/config"
	"github.com/adityeah8969/raft/types/constants"
	"github.com/adityeah8969/raft/util"
)

var followerTicker *time.Ticker
var minTickerIntervalInMs, maxTickerIntervalInMs int

func init() {
	minTickerIntervalInMs = config.GetMinTickerIntervalInMillisecond()
	maxTickerIntervalInMs = config.GetMaxTickerIntervalInMillisecond()
}

func (s *Server) startFollowing(ctx context.Context) {
	s.prepareFollowerState()
	s.startServerTicker(ctx)
}

func (s *Server) prepareFollowerState() {
	updateAttrs := map[string]interface{}{
		"State": constants.Follower,
	}
	followerTicker = time.NewTicker(util.GetRandomTickerDuration(minTickerIntervalInMs, maxTickerIntervalInMs))
	s.update(updateAttrs)
}

func (s *Server) startServerTicker(ctx context.Context) {
	defer followerTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			sugar.Infof("stopping ticker for server %s", s.serverId)
			return
		case t := <-followerTicker.C:
			sugar.Infof("election contest started by %s at %v", s.serverId, t)
			s.updateState(constants.Candidate, nil)
			return
		}
	}
}

func (s *Server) resetFollowerTicker() {
	followerTicker.Reset(util.GetRandomTickerDuration(minTickerIntervalInMs, maxTickerIntervalInMs))
}
