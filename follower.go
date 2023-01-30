package raft

import (
	"context"

	"github.com/adityeah8969/raft/config"
	"github.com/adityeah8969/raft/types/constants"
	"github.com/adityeah8969/raft/util"
)

func (s *Server) startFollowing() {
	s.State = string(constants.Follower)
	// create a util for the following
	ctx, cancel := context.WithCancel(context.Background())
	updatedCtx := &processContext{
		ctx:    ctx,
		cancel: cancel,
	}
	updateProcessContext(followerContextInst, updatedCtx, followerCtxMu)
	ctx, _ = context.WithCancel(followerContextInst.ctx)
	go s.startServerTicker(ctx)
}

func (s *Server) stopFollowing() {
	serverMu.Lock()
	defer serverMu.Unlock()
	updateProcessContext(followerContextInst, &processContext{}, followerCtxMu)
}

func (s *Server) resetTicker() {
	serverMu.Lock()
	defer serverMu.Unlock()
	s.ServerTicker.ticker.Reset(util.GetRandomTickerDuration(config.GetTickerIntervalInMillisecond()))
}
