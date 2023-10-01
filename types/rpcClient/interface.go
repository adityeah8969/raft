package rpcClient

import (
	"context"
	"time"
)

type RpcClientI interface {
	MakeRPC(context.Context, string, any, any, int, *time.Duration) error
}
