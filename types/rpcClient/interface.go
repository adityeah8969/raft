package rpcClient

import "context"

type RpcClientI interface {
	MakeRPC(context.Context, string, any, any, int) error
}
