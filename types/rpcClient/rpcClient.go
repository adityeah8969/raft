package rpcClient

import (
	"context"
	"time"

	"github.com/adityeah8969/raft/types/logger"
	"github.com/keegancsmith/rpc"
	"go.uber.org/zap"
)

type rpcClient struct {
	client *rpc.Client
}

var sugar *zap.SugaredLogger

func init() {
	sugar = logger.GetLogger()
}

func (r *rpcClient) MakeRPC(ctx context.Context, method string, req any, res any, retryCnt int, timeoutInSec int) error {

	ctx, cancel := context.WithTimeout(ctx, time.Duration(timeoutInSec))
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			sugar.Debugw("context done called on rpc", "rpcClient", r, "method", method)
			return nil
		default:
			var err error
			for i := 0; i < retryCnt; i++ {
				err = r.client.Call(ctx, method, req, res)
				if err != nil {
					sugar.Debugw("RPC errored out retrying", "error", err, "rpcClient", r, "method", method)
					continue
				}
			}
			return err
		}
	}
}

func GetRpcClient(protocol string, address string) (RpcClientI, error) {
	client, err := rpc.Dial(protocol, address)
	if err != nil {
		return nil, err
	}
	return &rpcClient{client: client}, nil
}
