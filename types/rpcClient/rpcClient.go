package rpcClient

import (
	"context"
	"time"

	"github.com/adityeah8969/raft/types/logger"
	"github.com/adityeah8969/raft/util"
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

func (r *rpcClient) MakeRPC(ctx context.Context, method string, req interface{}, res interface{}, retryCnt int, retryInterval *time.Duration) error {
	var err error
	for i := 0; i < retryCnt; i++ {
		err = r.client.Call(ctx, method, req, res)
		if err != nil && util.ShouldRetry(err) {
			sugar.Debugw("rpc retrying", "error", err, "rpcClient", r, "method", method)
			if retryInterval != nil {
				time.Sleep(*retryInterval)
			}
			continue
		}
		break
	}
	return err
}

func GetRpcClient(protocol string, address string, retryLimit int) (RpcClientI, error) {

	var client *rpc.Client
	var err error

	for cnt := 1; cnt <= retryLimit; cnt++ {
		client, err = rpc.Dial(protocol, address)
		if err != nil {
			// take this as an input param to the method. i don't think we should make this duration configurable  
			time.Sleep(2 * time.Second)
			sugar.Infof("sleeping 2 more seconds, waiting for peers to come up\n")
			continue
		}
	}
	return &rpcClient{client: client}, err
}
