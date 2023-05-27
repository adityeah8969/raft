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

func GetRpcClient(protocol string, address string, retryLimit int) (RpcClientI, error) {

	var client *rpc.Client
	var err error

	for cnt := 1; cnt <= retryLimit; cnt++ {
		client, err = rpc.Dial(protocol, address)
		if err != nil {

			time.Sleep(10 * time.Second)
			sugar.Infof("sleeping 10 more seconds, waiting for peers to come up\n")
			continue
			// _, ok := err.(*net.DNSError)
			// sugar.Infof("err dialing 1: %#v \n", opErr.Err)
			// sugar.Infof("err dialing 2: %v", opErr.Err.Error())
			// if ok {
			// 	sugar.Warnf("Connection refused, waiting for peer to come up")
			// 	time.Sleep(100 * time.Millisecond)
			// 	continue
			// } else {
			// 	return nil, err
			// }

		}
		// sugar.Infof("Retrying initialization of rpc client, current error: %v", err)
		// panic(fmt.Sprintf("unable to create rpc client: %v", err))
	}
	return &rpcClient{client: client}, err
}
