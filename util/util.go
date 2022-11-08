package util

import (
	"errors"
	"math/rand"
	"net/rpc"
	"time"

	"github.com/adityeah8969/raft/types/logEntry"
	"github.com/adityeah8969/raft/types/logger"
	"go.uber.org/zap"
)

var sugar *zap.SugaredLogger

func init() {
	sugar = logger.GetLogger()
}

func GetRandomInt(max int, min int) int {
	return rand.Intn(max-min) + min
}

func RPCWithRetry(client *rpc.Client, svcMethod string, request any, response any, retryCount int, timeoutInSeconds int) error {

	if timeoutInSeconds < 0 {
		// TODO : get this from config
		timeoutInSeconds = 1
	}

	var err error
	timer := time.NewTimer(time.Duration(timeoutInSeconds) * time.Second)
	for {
		select {
		case <-timer.C:
			sugar.Debugw("RPC timed out", "svcMethod", svcMethod, "rpcClient", client)
			return errors.New("RPC timed out")
		default:
			for i := 0; i < retryCount; i++ {
				err := client.Call(svcMethod, request, response)
				if err != nil {
					sugar.Debugw("RPC errored out, retrying", "error", err, "svcMethod", svcMethod, "rpcClient", client)
					continue
				}
				return nil
			}
			return err
		}
	}
}

func GetRandomTickerDuration(interval int) time.Duration {
	return time.Duration(GetRandomInt(interval, 2*interval) * int(time.Millisecond))
}

func GetReversedSlice(slice []logEntry.LogEntry) []logEntry.LogEntry {
	revSlice := make([]logEntry.LogEntry, len(slice))
	for i := len(slice) - 1; i >= 0; i-- {
		revSlice = append(revSlice, slice[i])
	}
	return revSlice
}
