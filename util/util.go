package util

import (
	"math/rand"
	"net/rpc"
	"time"

	"github.com/adityeah8969/raft/types/logEntry"
)

func GetRandomInt(max int, min int) int {
	return rand.Intn(max-min) + min
}

func RPCWithRetry(client *rpc.Client, svcMethod string, request any, response any, retryCount int) error {
	var err error
	for i := 0; i < retryCount; i++ {
		err := client.Call(svcMethod, request, response)
		if err == nil {
			return nil
		}
	}
	return err
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
