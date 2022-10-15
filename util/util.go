package util

import (
	"math/rand"
	"net/rpc"
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
