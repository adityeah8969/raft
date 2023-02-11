package util

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/adityeah8969/raft/types/logEntry"
)

// var sugar *zap.SugaredLogger

// func init() {
// 	sugar = logger.GetLogger()
// }

func GetRandomInt(max int, min int) int {
	return rand.Intn(max-min) + min
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

func GetServerId(id int) string {
	return fmt.Sprintf("server-%v", id)
}

func GetServerIndex(serverId string) int {
	// TODO: "server-" has to be taken from a variable
	index, _ := strconv.Atoi(strings.TrimPrefix(serverId, "server-"))
	return index
}
