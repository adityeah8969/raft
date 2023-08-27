package util

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/adityeah8969/raft/types/logEntry"
)

func GetRandomInt(max int, min int) int {
	return rand.Intn(max-min) + min
}

func GetRandomTickerDuration(minRange, maxRange int) time.Duration {
	rand.Seed(time.Now().UnixNano())
	return time.Duration((rand.Intn(maxRange-minRange) + minRange)) * time.Millisecond
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

func MergeMaps(maps ...map[string]interface{}) map[string]interface{} {
	res := make(map[string]interface{})
	for _, mp := range maps {
		for k, v := range mp {
			res[k] = v
		}
	}
	return res
}

func ShouldRetry(err error) bool {
	switch err.Error() {
	case "context canceled":
		return false
	default:
		return true
	}
}
