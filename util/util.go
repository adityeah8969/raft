package util

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/adityeah8969/raft/types/logEntry"
)

func GetRandomInt(min int, max int) int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(max-min) + min
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
