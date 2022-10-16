package types

import "github.com/adityeah8969/raft/types/logEntry"

type ClientRequest struct {
	entry logEntry.Entry
}

type RequestVoteRPC struct {
	ServerId string
	Term     int
}

type RequestAppendEntryRPC struct {
	Index         int
	Term          int
	LastCommitted int
	Entries       []logEntry.Entry
}
