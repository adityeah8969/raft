package types

import "github.com/adityeah8969/raft/types/logEntry"

type RequestVoteRPC struct {
	ServerId string
	Term     int
}

type RequestAppendEntryRPC struct {
	LastCommitted      int
	CurrentEntry       RequestEntry
	PrevEntry          RequestEntry
	LastCommittedEntry RequestEntry
}

type RequestEntry struct {
	Term  int
	Index int
	Entry logEntry.Entry
}
