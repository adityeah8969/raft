package types

type RequestVoteRPC struct {
	ServerId string
	Term     int
}

type RequestAppendEntryRPC struct {
	LastCommitted      int
	CurrentEntry       LogEntry
	PrevEntry          LogEntry
	LastCommittedEntry LogEntry
}

type LogEntry struct {
	Term  int
	Index int
	Entry interface{}
}
