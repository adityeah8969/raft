package types

import "github.com/adityeah8969/raft/types/logEntry"

type RequestVoteRPC struct {
	CandidateId  string
	Term         int
	LastLogTerm  int
	LastLogIndex int
}

type RequestAppendEntryRPC struct {
	LeaderTerm                 int
	LeaderId                   string
	PrevEntry                  logEntry.LogEntry
	Entries                    []logEntry.LogEntry
	LastCommittedEntryInLeader logEntry.LogEntry
}

type RequestEntry struct {
	Key string `json:"key"`
	Val string `json:"val"`
}
