package types

import "github.com/adityeah8969/raft/types/logEntry"

type RequestVoteRPC struct {
	CandidateId  string
	Term         int
	LastLogTerm  int
	LastLogIndex int
}

type RequestAppendEntryRPC struct {
	Term                  int
	LeaderId              string
	PrevEntryIndex        int
	PrevEntryTerm         int
	Entries               []logEntry.LogEntry
	LeaderLastCommitIndex int
}

type RequestEntry interface{}
