package types

type ResponseAppendEntryRPC struct {
	ServerId                     string
	Success                      bool
	OutdatedTerm                 bool
	CurrentLeader                string
	PreviousEntryAbsent          bool
	Term                         int
	LastCommittedIndexInFollower int
}

type ResponseVoteRPC struct {
	Term          int
	VoteGranted   bool
	OutdatedTerm  bool
	CurrentLeader string
	Err           error
}

type ResponseEntry struct {
	Success bool
	Err     error
	Data    interface{}
}
