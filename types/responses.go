package types

type ResponseAppendEntryRPC struct {
	ServerId string
	// Check if the 'Success' field is needed
	Success                      bool
	OutdatedTerm                 bool
	CurrentLeader                string
	PreviousEntryAbsent          bool
	Term                         int
	LastCommittedIndexInFollower int
}

type ResponseVoteRPC struct {
	Term          int
	Voter         string
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
