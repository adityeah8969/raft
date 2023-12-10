package types

type ResponseAppendEntryRPC struct {
	ServerId string
	// Check if the 'Success' field is needed
	Success                     bool
	OutdatedTerm                bool
	CurrentLeader               string
	PreviousEntryPresent        bool
	Term                        int
	LastAppendedIndexInFollower int
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

type ResonseProcessRPC struct {
	HasWon         bool
	IsOutdatedTerm bool
	NextIndex      []int
	MatchIndex     []int
	RPCsFired      int
}

type ResponseTrimLogs struct {
	PreviousEntryPresent bool
}
