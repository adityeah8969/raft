package types

type ResponseAppendEntryRPC struct {
	ServerId      string
	Success       bool
	OutdatedTerm  bool
	CurrentLeader string
	Data          interface{}
}

type ResponseVoteRPC struct {
	Voted         bool
	OutdatedTerm  bool
	CurrentLeader string
}
