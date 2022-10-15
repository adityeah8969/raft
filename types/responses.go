package types

type ServerResponse struct {
	serverId string
	success  bool
	data     interface{}
}

type ResponseVoteRPC struct {
	Voted         bool
	OutdatedTerm  bool
	CurrentLeader string
}
