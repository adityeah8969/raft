package constants

type MachineType string

const (
	Sqlite MachineType = "sqlite"
)

type ServerDbType string

const (
	SqliteDb ServerDbType = "sqlite"
)

type ServerState string

const (
	Follower  ServerState = "follower"
	Leader    ServerState = "leader"
	Candidate ServerState = "candidate"
)
