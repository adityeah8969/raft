package types

import (
	"github.com/adityeah8969/raft/types/logEntry"
	"github.com/adityeah8969/raft/types/stateMachine"
)

var serverInstancve Server

func init() {

}

type Server struct {
	serverId        string
	leaderId        string
	peers           []string
	state           string
	currentTerm     int
	votedFor        string
	lastCommitIndex int
	lastApplied     int
	// next log entry to send to servers
	nextIndex []int
	// index of the highest log entry known to be replicated on server
	matchIndex []int
	// check if this can be made an interface
	logs         []logEntry.Entry
	stateMachine stateMachine.StateMachine
}

// Read config and then create a server
// stateMC will be decided by the config
