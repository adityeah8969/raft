package stateMachine

import (
	"github.com/adityeah8969/raft/types/logEntry"
)

type StateMachine interface {
	Apply(entry logEntry.Entry, currTerm int, index int) error
	GetEntry(entry logEntry.Entry) (logEntry.Entry, error)
	GetStateMachineInstance() StateMachine
}
