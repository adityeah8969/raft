package stateMachine

import (
	"github.com/adityeah8969/raft/types/logEntry"
)

type StateMachine interface {
	Apply(entries []logEntry.LogEntry) error
	GetEntry(entry *logEntry.LogEntry) (logEntry.Entry, error)
	GetStateMachineInstance() StateMachine
}
