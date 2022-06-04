package logEntry

import "github.com/adityeah8969/raft/types/command"

type LogEntry interface {
	GetTerm() int
	GetCommand() command.Command
	GetIndex() int
}
