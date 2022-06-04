package stateMachine

import "github.com/adityeah8969/raft/types/command"

type StateMachine interface {
	GetType() string
	Apply(command command.Command) error
	Validate(command command.Command) bool
}
