package types

import (
	"github.com/adityeah8969/raft/types/command"
)

type ClientRequest struct {
	stateMachineType string
	command          command.Command
}
