package stateMachine

import (
	"fmt"

	"github.com/adityeah8969/raft/config"
	"github.com/adityeah8969/raft/types/constants"
)

func GetStateMachine() (StateMachine, error) {
	stateMachinetype := config.GetStateMachineType()
	fmt.Printf("stateMachinetype: %v\n", stateMachinetype)
	switch stateMachinetype {
	case string(constants.Sqlite):
		// TODO: Why are we initializing the var first and then calling associated method ?
		var sqlStateMachine *SqliteStateMachine
		return sqlStateMachine.GetStateMachineInstance(), nil
	}
	return nil, fmt.Errorf("incompatible state machine type %q", stateMachinetype)
}
