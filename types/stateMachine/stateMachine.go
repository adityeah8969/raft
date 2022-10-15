package stateMachine

import (
	"fmt"

	"github.com/adityeah8969/raft/config"
	"github.com/adityeah8969/raft/types/constants"
)

func GetStateMachine() (StateMachine, error) {
	stateMachinetype := config.GetStateMachineType()
	switch stateMachinetype {
	case string(constants.Sqlite):
		var sqlStateMachine SqliteStateMachine
		return sqlStateMachine.GetStateMachineInstance(), nil
	}
	return nil, fmt.Errorf("incompatible state machine type %q", stateMachinetype)
}
