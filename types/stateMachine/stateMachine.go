package stateMachine

import (
	"errors"

	"github.com/adityeah8969/raft/config"
	"github.com/adityeah8969/raft/types/constants"
)

func GetStateMachine() (StateMachine, error) {
	stateMachinetype := config.GetStateMachineType()
	switch stateMachinetype {
	case string(constants.Sqlite):
		var sqlStateMachine SqlStateMachine
		return sqlStateMachine.GetStateMachineInstance(), nil
	}
	return nil, errors.New("incompatible state machine type")
}
