package types

import "gorm.io/gorm"

type SqlStateMachine struct {
	stateMachineType string
	db               *gorm.DB
}

func (sm *SqlStateMachine) GetType() string {
	return sm.stateMachineType
}
