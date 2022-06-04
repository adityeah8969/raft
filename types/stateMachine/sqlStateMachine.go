package stateMachine

import "gorm.io/gorm"

type SqlStateMachine struct {
	stateMachineType string
	db               *gorm.DB
}
