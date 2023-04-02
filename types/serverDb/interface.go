package serverdb

import (
	"github.com/adityeah8969/raft/types"
	"github.com/adityeah8969/raft/types/logEntry"
	"gorm.io/gorm"
)

type DAO interface {
	GetDB() *gorm.DB
	SaveVote(*types.Vote) error
	SaveLogs([]logEntry.LogEntry) error
}
