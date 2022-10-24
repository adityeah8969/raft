package logEntry

import (
	"errors"

	"gorm.io/gorm"
)

type SqliteStateMcLog struct {
	gorm.Model
	Term    int
	Index   int
	SqlData sqlData `gorm:"embedded"`
}

// key can be made a non-unique index here
type sqlData struct {
	Key string `json:"key"`
	Val string `json:"val"`
}

func (s *SqliteStateMcLog) GetStateMcLogFromLogEntry(entry *LogEntry) error {

	sqlData, ok := entry.Entry.(sqlData)
	if !ok {
		return errors.New("unable to cast into SqliteStateMcLog")
	}

	s = &SqliteStateMcLog{
		Term:    entry.Term,
		Index:   entry.Index,
		SqlData: sqlData,
	}
	return nil
}
