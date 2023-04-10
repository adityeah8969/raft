package logEntry

import "gorm.io/gorm"

type Entry interface{}

type LogEntry struct {
	gorm.Model
	Term  int
	Index int
	Entry interface{} `gorm:"type:jsonb"`
}
