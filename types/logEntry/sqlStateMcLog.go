package logEntry

import "gorm.io/gorm"

type SqliteStateMcLog struct {
	gorm.Model
	Term    int
	Index   int
	sqlData SqlData `gorm:"embedded"`
}

// key can be made a non-unique index here
type SqlData struct {
	Key string `json:"key"`
	Val string `json:"val"`
}
