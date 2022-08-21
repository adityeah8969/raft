package logEntry

import "gorm.io/gorm"

type SqlStateMcLog struct {
	gorm.Model
	Term    int
	Index   int
	SqlData Entry `gorm:"embedded"`
}

// key can be made a non-unique unique index here
type SqlData struct {
	Key string `json:"key"`
	Val string `json:"val"`
}
