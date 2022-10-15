package stateMachine

import (
	"errors"
	"log"
	"sync"

	"github.com/adityeah8969/raft/config"

	"github.com/adityeah8969/raft/types/logEntry"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

var lock = &sync.Mutex{}
var sqliteDB *gorm.DB

type SqlStateMachine struct {
	db *gorm.DB
}

func (sm *SqlStateMachine) GetStateMachineInstance() StateMachine {
	if sm == nil {
		lock.Lock()
		defer lock.Unlock()
		if sm == nil {
			sqliteDB, err := getSqliteDbDetails()
			if err != nil {
				log.Fatal("initializing sqlite db: ", err)
			}
			sqliteDB.AutoMigrate(&logEntry.SqlStateMcLog{})
			sm = &SqlStateMachine{
				db: sqliteDB,
			}
			return sm
		} else {
			return sm
		}
	}
	return sm
}

func (sm *SqlStateMachine) Apply(entry logEntry.Entry, currTerm int, index int) error {
	sqlData, ok := entry.(*logEntry.SqlData)
	if !ok {
		return errors.New("cannot marshal entry to sql data")
	}
	stateMcLog := &logEntry.SqlStateMcLog{
		Term:    currTerm,
		Index:   index,
		SqlData: sqlData,
	}
	return sm.db.Model(&logEntry.SqlStateMcLog{}).Create(stateMcLog).Error
}

// Fetching the last (latest) entry from the state machine, for a given key.
func (sm *SqlStateMachine) GetEntry(entry logEntry.Entry) (logEntry.Entry, error) {
	entryRequest := entry.(*logEntry.SqlData)
	key := entryRequest.Key
	var entryResponse logEntry.SqlStateMcLog
	err := sm.db.Model(&logEntry.SqlStateMcLog{}).Where("key = ?", key).Last(&entryResponse).Error
	if err != nil {
		return nil, err
	}
	return &entryResponse, nil
}

func getSqliteDbDetails() (*gorm.DB, error) {
	stateMachineConfig, err := config.GetStateMachineConfig()
	if err != nil {
		return nil, err
	}
	var sqliteConfig config.SqliteConfig
	sqliteConfig.LoadConfig(stateMachineConfig)
	srcFile := sqliteConfig.GetSrcFile()
	sqliteDB, err = gorm.Open(sqlite.Open(srcFile), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	return sqliteDB, nil
}
