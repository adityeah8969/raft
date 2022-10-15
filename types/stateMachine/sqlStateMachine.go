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

type SqliteStateMachine struct {
	db *gorm.DB
}

func (sm *SqliteStateMachine) GetStateMachineInstance() StateMachine {
	if sm == nil {
		lock.Lock()
		defer lock.Unlock()
		if sm == nil {
			sqliteDB, err := getSqliteDbDetails()
			if err != nil {
				log.Fatal("initializing sqlite db: ", err)
			}
			sqliteDB.AutoMigrate(&logEntry.SqliteStateMcLog{})
			sm = &SqliteStateMachine{
				db: sqliteDB,
			}
			return sm
		} else {
			return sm
		}
	}
	return sm
}

func (sm *SqliteStateMachine) Apply(entry logEntry.Entry, currTerm int, index int) error {
	sqlData, ok := entry.(*logEntry.SqlData)
	if !ok {
		return errors.New("cannot marshal entry to sql data")
	}
	stateMcLog := &logEntry.SqliteStateMcLog{
		Term:    currTerm,
		Index:   index,
		SqlData: sqlData,
	}
	return sm.db.Model(&logEntry.SqliteStateMcLog{}).Create(stateMcLog).Error
}

// Fetching the last (latest) entry from the state machine, for a given key.
func (sm *SqliteStateMachine) GetEntry(entry logEntry.Entry) (logEntry.Entry, error) {
	entryRequest := entry.(*logEntry.SqlData)
	key := entryRequest.Key
	var entryResponse logEntry.SqliteStateMcLog
	err := sm.db.Model(&logEntry.SqliteStateMcLog{}).Where("key = ?", key).Last(&entryResponse).Error
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
