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
var sqlStateMachineInstance *SqlStateMachine
var sqliteDB *gorm.DB

type SqlStateMachine struct {
	stateMachineType MachineType
	db               *gorm.DB
}

func init() {
	var err error
	sqliteDB, err = gorm.Open(sqlite.Open(config.GetSqliteSrcFileName()), &gorm.Config{})
	if err != nil {
		log.Fatal("initializing db session: ", err)
	}
	sqliteDB.AutoMigrate(&logEntry.SqlStateMcLog{})
}

func GetSqlStateMachineInstance() (*SqlStateMachine, error) {
	if sqlStateMachineInstance == nil {
		lock.Lock()
		defer lock.Unlock()
		if sqlStateMachineInstance == nil {
			sqlStateMachineInstance = &SqlStateMachine{
				stateMachineType: MachineType(config.GetStateMachineType()),
				db:               sqliteDB,
			}
			return sqlStateMachineInstance, nil
		} else {
			return sqlStateMachineInstance, nil
		}
	}
	return sqlStateMachineInstance, nil
}

// check if this is needed
func (sm *SqlStateMachine) GetMachineType() MachineType {
	return sm.stateMachineType
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
