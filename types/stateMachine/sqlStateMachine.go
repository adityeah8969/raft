package stateMachine

import (
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

func (sm *SqlStateMachine) GetMachineType() MachineType {
	return sm.stateMachineType
}

// write the apply query
// keep in ming that term + index comes in the parameter entry, from the server.
func (sm *SqlStateMachine) Apply(entry logEntry.Entry) error {
	// stateMcLog := entry.(*logEntry.SqlStateMcLog)

	// res := sm.db.Model(&sm.stateMachineType).Where("key = ?", stateMcLog.Key).Updates(stateMcLog)

	// return sm.db.Model(&sm.stateMachineType).Where("key = ?", sqlData.Key).Update("val", sqlData.Val).Error

	return nil
}

func (sm *SqlStateMachine) GetEntry(entry logEntry.Entry) (logEntry.Entry, error) {
	entryRequest := entry.(*logEntry.SqlData)
	key := entryRequest.Key
	var entryResponse logEntry.SqlData
	err := sm.db.Model(&sm.stateMachineType).Where("key = ?", key).First(&entryResponse).Error
	if err != nil {
		return nil, err
	}
	return &entryResponse, nil
}
