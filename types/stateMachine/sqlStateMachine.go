package stateMachine

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/adityeah8969/raft/config"

	"github.com/adityeah8969/raft/types/logEntry"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// TODO: Can we make these a part of 'SqliteStateMachine' struct
var lock = &sync.Mutex{}
var sqliteDB *gorm.DB

type SqliteStateMachine struct {
	db *gorm.DB
}

type StateMcLog struct {
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

// TODO: This methid should not be a part of 'SqliteStateMachine' struct. rather a public package method
// TODO: Also is there any need of going the singleton way ? we are calling this method exactly once :), Let's not force JAVA down this guys throat
func (sm *SqliteStateMachine) GetStateMachineInstance() StateMachine {
	if sm == nil {
		lock.Lock()
		defer lock.Unlock()
		if sm == nil {
			sqliteDB, err := getSqliteDbDetails()
			if err != nil {
				panic(fmt.Errorf("error opening gorm db connection: %v", err))
			}
			err = sqliteDB.AutoMigrate(&StateMcLog{})
			if err != nil {
				panic(fmt.Errorf("error migrating models for sqlite state machine: %v", err))
			}
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

func getSqliteDbDetails() (*gorm.DB, error) {
	stateMachineConfig, err := config.GetStateMachineConfig()
	if err != nil {
		return nil, err
	}
	var sqliteConfig config.SqliteConfig
	sqliteConfig.LoadConfig(stateMachineConfig)
	srcFile := sqliteConfig.GetSrcFile()
	fmt.Printf("srcFile: %v\n", srcFile)
	sqliteDB, err = gorm.Open(sqlite.Open(srcFile), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	return sqliteDB, nil
}

func (sm *SqliteStateMachine) Apply(entries []logEntry.LogEntry) error {
	stateMcLogs := make([]*StateMcLog, 0)
	for _, entry := range entries {
		stateMcLog, err := getStateMcLogFromLogEntry(&entry)
		if err != nil {
			return err
		}
		stateMcLogs = append(stateMcLogs, stateMcLog)
	}
	return sm.db.Model(&StateMcLog{}).Create(stateMcLogs).Error
}

// Fetching the last (latest) entry from the state machine, for a given key.
func (sm *SqliteStateMachine) GetEntry(entry *logEntry.LogEntry) (logEntry.Entry, error) {
	stateMcLog, err := getStateMcLogFromLogEntry(entry)
	if err != nil {
		return nil, err
	}
	var entryResponse StateMcLog
	err = sm.db.Model(&StateMcLog{}).Where("key = ?", stateMcLog.SqlData.Key).Last(&entryResponse).Error
	if err != nil {
		return nil, err
	}
	return &entryResponse, nil
}

// While confirming if the entry has been applied, we need to check if the appended entry matches with that of applied entry.
// This equality check expects the 'LogEntry' operands to be pointing to some values and not pointers. That is the reason that we are returing '*sqliteLogEntry'.
func (sm *SqliteStateMachine) GetStateMcLogFromLogEntry(entry *logEntry.LogEntry) (logEntry.Entry, error) {
	sqliteLogEntry, err := getStateMcLogFromLogEntry(entry)
	if err != nil {
		return "", nil
	}
	return *sqliteLogEntry, nil
}

func getStateMcLogFromLogEntry(entry *logEntry.LogEntry) (*StateMcLog, error) {

	// starts here
	jsonData, err := json.Marshal(entry.Entry)
	if err != nil {
		return nil, err
	}

	// Create a struct instance
	var data sqlData

	// Unmarshal the JSON into the struct
	err = json.Unmarshal(jsonData, &data)
	if err != nil {
		return nil, err
	}
	// till here

	// sqlData, ok := entry.Entry.(sqlData)
	// if !ok {
	// 	return nil, errors.New("unable to cast into StateMcLog")
	// }
	return &StateMcLog{
		Term:    entry.Term,
		Index:   entry.Index,
		SqlData: data,
	}, nil
}
