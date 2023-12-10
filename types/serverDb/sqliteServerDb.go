package serverdb

import (
	"database/sql/driver"
	"encoding/json"
	"errors"

	"github.com/adityeah8969/raft/config"
	"github.com/adityeah8969/raft/types"
	"github.com/adityeah8969/raft/types/logEntry"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type SqliteServerDb struct {
	db *gorm.DB
}

type CommitLog struct {
	gorm.Model
	Term  int
	Index int
	Entry CommitEntry `gorm:"type:jsonb"`
}

type CommitEntry map[string]interface{}

func (c CommitEntry) Value() (driver.Value, error) {
	return json.Marshal(c)
}

func (c *CommitEntry) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("Scan source is not []byte")
	}

	return json.Unmarshal(b, &c)
}

func (s *SqliteServerDb) GetServerDb() (*gorm.DB, error) {
	configBytes, err := config.GetServerDbConfig()
	if err != nil {
		return nil, err
	}
	var serverDbConfig config.ServerSqliteDbConfig
	serverDbConfig.LoadConfig(configBytes)
	srcFile := serverDbConfig.GetSrcFile()
	sqliteDB, err := gorm.Open(sqlite.Open(srcFile), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	return sqliteDB, nil
}

func (s *SqliteServerDb) GetDB() *gorm.DB {
	return s.db
}

func (s *SqliteServerDb) SaveVote(vote *types.Vote) error {
	return s.db.Model(&types.Vote{}).Save(vote).Error
}

func (s *SqliteServerDb) SaveLogs(entries []logEntry.LogEntry) error {
	entities := make([]*CommitLog, len(entries))
	for ind, entry := range entries {
		entities[ind] = &CommitLog{
			Term:  entry.Term,
			Index: entry.Term,
			Entry: entry.Entry.(map[string]interface{}),
		}
	}
	return s.db.Model(&CommitLog{}).Save(entities).Error
}
