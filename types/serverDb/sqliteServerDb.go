package serverdb

import (
	"github.com/adityeah8969/raft/config"
	"github.com/adityeah8969/raft/types"
	"github.com/adityeah8969/raft/types/logEntry"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type SqliteServerDb struct {
	db *gorm.DB
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
	return s.db.Model(&logEntry.LogEntry{}).Save(entries).Error
}
