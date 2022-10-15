package serverdb

import (
	"github.com/adityeah8969/raft/config"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type SqliteServerDb struct{}

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
