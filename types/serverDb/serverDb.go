package serverdb

import (
	"fmt"

	"github.com/adityeah8969/raft/config"
	"github.com/adityeah8969/raft/types"
	"github.com/adityeah8969/raft/types/constants"
	"github.com/adityeah8969/raft/types/logEntry"
)

func GetServerDbInstance() (DAO, error) {
	serverDbType := config.GetServerDbType()
	switch serverDbType {
	case string(constants.SqliteDb):
		var sqliteServerDb SqliteServerDb
		db, err := sqliteServerDb.GetServerDb()
		if err != nil {
			return nil, err
		}
		return &SqliteServerDb{db: db}, nil
	}
	return nil, fmt.Errorf("incompatible server db type %q", serverDbType)
}

func AutoMigrateModels(dbInst DAO) error {

	dbConn := dbInst.GetDB()
	if dbConn == nil {
		return fmt.Errorf("nil dbConn")
	}

	err := dbConn.AutoMigrate(&types.Vote{}, &logEntry.LogEntry{})
	if err != nil {
		return err
	}
	return nil
}
