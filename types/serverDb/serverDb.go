package serverdb

import (
	"errors"
	"fmt"

	"github.com/adityeah8969/raft/config"
	"github.com/adityeah8969/raft/types/constants"
	"gorm.io/gorm"
)

func GetServerDbInstance() (*gorm.DB, error) {
	serverDbType := config.GetServerDbType()
	switch serverDbType {
	case string(constants.SqliteDb):
		var sqliteServerDb SqliteServerDb
		db, err := sqliteServerDb.GetServerDb()
		if err != nil {
			return nil, err
		}
		return db, nil
	}
	return nil, fmt.Errorf("incompatible server db type %q", serverDbType))
}
