package serverdb

import "gorm.io/gorm"

type ServerDb interface {
	GetServerDb() (*gorm.DB, error)
}
