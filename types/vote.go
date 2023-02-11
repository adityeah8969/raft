package types

import "gorm.io/gorm"

type Vote struct {
	gorm.Model
	Term     int
	VotedFor string
}
