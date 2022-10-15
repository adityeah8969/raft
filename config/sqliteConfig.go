package config

import (
	"encoding/json"
)

type SqliteConfig struct {
	SrcFileName string `mapstructure:"srcfilename"`
}

func (s *SqliteConfig) LoadConfig(bytes []byte) (StateMachineConfig, error) {
	var sqliteConfig SqliteConfig
	if err := json.Unmarshal(bytes, &sqliteConfig); err != nil {
		return nil, err
	}
	return &sqliteConfig, nil
}

func (s *SqliteConfig) GetSrcFile() string {
	return s.SrcFileName
}
