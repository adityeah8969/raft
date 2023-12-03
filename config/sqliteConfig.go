package config

import (
	"encoding/json"
)

type SqliteConfig struct {
	SrcFileName string `mapstructure:"SrcFileName"`
}

// TODO: Can we improve on this 'LoadConfig'
func (s *SqliteConfig) LoadConfig(bytes []byte) (StateMachineConfig, error) {
	if err := json.Unmarshal(bytes, s); err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *SqliteConfig) GetSrcFile() string {
	return s.SrcFileName
}
