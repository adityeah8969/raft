package config

import (
	"encoding/json"
)

type ServerSqliteDbConfig struct {
	SrcFileName string `mapstructure:"SrcFileName"`
}

// TODO: Can we improve on this 'LoadConfig'
func (s *ServerSqliteDbConfig) LoadConfig(bytes []byte) (ServerDBConfig, error) {
	if err := json.Unmarshal(bytes, s); err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *ServerSqliteDbConfig) GetSrcFile() string {
	return s.SrcFileName
}
