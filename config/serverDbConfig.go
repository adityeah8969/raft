package config

import (
	"encoding/json"
)

type ServerSqliteDbConfig struct {
	SrcFileName string `mapstructure:"srcfilename"`
}

func (s *ServerSqliteDbConfig) LoadConfig(bytes []byte) (ServerDBConfig, error) {
	var serverSqliteDbConfig ServerDBConfig
	if err := json.Unmarshal(bytes, &serverSqliteDbConfig); err != nil {
		return nil, err
	}
	return serverSqliteDbConfig, nil
}

func (s *ServerSqliteDbConfig) GetSrcFile() string {
	return s.SrcFileName
}
