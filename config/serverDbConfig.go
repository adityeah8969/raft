package config

import (
	"encoding/json"
)

type ServerSqliteDbConfig struct {
	SrcFileName string `mapstructure:"srcfilename"`
}

func (s *ServerSqliteDbConfig) LoadConfig(bytes []byte) (ServerDBConfig, error) {
	// var serverSqliteDbConfig ServerDBConfig
	// if err := json.Unmarshal(bytes, &serverSqliteDbConfig); err != nil {
	// 	return nil, err
	// }

	if err := json.Unmarshal(bytes, s); err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *ServerSqliteDbConfig) GetSrcFile() string {
	return s.SrcFileName
}
