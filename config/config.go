package config

import (
	"fmt"

	"github.com/spf13/viper"
)

const (
	configFileName = "config"
	configFileType = "yaml"
	configFilePath = "."

	defaultSqliteSrcFileName = "/tmp/raft.db"
)

var configuration Config

func init() {

	viper.SetConfigName(configFileName)
	viper.AddConfigPath(configFilePath)
	// viper.AutomaticEnv()
	viper.SetConfigType(configFileType)

	if err := viper.ReadInConfig(); err != nil {
		fmt.Printf("Error reading config file, %s", err)
	}

	err := viper.Unmarshal(&configuration)
	if err != nil {
		fmt.Printf("Unable to decode into Config struct, %v", err)
	}
}

type Config struct {
	StateMachineType string
	Sql              SqlConfig
}

type SqlConfig struct {
	Sqlite SqliteConfig
}

type SqliteConfig struct {
	SrcFileName string
}

func GetStateMachineType() string {
	return configuration.StateMachineType
}

func GetSqliteSrcFileName() string {
	return configuration.Sql.Sqlite.SrcFileName
}
