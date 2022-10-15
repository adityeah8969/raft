package config

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/viper"
)

const (
	configFilePath = "."
	configFileName = "config"
	configFileType = "yaml"
)

// check if the memebers can be made private
type Config struct {
	StateMachineType   string      `mapstructure:"StateMachineType"`
	StateMachineConfig interface{} `mapstructure:"StateMachineConfig"`
}

var config Config

func init() {
	fmt.Println("Reading config")
	viper.AddConfigPath(configFilePath)
	viper.SetConfigName(configFileName)
	viper.SetConfigType(configFileType)

	if err := viper.ReadInConfig(); err != nil {
		fmt.Printf("Error reading config file, %s", err)
	}

	err := viper.Unmarshal(&config)
	if err != nil {
		fmt.Printf("Unable to decode into Config struct, %v", err)
	}
}

func GetStateMachineConfig() ([]byte, error) {
	bytes, err := json.Marshal(config.StateMachineConfig)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

// len == 0 check
func GetStateMachineType() string {
	return config.StateMachineType
}

type StateMachineConfig interface {
	LoadConfig(bytes []byte) (StateMachineConfig, error)
}
