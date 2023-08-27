package config

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/viper"
)

const (
	configFilePath = "./config"
	configFileName = "config"
	configFileType = "yaml"
)

// check if the members can be made private
type Config struct {
	StateMachineType                      string      `mapstructure:"StateMachineType"`
	StateMachineConfig                    interface{} `mapstructure:"StateMachineConfig"`
	ServerDbType                          string      `mapstructure:"ServerDbType"`
	ServerDBConfig                        interface{} `mapstructure:"ServerDbConfig"`
	ServerId                              string      `mapstructure:"ServerId"`
	Peers                                 interface{} `mapstructure:"Peers"`
	HeartBeatTickerIntervalInMilliseconds int         `mapstructure:"HeartBeatTickerIntervalInMilliseconds"`
	MinTickerIntervalInMiliseconds        int         `mapstructure:"MinFollowerTickerIntervalInMiliseconds"`
	MaxTickerIntervalInMiliseconds        int         `mapstructure:"MaxFollowerTickerIntervalInMiliseconds"`
	RpcRetryLimit                         int         `mapstructure:"RpcRetryLimit"`
	RpcTimeoutInSeconds                   int         `mapstructure:"RpcTimeoutInSeconds"`
	ClientRequestTimeoutInSeconds         int         `mapstructure:"ClientRequestTimeoutInSeconds"`
	MaxElectionTimeOutInSeconds           int         `mapstructure:"MaxElectionTimeOutInSeconds"`
	Port                                  int         `mapstructure:"Port"`
}

var config Config

func init() {
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

func GetServerDbConfig() ([]byte, error) {
	bytes, err := json.Marshal(config.ServerDBConfig)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

func GetStateMachineType() string {
	return config.StateMachineType
}

func GetServerId() string {
	return config.ServerId
}

func GetPeers() ([]byte, error) {
	bytes, err := json.Marshal(config.Peers)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

func GetServerDbType() string {
	return config.ServerDbType
}

func GetHeartBeatTickerIntervalInMilliseconds() int {
	return config.HeartBeatTickerIntervalInMilliseconds
}

func GetMinTickerIntervalInMillisecond() int {
	return config.MinTickerIntervalInMiliseconds
}

func GetMaxTickerIntervalInMillisecond() int {
	return config.MaxTickerIntervalInMiliseconds
}

func GetRpcRetryLimit() int {
	return config.RpcRetryLimit
}

func GetRpcTimeoutInSeconds() int {
	return config.RpcTimeoutInSeconds
}

func GetClientRequestTimeoutInSeconds() int {
	return config.ClientRequestTimeoutInSeconds
}

func GetMaxElectionTimeOutInSec() int {
	return config.MaxElectionTimeOutInSeconds
}

func GetAppPort() int {
	return config.Port
}

type StateMachineConfig interface {
	LoadConfig(bytes []byte) (StateMachineConfig, error)
}

type ServerDBConfig interface {
	LoadConfig(bytes []byte) (ServerDBConfig, error)
}
