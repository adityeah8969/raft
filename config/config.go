package config

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/viper"
)

const (
	configFilePath = "/home/aditya/workspace/repos/raft/config"
	configFileName = "config"
	configFileType = "yaml"
)

// check if the members can be made private
type Config struct {
	StateMachineType              string      `mapstructure:"StateMachineType"`
	StateMachineConfig            interface{} `mapstructure:"StateMachineConfig"`
	ServerDbType                  string      `mapstructure:"ServerDbType"`
	ServerDBConfig                interface{} `mapstructure:"ServerDbConfig"`
	ServerId                      string      `mapstructure:"ServerId"`
	Peers                         interface{} `mapstructure:"Peers"`
	TickerIntervalInMiliseconds   int         `mapstructure:"TickerIntervalInMiliseconds"`
	RpcRetryLimit                 int         `mapstructure:"RpcRetryLimit"`
	RPCTimeoutInSeconds           int         `mapstructure:"RPCTimeoutInSeconds"`
	ClientRequestTimeoutInSeconds int         `mapstructure:"ClientRequestTimeoutInSeconds"`
	MinElectionTimeOutInSeconds   int         `mapstructure:"ElectionTimerDurationInSeconds"`
	MaxElectionTimeOutInSeconds   int         `mapstructure:"MaxElectionTimeOutInSeconds"`
	Port                          int         `mapstructure:"MaxElectionTimeOutInSeconds"`
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

func GetTickerIntervalInMillisecond() int {
	return config.TickerIntervalInMiliseconds
}

func GetRpcRetryLimit() int {
	return config.RpcRetryLimit
}

func GetRPCTimeoutInSeconds() int {
	return config.RPCTimeoutInSeconds
}

func GetClientRequestTimeoutInSeconds() int {
	return config.ClientRequestTimeoutInSeconds
}

func GetMinElectionTimeOutInSec() int {
	return config.MinElectionTimeOutInSeconds
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
