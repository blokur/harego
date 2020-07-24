package internal

// this file contains the settings for environment variables used during tests.

import (
	"github.com/spf13/viper"
)

// Argument names.
const (
	RabbitMQAddrName    = "rabbitmq_addr"
	RabbitMQUserName    = "rabbitmq_user"
	RabbitMQPassName    = "rabbitmq_password"
	RabbitMQVirtualName = "rabbitmq_vh"
)

// Environment variables.
var (
	RabbitMQAddr    string
	RabbitMQUser    string
	RabbitMQPass    string
	RabbitMQVirtual string
)

func init() {
	viper.AutomaticEnv()
	viper.SetDefault(RabbitMQAddrName, "localhost:5672")
	viper.SetDefault(RabbitMQUserName, "guest")
	viper.SetDefault(RabbitMQPassName, "guest")
	viper.SetDefault(RabbitMQVirtualName, "harego")
}
