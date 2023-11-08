// Package internal provides some internal functionalities for the library.
package internal

// this file contains the settings for environment variables used during tests.

import (
	"sync"

	"github.com/kelseyhightower/envconfig"
)

var (
	once    sync.Once
	environ Env
)

// Env contains the environment variables required to run the application.
// nolint:govet // unlikely to have a lot of these objects.
type Env struct {
	RabbitMQAddr    string `envconfig:"RABBITMQ_ADDR" default:"localhost:5672"`
	RabbitMQUser    string `envconfig:"RABBITMQ_USER" default:"guest"`
	RabbitMQPass    string `envconfig:"RABBITMQ_PASSWORD" default:"guest"`
	RabbitMQVirtual string `envconfig:"RABBITMQ_VH" default:"harego"`
}

// GetEnv returns the environment variables required to run the application.
func GetEnv() (*Env, error) {
	var err error
	once.Do(func() {
		err = envconfig.Process("", &environ)
	})
	return &environ, err
}
