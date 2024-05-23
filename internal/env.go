// Package internal provides some internal functionalities for the library.
package internal

// this file contains the settings for environment variables used during tests.

import (
	"fmt"
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
	RabbitMQAddr    string `default:"localhost:5672" envconfig:"RABBITMQ_ADDR"`
	RabbitMQUser    string `default:"guest"          envconfig:"RABBITMQ_USER"`
	RabbitMQPass    string `default:"guest"          envconfig:"RABBITMQ_PASSWORD"`
	RabbitMQVirtual string `default:"harego"         envconfig:"RABBITMQ_VH"`
}

// GetEnv returns the environment variables required to run the application.
func GetEnv() (*Env, error) {
	var err error
	once.Do(func() {
		err = envconfig.Process("", &environ)
	})
	if err != nil {
		return nil, fmt.Errorf("processing environment variables: %w", err)
	}
	return &environ, nil
}
