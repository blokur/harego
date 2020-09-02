package mocks

import (
	"github.com/blokur/harego"
)

// RabbitMQSimple is a simple mock type for the RabbitMQ.
type RabbitMQSimple struct {
	ChannelFunc func() (harego.Channel, error)
	CloseFunc   func() error
}

// Channel mocks the RabbitMQ.Channel() method.
func (r *RabbitMQSimple) Channel() (harego.Channel, error) {
	if r.ChannelFunc != nil {
		return r.ChannelFunc()
	}
	return &ChannelSimple{}, nil
}

// Close mocks the RabbitMQ.Close() method.
func (r *RabbitMQSimple) Close() error {
	if r.CloseFunc != nil {
		return r.CloseFunc()
	}
	return nil
}
