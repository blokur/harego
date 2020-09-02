package mocks

import (
	"github.com/streadway/amqp"
)

// ChannelSimple mocks the Channel type.
type ChannelSimple struct {
	CloseFunc           func() error
	ConsumeFunc         func(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	ExchangeDeclareFunc func(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	NotifyCloseFunc     func(receiver chan *amqp.Error) chan *amqp.Error
	PublishFunc         func(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	QosFunc             func(prefetchCount int, prefetchSize int, global bool) error
	QueueBindFunc       func(name, key, exchange string, noWait bool, args amqp.Table) error
	QueueDeclareFunc    func(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
}

// Close mocks the Channel.Close() method.
func (c *ChannelSimple) Close() error {
	if c.CloseFunc != nil {
		return c.CloseFunc()
	}
	return nil
}

// Consume mocks the Channel.Consume() method.
func (c *ChannelSimple) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	if c.ConsumeFunc != nil {
		return c.ConsumeFunc(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
	}
	ch := make(chan amqp.Delivery)
	close(ch)
	return ch, nil
}

// ExchangeDeclare mocks the Channel.ExchangeDeclare() method.
func (c *ChannelSimple) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	if c.ExchangeDeclareFunc != nil {
		return c.ExchangeDeclareFunc(name, kind, durable, autoDelete, internal, noWait, args)
	}
	return nil
}

// NotifyClose mocks the Channel.NotifyClose() method.
func (c *ChannelSimple) NotifyClose(receiver chan *amqp.Error) chan *amqp.Error {
	if c.NotifyCloseFunc != nil {
		return c.NotifyCloseFunc(receiver)
	}
	return make(chan *amqp.Error, 100)
}

// Publish mocks the Channel.Publish() method.
// nolint:gocritic // this is a mock.
func (c *ChannelSimple) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	if c.PublishFunc != nil {
		return c.PublishFunc(exchange, key, mandatory, immediate, msg)
	}
	return nil
}

// Qos mocks the Channel.Qos() method.
func (c *ChannelSimple) Qos(prefetchCount, prefetchSize int, global bool) error {
	if c.QosFunc != nil {
		return c.QosFunc(prefetchCount, prefetchSize, global)
	}
	return nil
}

// QueueBind mocks the Channel.QueueBind() method.
func (c *ChannelSimple) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	if c.QueueBindFunc != nil {
		return c.QueueBindFunc(name, key, exchange, noWait, args)
	}
	return nil
}

// QueueDeclare mocks the Channel.QueueDeclare() method.
func (c *ChannelSimple) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	if c.QueueDeclareFunc != nil {
		return c.QueueDeclareFunc(name, durable, autoDelete, exclusive, noWait, args)
	}
	return amqp.Queue{}, nil
}
