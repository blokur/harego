package harego

import (
	"context"
	"time"

	"github.com/streadway/amqp"
)

// RabbitMQ defines a rabbitmq exchange.
//go:generate mockery -name RabbitMQ -filename rabbitmq_mock.go
type RabbitMQ interface {
	Channel() (Channel, error)
	Close() error
}

// A Channel can operate queues. This is a subset of the amqp.Channel api.
//go:generate mockery -name Channel -filename channel_mock.go
type Channel interface {
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	Qos(prefetchCount, prefetchSize int, global bool) error
	Close() error
	NotifyClose(receiver chan *amqp.Error) chan *amqp.Error
}

// A HandlerFunc receives a message when it is available. The returned AckType
// dictates how to deal with the message. The delay can be 0 or any duration.
// The consumer will sleep this amount before sending Ack. If the user needs to
// requeue the message, they may mutate the message if required. Mutate the msg
// at your own peril.
type HandlerFunc func(msg *amqp.Delivery) (a AckType, delay time.Duration)

// Client is a concurrent safe construct for publishing a message to an
// exchange. It creates multiple workers for safe communication. Zero value is
// not usable.
type Client interface {
	Publish(msg *amqp.Publishing) error
	Consume(ctx context.Context, handler HandlerFunc) error
	Close() error
}
