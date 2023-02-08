package harego

import (
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMQ defines a rabbitmq exchange.
//
//go:generate mockery --name RabbitMQ --filename rabbitmq_mock.go
type RabbitMQ interface {
	Channel() (Channel, error)
	Close() error
}

// A HandlerFunc receives a message when it is available. The returned AckType
// dictates how to deal with the message. The delay can be 0 or any duration.
// The consumer will sleep this amount before sending Ack. If the user needs to
// requeue the message, they may mutate the message if required. Mutate the msg
// at your own peril.
type HandlerFunc func(msg *amqp.Delivery) (a AckType, delay time.Duration)

// A Channel can operate queues. This is a subset of the amqp.Channel api.
//
//go:generate mockery --name Channel --filename channel_mock.go
type Channel interface {
	// ExchangeDeclare declares an exchange on the server. If the exchange does
	// not already exist, the server will create it. If the exchange exists,
	// the server verifies that it is of the provided type, durability and
	// auto-delete flags.
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error

	// Publish sends a Publishing from the client to an exchange on the server.
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error

	// QueueDeclare declares a queue to hold messages and deliver to consumers.
	// Declaring creates a queue if it doesn't already exist, or ensures that
	// an existing queue matches the same parameters.
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)

	// QueueBind binds an exchange to a queue so that publishings to the
	// exchange will be routed to the queue when the publishing routing key
	// matches the binding routing key.
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error

	// Consume immediately starts delivering queued messages.
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)

	// Qos controls how many messages or how many bytes the server will try to
	// keep on the network for consumers before receiving delivery acks. The
	// intent of Qos is to make sure the network buffers stay full between the
	// server and client.
	Qos(prefetchCount, prefetchSize int, global bool) error

	// Close initiate a clean channel closure by sending a close message with
	// the error code set to '200'.
	Close() error

	// NotifyClose registers a listener for when the server sends a channel or
	// connection exception in the form of a Connection.Close or Channel.Close
	// method. Connection exceptions will be broadcast to all open channels and
	// all channels will be closed, where channel exceptions will only be
	// broadcast to listeners to this channel.
	NotifyClose(receiver chan *amqp.Error) chan *amqp.Error
}

// logger implements ways of writing user facing logs to the stdout/stderr.
type logger interface {
	Warn(args ...interface{})
	Warnf(format string, args ...interface{})
}

type nullLogger struct{}

func (nullLogger) Warn(args ...interface{})                 {}
func (nullLogger) Warnf(format string, args ...interface{}) {}
