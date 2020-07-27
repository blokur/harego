package harego

import (
	"github.com/streadway/amqp"
)

// rabbitWrapper is defined to make it easy for passing a mocked connection.
type rabbitWrapper struct {
	conn *amqp.Connection
}

func (r *rabbitWrapper) Close() error {
	return r.conn.Close()
}

func (r *rabbitWrapper) Channel() (Channel, error) {
	return r.conn.Channel()
}

// ConfigFunc is a function for setting up the Client.
type ConfigFunc func(*Client)

// Connection sets the RabbitMQ connection.
func Connection(conn *amqp.Connection) ConfigFunc {
	return func(c *Client) {
		c.conn = &rabbitWrapper{conn: conn}
	}
}

// WithRabbitMQMock sets up a mock version of RabbitMQ.
func WithRabbitMQMock(r RabbitMQ) ConfigFunc {
	return func(c *Client) {
		c.conn = r
	}
}

// QueueName sets the queue name.
func QueueName(name string) ConfigFunc {
	return func(c *Client) {
		c.queueName = name
	}
}

// RoutingKey sets the routing key of the queue.
func RoutingKey(key string) ConfigFunc {
	return func(c *Client) {
		c.routingKey = key
	}
}

// Workers sets the worker count for cusuming messages.
func Workers(n int) ConfigFunc {
	return func(c *Client) {
		c.workers = n
	}
}

// WithDeliveryMode sets the default delivery mode of messages.
func WithDeliveryMode(mode DeliveryMode) ConfigFunc {
	return func(c *Client) {
		c.deliveryMode = mode
	}
}

// PrefetchCount sets how many items should be prefetched for consumption. With
// a prefetch count greater than zero, the server will deliver that many
// messages to consumers before acknowledgments are received. The server ignores
// this option when consumers are started with noAck because no acknowledgments
// are expected or sent.
func PrefetchCount(i int) ConfigFunc {
	return func(c *Client) {
		c.prefetchCount = i
	}
}

// PrefetchSize sets the prefetch size of the Qos. If it is greater than zero,
// the server will try to keep at least that many bytes of deliveries flushed to
// the network before receiving acknowledgments from the consumers.
func PrefetchSize(i int) ConfigFunc {
	return func(c *Client) {
		c.prefetchSize = i
	}
}

// WithExchangeType sets the exchange type. The default is ExchangeTypeTopic.
func WithExchangeType(t ExchangeType) ConfigFunc {
	return func(c *Client) {
		c.exchType = t
	}
}

// ExchangeName sets the exchange name.
func ExchangeName(name string) ConfigFunc {
	return func(c *Client) {
		c.exchName = name
	}
}

// ConsumerName sets the consumer name of the consuming queue.
func ConsumerName(name string) ConfigFunc {
	return func(c *Client) {
		c.consumerName = name
	}
}

// Durable marks the exchange and the queue to be durable.
func Durable(b bool) ConfigFunc {
	return func(c *Client) {
		c.durable = b
	}
}

// AutoDelete marks the exchange and queues with autoDelete property which
// causes the messages to be automatically removed from the queue when
// consumed.
func AutoDelete(b bool) ConfigFunc {
	return func(c *Client) {
		c.autoDelete = b
	}
}

// Internal sets the exchange to be internal.
func Internal(b bool) ConfigFunc {
	return func(c *Client) {
		c.internal = b
	}
}

// NoWait marks the exchange as noWait. When noWait is true, declare
// without waiting for a confirmation from the server. The channel may be closed
// as a result of an error.
func NoWait(b bool) ConfigFunc {
	return func(c *Client) {
		c.noWait = b
	}
}
