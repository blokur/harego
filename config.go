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

// QueueName set the queueName property of the client.
func QueueName(name string) ConfigFunc {
	return func(c *Client) {
		c.queueName = name
	}
}

// RoutingKey set the routingKey property of the client.
func RoutingKey(key string) ConfigFunc {
	return func(c *Client) {
		c.routingKey = key
	}
}

// Workers set the workers property of the client.
func Workers(n int) ConfigFunc {
	return func(c *Client) {
		c.workers = n
	}
}

// WithDeliveryMode set the deliveryMode property of the client.
func WithDeliveryMode(mode DeliveryMode) ConfigFunc {
	return func(c *Client) {
		c.deliveryMode = mode
	}
}

// PrefetchCount set the prefetchCount property of the client.
func PrefetchCount(i int) ConfigFunc {
	return func(c *Client) {
		c.prefetchCount = i
	}
}

// PrefetchSize set the prefetchSize property of the client.
func PrefetchSize(i int) ConfigFunc {
	return func(c *Client) {
		c.prefetchSize = i
	}
}

// WithClientType set the exchType property of the client.
func WithClientType(t ExchangeType) ConfigFunc {
	return func(c *Client) {
		c.exchType = t
	}
}

// ExchangeName set the exchName property of the client.
func ExchangeName(name string) ConfigFunc {
	return func(c *Client) {
		c.exchName = name
	}
}

// ConsumerName set the consumerName property of the client.
func ConsumerName(name string) ConfigFunc {
	return func(c *Client) {
		c.consumerName = name
	}
}

// DurableClient set the durable property of the client.
func DurableClient(b bool) ConfigFunc {
	return func(c *Client) {
		c.durable = b
	}
}

// AutoDeleteClient set the autoDelete property of the client.
func AutoDeleteClient(b bool) ConfigFunc {
	return func(c *Client) {
		c.autoDelete = b
	}
}

// InternalClient set the internal property of the client.
func InternalClient(b bool) ConfigFunc {
	return func(c *Client) {
		c.internal = b
	}
}

// NoWaitClient set the noWait property of the client.
func NoWaitClient(b bool) ConfigFunc {
	return func(c *Client) {
		c.noWait = b
	}
}
