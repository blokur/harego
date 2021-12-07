package harego

import (
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

// rabbitWrapper is defined to make it easy for passing a mocked connection.
type rabbitWrapper struct {
	*amqp.Connection
}

// Channel returns the underlying channel.
func (r *rabbitWrapper) Channel() (Channel, error) {
	return r.Connection.Channel() //nolint:wrapcheck // Okay here.
}

// A Connector should return a live connection. It will be called during the
// Client initialisation and during reconnection process.
type Connector func() (RabbitMQ, error)

// URLConnector creates a new connection from url.
func URLConnector(url string) Connector {
	return func() (RabbitMQ, error) {
		conn, err := amqp.Dial(url)
		if err != nil {
			return nil, errors.Wrapf(err, "creating a connection to %q", url)
		}
		return &rabbitWrapper{conn}, nil
	}
}

// AMQPConnector uses r everytime the Client needs a new connection. You should
// make sure r keep being alive.
func AMQPConnector(r *amqp.Connection) Connector {
	return func() (RabbitMQ, error) {
		if r.IsClosed() {
			return nil, errors.New("connection is closed")
		}
		return &rabbitWrapper{r}, nil
	}
}

// ConfigFunc is a function for setting up the Client. A config function returns
// an error if the client is already started. You should not use this type
// outside of the NewClient function call.
type ConfigFunc func(*Client) error

// QueueName sets the queue name.
func QueueName(name string) ConfigFunc {
	return func(c *Client) error {
		if c.started {
			return ErrAlreadyConfigured
		}
		c.queueName = name
		return nil
	}
}

// QueueArgs sets the args possed to the QueueDeclare method.
func QueueArgs(args amqp.Table) ConfigFunc {
	return func(c *Client) error {
		if c.started {
			return ErrAlreadyConfigured
		}
		c.queueArgs = args
		return nil
	}
}

// RoutingKey sets the routing key of the queue.
func RoutingKey(key string) ConfigFunc {
	return func(c *Client) error {
		if c.started {
			return ErrAlreadyConfigured
		}
		c.routingKey = key
		return nil
	}
}

// Workers sets the worker count for consuming messages.
func Workers(n int) ConfigFunc {
	return func(c *Client) error {
		if c.started {
			return ErrAlreadyConfigured
		}
		c.workers = n
		return nil
	}
}

// WithDeliveryMode sets the default delivery mode of messages.
func WithDeliveryMode(mode DeliveryMode) ConfigFunc {
	return func(c *Client) error {
		if c.started {
			return ErrAlreadyConfigured
		}
		c.deliveryMode = mode
		return nil
	}
}

// PrefetchCount sets how many items should be prefetched for consumption. With
// a prefetch count greater than zero, the server will deliver that many
// messages to consumers before acknowledgments are received. The server
// ignores this option when consumers are started with noAck because no
// acknowledgments are expected or sent.
func PrefetchCount(i int) ConfigFunc {
	return func(c *Client) error {
		if c.started {
			return ErrAlreadyConfigured
		}
		c.prefetchCount = i
		return nil
	}
}

// PrefetchSize sets the prefetch size of the Qos. If it is greater than zero,
// the server will try to keep at least that many bytes of deliveries flushed
// to the network before receiving acknowledgments from the consumers.
func PrefetchSize(i int) ConfigFunc {
	return func(c *Client) error {
		if c.started {
			return ErrAlreadyConfigured
		}
		c.prefetchSize = i
		return nil
	}
}

// WithExchangeType sets the exchange type. The default is ExchangeTypeTopic.
func WithExchangeType(t ExchangeType) ConfigFunc {
	return func(c *Client) error {
		if c.started {
			return ErrAlreadyConfigured
		}
		c.exchType = t
		return nil
	}
}

// ExchangeName sets the exchange name. For each worker, and additional string
// will be appended for the worker number.
func ExchangeName(name string) ConfigFunc {
	return func(c *Client) error {
		if c.started {
			return ErrAlreadyConfigured
		}
		c.exchName = name
		return nil
	}
}

// ConsumerName sets the consumer name of the consuming queue.
func ConsumerName(name string) ConfigFunc {
	return func(c *Client) error {
		if c.started {
			return ErrAlreadyConfigured
		}
		c.consumerName = name
		return nil
	}
}

// NotDurable marks the exchange and the queue not to be durable. Default is
// durable.
func NotDurable(c *Client) error {
	if c.started {
		return ErrAlreadyConfigured
	}
	c.durable = false
	return nil
}

// AutoDelete marks the exchange and queues with autoDelete property which
// causes the messages to be automatically removed from the queue when
// consumed.
func AutoDelete(c *Client) error {
	if c.started {
		return ErrAlreadyConfigured
	}
	c.autoDelete = true
	return nil
}

// Internal sets the exchange to be internal.
func Internal(c *Client) error {
	if c.started {
		return ErrAlreadyConfigured
	}
	c.internal = true
	return nil
}

// NoWait marks the exchange as noWait. When noWait is true, declare without
// waiting for a confirmation from the server. The channel may be closed as a
// result of an error.
func NoWait(c *Client) error {
	if c.started {
		return ErrAlreadyConfigured
	}
	c.noWait = true
	return nil
}

// ExclusiveQueue marks the queue as exclusive. Exclusive queues are only
// accessible by the connection that declares them and will be deleted when the
// connection closes. Channels on other connections will receive an error when
// attempting to declare, bind, consume, purge or delete a queue with the same
// name.
func ExclusiveQueue(c *Client) error {
	if c.started {
		return ErrAlreadyConfigured
	}
	c.exclusive = true
	return nil
}
