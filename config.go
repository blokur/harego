package harego

import (
	"errors"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/net/context"

	"github.com/blokur/harego/v2/internal"
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
			return nil, fmt.Errorf("creating a connection to %q: %w", url, err)
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

// nolint:govet // most likely not an issue, but cleaner this way.
type config struct {
	workers      int
	consumerName string
	retryDelay   time.Duration
	logger       logr.Logger
	ctx          context.Context

	// queue properties.
	queueName  string
	routingKey string
	exclusive  bool
	queueArgs  amqp.Table

	// exchange properties.
	exchName   string
	exchType   ExchangeType
	durable    bool
	autoDelete bool
	autoAck    bool
	internal   bool
	noWait     bool

	// message properties.
	prefetchCount int
	prefetchSize  int
	deliveryMode  DeliveryMode

	chBuff int
}

func defaultConfig() *config {
	return &config{
		exchName:     "default",
		workers:      1,
		chBuff:       10,
		exchType:     ExchangeTypeTopic,
		deliveryMode: DeliveryModePersistent,
		durable:      true,
		consumerName: internal.GetRandomName(),
		retryDelay:   100 * time.Millisecond,
		logger:       logr.Discard(),
		ctx:          context.Background(),
	}
}

func (c *config) consumer() *Consumer {
	return &Consumer{
		workers:       c.workers,
		consumerName:  c.consumerName,
		retryDelay:    c.retryDelay,
		queueName:     c.queueName,
		routingKey:    c.routingKey,
		exclusive:     c.exclusive,
		queueArgs:     c.queueArgs,
		exchName:      c.exchName,
		exchType:      c.exchType,
		durable:       c.durable,
		autoDelete:    c.autoDelete,
		autoAck:       c.autoAck,
		internal:      c.internal,
		noWait:        c.noWait,
		prefetchCount: c.prefetchCount,
		prefetchSize:  c.prefetchSize,
		deliveryMode:  c.deliveryMode,
		chBuff:        c.chBuff,
		logger:        c.logger,
		ctx:           c.ctx,
	}
}

func (c *config) publisher() *Publisher {
	return &Publisher{
		workers:       c.workers,
		retryDelay:    c.retryDelay,
		routingKey:    c.routingKey,
		exclusive:     c.exclusive,
		queueArgs:     c.queueArgs,
		exchName:      c.exchName,
		exchType:      c.exchType,
		durable:       c.durable,
		autoDelete:    c.autoDelete,
		internal:      c.internal,
		noWait:        c.noWait,
		prefetchCount: c.prefetchCount,
		prefetchSize:  c.prefetchSize,
		deliveryMode:  c.deliveryMode,
		chBuff:        c.chBuff,
		logger:        c.logger,
		ctx:           c.ctx,
	}
}

// ConfigFunc is a function for setting up the Client. You should not use this
// type outside of the NewConsumer or NewPublisher function calls.
type ConfigFunc func(*config)

// QueueName sets the queue name.
func QueueName(name string) ConfigFunc {
	return func(c *config) {
		c.queueName = name
	}
}

// QueueArgs sets the args possed to the QueueDeclare method.
func QueueArgs(args amqp.Table) ConfigFunc {
	return func(c *config) {
		c.queueArgs = args
	}
}

// RoutingKey sets the routing key of the queue.
func RoutingKey(key string) ConfigFunc {
	return func(c *config) {
		c.routingKey = key
	}
}

// Workers sets the worker count for consuming messages.
func Workers(n int) ConfigFunc {
	return func(c *config) {
		c.workers = n
	}
}

// WithDeliveryMode sets the default delivery mode of messages.
func WithDeliveryMode(mode DeliveryMode) ConfigFunc {
	return func(c *config) {
		c.deliveryMode = mode
	}
}

// PrefetchCount sets how many items should be prefetched for consumption. With
// a prefetch count greater than zero, the server will deliver that many
// messages to consumers before acknowledgments are received. The server
// ignores this option when consumers are started with noAck because no
// acknowledgments are expected or sent.
func PrefetchCount(i int) ConfigFunc {
	return func(c *config) {
		c.prefetchCount = i
	}
}

// PrefetchSize sets the prefetch size of the Qos. If it is greater than zero,
// the server will try to keep at least that many bytes of deliveries flushed
// to the network before receiving acknowledgments from the consumers.
func PrefetchSize(i int) ConfigFunc {
	return func(c *config) {
		c.prefetchSize = i
	}
}

// WithExchangeType sets the exchange type. The default is ExchangeTypeTopic.
func WithExchangeType(t ExchangeType) ConfigFunc {
	return func(c *config) {
		c.exchType = t
	}
}

// ExchangeName sets the exchange name. For each worker, and additional string
// will be appended for the worker number.
func ExchangeName(name string) ConfigFunc {
	return func(c *config) {
		c.exchName = name
	}
}

// ConsumerName sets the consumer name of the consuming queue.
func ConsumerName(name string) ConfigFunc {
	return func(c *config) {
		c.consumerName = name
	}
}

// NotDurable marks the exchange and the queue not to be durable. Default is
// durable.
func NotDurable(c *config) {
	c.durable = false
}

// AutoDelete marks the exchange and queues with autoDelete property which
// causes the messages to be automatically removed from the queue when
// consumed.
func AutoDelete(c *config) {
	c.autoDelete = true
}

// Internal sets the exchange to be internal.
func Internal(c *config) {
	c.internal = true
}

// NoWait marks the exchange as noWait. When noWait is true, declare without
// waiting for a confirmation from the server. The channel may be closed as a
// result of an error.
func NoWait(c *config) {
	c.noWait = true
}

// ExclusiveQueue marks the queue as exclusive. Exclusive queues are only
// accessible by the connection that declares them and will be deleted when the
// connection closes. Channels on other connections will receive an error when
// attempting to declare, bind, consume, purge or delete a queue with the same
// name.
func ExclusiveQueue(c *config) {
	c.exclusive = true
}

// RetryDelay sets the time delay for attempting to reconnect. The default
// value is 100ms.
func RetryDelay(d time.Duration) ConfigFunc {
	return func(c *config) {
		c.retryDelay = d
	}
}

// AutoAck sets the consuming ack behaviour. The default is false.
func AutoAck(c *config) {
	c.autoAck = true
}

// Buffer sets the amount of messages each worker can keep in their channels.
func Buffer(n int) ConfigFunc {
	return func(c *config) {
		c.chBuff = n
	}
}

// Logger lets the user to provide their own logger. The default logger is a
// noop struct.
func Logger(l logr.Logger) ConfigFunc {
	return func(c *config) {
		c.logger = l
	}
}

// Context sets a context on the object that would stop it when the context is
// cancelled. The default context has no condition for cancellation.
func Context(ctx context.Context) ConfigFunc {
	return func(c *config) {
		c.ctx = ctx
	}
}
