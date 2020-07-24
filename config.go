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

// ConfigFunc is a function for setting up the Exchange.
type ConfigFunc func(*Exchange)

// Connection sets the RabbitMQ connection.
func Connection(c *amqp.Connection) ConfigFunc {
	return func(e *Exchange) {
		e.conn = &rabbitWrapper{conn: c}
	}
}

// WithRabbitMQMock sets up a mock version of RabbitMQ.
func WithRabbitMQMock(r RabbitMQ) ConfigFunc {
	return func(e *Exchange) {
		e.conn = r
	}
}

// QueueName set the queueName property of the exchange.
func QueueName(name string) ConfigFunc {
	return func(e *Exchange) {
		e.queueName = name
	}
}

// RoutingKey set the routingKey property of the exchange.
func RoutingKey(key string) ConfigFunc {
	return func(e *Exchange) {
		e.routingKey = key
	}
}

// Workers set the workers property of the exchange.
func Workers(n int) ConfigFunc {
	return func(e *Exchange) {
		e.workers = n
	}
}

// WithDeliveryMode set the deliveryMode property of the exchange.
func WithDeliveryMode(mode DeliveryMode) ConfigFunc {
	return func(e *Exchange) {
		e.deliveryMode = mode
	}
}

// PrefetchCount set the prefetchCount property of the exchange.
func PrefetchCount(i int) ConfigFunc {
	return func(e *Exchange) {
		e.prefetchCount = i
	}
}

// PrefetchSize set the prefetchSize property of the exchange.
func PrefetchSize(i int) ConfigFunc {
	return func(e *Exchange) {
		e.prefetchSize = i
	}
}

// WithExchangeType set the exchType property of the exchange.
func WithExchangeType(t ExchangeType) ConfigFunc {
	return func(e *Exchange) {
		e.exchType = t
	}
}

// ExchangeName set the exchName property of the exchange.
func ExchangeName(name string) ConfigFunc {
	return func(e *Exchange) {
		e.exchName = name
	}
}

// ConsumerName set the consumerName property of the exchange.
func ConsumerName(name string) ConfigFunc {
	return func(e *Exchange) {
		e.consumerName = name
	}
}

// DurableExchange set the durable property of the exchange.
func DurableExchange(b bool) ConfigFunc {
	return func(e *Exchange) {
		e.durable = b
	}
}

// AutoDeleteExchange set the autoDelete property of the exchange.
func AutoDeleteExchange(b bool) ConfigFunc {
	return func(e *Exchange) {
		e.autoDelete = b
	}
}

// InternalExchange set the internal property of the exchange.
func InternalExchange(b bool) ConfigFunc {
	return func(e *Exchange) {
		e.internal = b
	}
}

// NoWaitExchange set the noWait property of the exchange.
func NoWaitExchange(b bool) ConfigFunc {
	return func(e *Exchange) {
		e.noWait = b
	}
}
