package harego

import "github.com/streadway/amqp"

//go:generate stringer -type=AckType,DeliveryMode -output=constants_string.go

// AckType specifies how the message is acknowledged to RabbotMQ.
type AckType int

const (
	// AckTypeAck causes the message to be removed in broker. The multiple value is
	// false, causing the broker to act on one message.
	AckTypeAck AckType = iota

	// AckTypeNack causes the message to be requeued in broker. The multiple value
	// is false, causing the broker to act on one message.
	AckTypeNack

	// AckTypeReject causes the message to be dropped in broker. AckTypeNack must
	// not be used to select or requeue messages the client wishes not to handle,
	// rather it is to inform the server that the client is incapable of handling
	// this message at this time.
	AckTypeReject

	// AckTypeRequeue causes the message to be requeued back to the end of the
	// queue.
	AckTypeRequeue

	_invalidAckType
)

// IsValid returns true if the object is within the valid boundries.
func (a AckType) IsValid() bool {
	return a < _invalidAckType && a >= 0
}

// ExchangeType is the kind of exchange.
type ExchangeType int

const (
	// ExchangeTypeDirect defines a direct exchange.
	ExchangeTypeDirect ExchangeType = iota

	// ExchangeTypeFanout defines a fanout exchange.
	ExchangeTypeFanout

	// ExchangeTypeTopic defines a topic exchange.
	ExchangeTypeTopic

	// ExchangeTypeHeaders defines a headers exchange.
	ExchangeTypeHeaders

	_invalidExchangeType
)

// IsValid returns true if the object is within the valid boundries.
func (e ExchangeType) IsValid() bool {
	return e < _invalidExchangeType && e >= 0
}

func (e ExchangeType) String() string {
	switch e {
	case ExchangeTypeDirect:
		return "direct"
	case ExchangeTypeFanout:
		return "fanout"
	case ExchangeTypeTopic:
		return "topic"
	case ExchangeTypeHeaders:
		return "headers"
	}
	return ""
}

// DeliveryMode is the DeliveryMode of a amqp.Publishing message.
type DeliveryMode uint8

const (
	// DeliveryModeTransient means higher throughput but messages will not be
	// restored on broker restart. The delivery mode of publishings is unrelated
	// to the durability of the queues they reside on. Transient messages will not
	// be restored to durable queues
	DeliveryModeTransient = DeliveryMode(amqp.Transient)

	// DeliveryModePersistent messages will be restored to
	// durable queues and lost on non-durable queues during server restart.
	DeliveryModePersistent = DeliveryMode(amqp.Persistent)
)

// IsValid returns true if the object is within the valid boundries.
func (d DeliveryMode) IsValid() bool {
	return d == DeliveryModePersistent || d == DeliveryModeTransient
}
