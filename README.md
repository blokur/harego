# Harego

High-level library on top of [amqp][amqp].

[![Build Status](https://travis-ci.com/blokur/harego.svg?token=TM5LRGpEAwKms8UULFDi&branch=master)](https://travis-ci.com/blokur/harego)

![Harego](https://media.giphy.com/media/uNNsPzWVFzfuE/giphy.gif)

1. [Description](#description)
   - [Note](#note)
2. [Usage](#usage)
   - [Consumer](#consumer)
   - [Publisher](#publisher)
   - [Delays](#delays)
   - [Requeueing](#requeueing)
3. [Development](#development)
   - [Prerequisite](#prerequisite)
   - [Running Tests](#running-tests)
   - [Make Examples](#make-examples)
   - [Mocks](#mocks)
   - [RabbitMQ](#rabbitmq)

## Description

A `harego.Consumer`/`harego.Publisher` is a concurrent safe queue manager for
RabbitMQ, and a high-level implementation on top of
[amqp](github.com/rabbitmq/amqp091-go) library. A Consumer/Publisher creates
one or more workers for publishing/consuming messages. The default values are
chosen to make the Consumer/Publisher a durable queue working with the
`default` exchange and `topic` kind. Consumer/Publisher can be configure by
passing provided `ConfigFunc` functions to NewConsumer/NewPublisher
constructors.

The `Consume()` method will call the provided `HandlerFunc` with the next
available message on the next available worker. The return value of the
`HandlerFunc` decided what would happen to the message. The `Consume` worker
will delay before act on the `ack` for the amount of time the `HandlerFunc`
returns as the second value.

You can increase the worker sizes by passing `Workers(n)` to the `NewClient`
constructor.

When the `Close()` method is called, all connections will be closed and the
`Client` will be useless. You can create a new object for more works.

### Note

This library is in beta phase and the API might change until we reach a stable
release.

## Usage

### Consumer

The only requirement for the NewConsumer function is a Connector to connect to
the broker when needed:

```go
// to use an address:
harego.NewConsumer(harego.URLConnector(address))
// to use an amqp connection:
harego.NewConsumer(harego.AMQPConnector(conn))
```

The connector is used when the connection is lost, so the Client can initiate a
new connection.

In this setup the `myqueue` is bound to the `myexchange` exchange, and handler
is called for each message that are read from this queue:

```go
consumer, err := harego.NewConsumer(harego.URLConnector(address),
	harego.ExchangeName("myexchange"),
	harego.QueueName("myqueue"),
)
// handle the error.
err = consumer.Consume(ctx, func(msg *amqp.Delivery) (harego.AckType, time.Duration) {
	return harego.AckTypeAck, 0
})
// handle the error.
```

You can create multiple workers in the above example for concurrently handle
multiple messages:

```go
consumer, err := harego.NewConsumer(harego.URLConnector(address),
	harego.ExchangeName("myexchange"),
	harego.QueueName("myqueue"),
	harego.Workers(20),
)
// handle the error.
err = consumer.Consume(ctx, func(msg *amqp.Delivery) (harego.AckType, time.Duration) {
	return harego.AckTypeAck, 0
})
// handle the error.
```

The handler will receive 20 messages concurrently and the Ack is sent for each
message separately.

### Publisher

In this setup the message is sent to the `myexchange` exchange:

```go
publisher, err := harego.NewPublisher(harego.URLConnector(address),
	harego.ExchangeName("myexchange"),
)
// handle the error.
err = publisher.Publish(&amqp.Publishing{
	Body: []byte(msg),
})
// handle the error.
```

### Delays

If the returned duration is 0, the acknowledgement is sent to the broker
immediately. Otherwise Consume function sleeps for that duration before it's
been sent. Please note that the delay will cause the current handler to sleep
for this duration, therefore you need enough workers to be able to handle next
available messages.

### Requeueing

If you return a `harego.AckTypeRequeue` from the handler, the message is sent
back to the same queue. This means this message will be consumed after all
messages in the queue is consumed.

## Development

### Prerequisite

This project supports Go >= `1.20.`. To run targets from the `Makefile` you
need to install GNU make. You also need docker installed for integration tests.

If you have [asdf][asdf] installed, the .tool-versions file will set the go
version to the latest supported version.

In order to install dependencies:

```bash
make dependencies
```

This also installs [reflex][reflex] to help with development process.

To run this application you need to provide the following settings as
environment variables or application arguments:

```
RABBITMQ_PORT
RABBITMQ_ADDR
RABBITMQ_ADMIN_PORT
RABBITMQ_USER
RABBITMQ_PASSWORD
RABBITMQ_VH
```

### Running Tests

To watch for file changes and run unittest:

```bash
make unittest
# or to run them with race flag:
make unittest_race
```

There is also a `integration_test` target for running integration tests.

### Make Examples

```bash
make unittest
make unittest run=TestMyTest # runs a specific test with regexp
make unittest dir=./db/...   # runs tests in a package
make unittest dir=./db/... run=TestSomethingElse
make unittest flags="-race -count=2"
```

Please see the Makefile for more targets.

### Mocks

To generate mocks run:

```bash
make mocks
```

## RabbitMQ

For convenience you can trigger the `integration_deps` target to setup required
RabbitMQ instance:

```bash
make integration_deps
```

[reflex]: https://github.com/cespare/reflex
[amqp]: https://github.com/rabbitmq/amqp091-go
[asdf]: https://github.com/asdf-vm/asdf
