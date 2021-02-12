# Harego

High-level library on top of [amqp][amqp].

[![Build Status](https://travis-ci.com/blokur/harego.svg?token=TM5LRGpEAwKms8UULFDi&branch=master)](https://travis-ci.com/blokur/harego)

![Harego](https://media.giphy.com/media/uNNsPzWVFzfuE/giphy.gif)


1. [Description](#description)
   - [Note](#note)
2. [Development](#development)
   - [Prerequisite](#prerequisite)
   - [Running Tests](#running-tests)
   - [Make Examples](#make-examples)
   - [Changelog](#changelog)
   - [Mocks](#mocks)
   - [RabbitMQ](#rabbitmq)
3. [References](#references)


## Description

A harego.Client is a concurrent safe queue manager for RabbitMQ, and a
high-level implementation on top of [amqp](github.com/streadway/amqp) library.
The only requirement for an Client to operate is the address to the broker. The
Client creates one or more workers for publishing/consuming messages. The
default values are chosen to make the Client a durable queue working with the
`default` exchange and `topic` kind. Client can be configure by passing
provided `ConfigFunc` functions to NewClient() constructor.

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

## Development

### Prerequisite

This project supports Go > `1.14`. To run targets from the `Makefile` you need
to install GNU make.

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

### Changelog

You need to update the changelogs file before each release. In order to update
the changelogs file run the following:

```bash
make changelog
```

When you are ready to make a commitment and tag the next release, use this
target and pass in the next tag:

```bash
make changelog_release tag=v1.0.1
```

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

## References

[reflex]: https://github.com/cespare/reflex
[amqp]: github.com/streadway/amqp
