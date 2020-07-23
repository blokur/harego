# Harego

High-level library on top of [amqp][amqp].

[![Build Status](https://travis-ci.com/blokur/harego.svg?token=TM5LRGpEAwKms8UULFDi&branch=master)](https://travis-ci.com/blokur/harego)

![Harego](https://media.giphy.com/media/uNNsPzWVFzfuE/giphy.gif)


1. [Development](#development)
   - [Prerequisite](#prerequisite)
   - [Running Tests](#running-tests)
   - [Make Examples](#make-examples)
   - [Changelog](#changelog)
   - [Mocks](#mocks)
2. [Database](#database)
3. [References](#references)


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

## Database

For convenience you can trigger the `integration_deps` target to setup required
databases:

```bash
make integration_deps
```

## References

[reflex]: https://github.com/cespare/reflex
[amqp]: github.com/streadway/amqp
