package harego_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/arsham/retry/v2"
	"github.com/blokur/harego/v2"
	"github.com/blokur/harego/v2/mocks"
	"github.com/blokur/testament"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/pkg/ioutils"
	"github.com/go-logr/logr"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"
)

func randomBody(lines int) string {
	body := make([]string, lines)
	for i := range body {
		body[i] = testament.RandomString(rand.Intn(100) + 10)
	}
	return strings.Join(body, "\n")
}

var r = &retry.Retry{
	Attempts: 20,
	Delay:    300 * time.Millisecond,
}

func init() {
	// If you faced with any issues setting up containers, comment this out:
	testcontainers.Logger = log.New(&ioutils.NopWriter{}, "", 0)
}

// getConsumerPublisher returns a pair of consumer and publisher. What
// publisher sends to the exchange, the consumer will receive. If the queueName
// is empty, a random queueName is picked. It tries to get a new container if
// there was an error.
func getConsumerPublisher(t *testing.T, exchange, queueName string, conf ...harego.ConfigFunc) (cons *harego.Consumer, pub *harego.Publisher) {
	t.Helper()
	var (
		addr string
		c    testcontainers.Container
		ctx  = context.Background()
	)
	err := r.Do(func() error {
		var err error
		c, addr = getContainer(t)
		cons, pub, err = getConsumerPublisherWithAddr(t, addr, exchange, queueName, conf...)
		if err != nil {
			c.Terminate(ctx)
			cons.Close()
			pub.Close()
			return err
		}
		return nil
	})
	require.NoError(t, err)

	return cons, pub
}

// getConsumerPublisherWithAddr returns a pair of consumer and publisher
// connecting to a broker at the given address. What publisher sends to the
// exchange, the consumer will receive. If the queueName is empty, a random
// queueName is picked.
func getConsumerPublisherWithAddr(t *testing.T, addr, exchange, queueName string, conf ...harego.ConfigFunc) (*harego.Consumer, *harego.Publisher, error) {
	t.Helper()
	var err error
	if queueName == "" {
		queueName = testament.RandomLowerString(20)
	}

	conf = append([]harego.ConfigFunc{
		harego.ExchangeName(exchange),
		harego.QueueName(queueName),
	}, conf...)

	var (
		pub  *harego.Publisher
		cons *harego.Consumer
	)

	err = r.Do(func() error {
		var err error
		pub, err = harego.NewPublisher(harego.URLConnector(addr),
			conf...,
		)
		return err
	})
	if err != nil {
		return nil, nil, err
	}
	t.Cleanup(func() {
		pub.Close()
	})

	err = r.Do(func() error {
		var err error
		cons, err = harego.NewConsumer(harego.URLConnector(addr),
			conf...,
		)
		return err
	})
	if err != nil {
		return nil, nil, err
	}
	t.Cleanup(func() {
		cons.Close()
	})

	return cons, pub, nil
}

// getContainer returns a new container running rabbimq that is ready for
// accepting connections.
func getContainer(t *testing.T) (testcontainers.Container, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rabbitmqContainer, err := rabbitmq.RunContainer(ctx,
		testcontainers.WithImage("rabbitmq:3.13-management-alpine"),
		rabbitmq.WithAdminUsername("guest"),
		rabbitmq.WithAdminPassword("guest"),
		testcontainers.WithHostConfigModifier(func(c *container.HostConfig) {
			c.Memory = 256 * 1024 * 1024
			c.CPUShares = 500
		}),
		testcontainers.CustomizeRequestOption(func(req *testcontainers.GenericContainerRequest) error {
			req.Name = "harego_" + testament.RandomString(55)
			return nil
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := rabbitmqContainer.Terminate(ctx)
		require.NoError(t, err)
	})

	addr, err := rabbitmqContainer.AmqpURL(ctx)
	require.NoError(t, err)

	return rabbitmqContainer, addr
}

// restartRabbitMQ restarts the rabbitmq server inside the container.
func restartRabbitMQ(t *testing.T, container testcontainers.Container) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	_, _, err := container.Exec(ctx, []string{
		"rabbitmqctl",
		"stop_app",
	})
	require.NoError(t, err)

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		container.Exec(ctx, []string{
			"rabbitmqctl",
			"start_app",
		})
	}()
}

// sink represents a logging implementation.
type sink struct {
	values map[any]any
	info   [][]any
	errors []error
}

func (s *sink) Init(logr.RuntimeInfo) {}
func (s *sink) Enabled(int) bool      { return true }
func (s *sink) Info(_ int, msg string, kv ...any) {
	s.info = append(s.info, append([]any{msg}, kv...))
}

func (s *sink) Error(err error, _ string, _ ...any) {
	s.errors = append(s.errors, err)
}

func (s *sink) WithValues(kv ...any) logr.LogSink {
	for i := 0; i < len(kv); i += 2 {
		s.values[kv[i]] = kv[i+1]
	}
	return s
}

func (s *sink) WithName(string) logr.LogSink { return s }

type mockLogger struct {
	sink   *sink
	logger logr.Logger
}

func newMockLogger() *mockLogger {
	s := &sink{
		values: make(map[any]any),
	}
	return &mockLogger{
		sink:   s,
		logger: logr.New(s),
	}
}

func (m *mockLogger) isInError(t *testing.T, err error) {
	t.Helper()
	// We give the errors.Is a chance to traverse the error chain first.
	for _, needle := range m.sink.errors {
		if errors.Is(needle, err) {
			return
		}
	}
	for _, needle := range m.sink.errors {
		if strings.Contains(needle.Error(), err.Error()) {
			return
		}
	}
	t.Errorf("expected error %v to be in %v", err, m.sink.errors)
}

type acknowledger struct {
	ackFunc    func(tag uint64, multiple bool) error
	nackFunc   func(tag uint64, multiple, requeue bool) error
	rejectFunc func(tag uint64, requeue bool) error
}

func (a *acknowledger) Ack(tag uint64, multiple bool) error {
	if a.ackFunc != nil {
		return a.ackFunc(tag, multiple)
	}
	return nil
}

func (a *acknowledger) Nack(tag uint64, multiple, requeue bool) error {
	if a.nackFunc != nil {
		return a.nackFunc(tag, multiple, requeue)
	}
	return nil
}

func (a *acknowledger) Reject(tag uint64, multiple bool) error {
	if a.rejectFunc != nil {
		return a.rejectFunc(tag, multiple)
	}
	return nil
}

// getPassingChannel returns a mock of the amqp.Channel interface that has
// messages in it's queue.
func getPassingChannel(t *testing.T, messages int) *mocks.Channel {
	t.Helper()
	ch := mocks.NewChannel(t)
	ch.On("Qos", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Maybe()
	ch.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Maybe()
	ch.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything).
		Return(amqp.Queue{}, nil).Maybe()
	ch.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Maybe()
	ch.On("NotifyClose", mock.Anything).
		Return(make(chan *amqp.Error, 10)).Maybe()

	delivery := make(chan amqp.Delivery, messages)
	for i := 0; i < messages; i++ {
		delivery <- amqp.Delivery{
			Acknowledger: &acknowledger{},
			Body:         []byte(fmt.Sprintf("message #%d", i)),
		}
	}
	ch.On("Consume", mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Return((<-chan amqp.Delivery)(delivery), nil).Maybe()
	ch.On("Publish", mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything).
		Return(nil).Maybe()
	ch.On("Close").Return(nil).Maybe()
	return ch
}
