package harego_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/arsham/retry/v2"
	"github.com/blokur/testament"
	"github.com/go-logr/logr"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/blokur/harego/v2"
	"github.com/blokur/harego/v2/internal"
	"github.com/blokur/harego/v2/mocks"
)

func randomBody(lines int) string {
	body := make([]string, lines)
	for i := range body {
		body[i] = testament.RandomString(rand.Intn(100) + 10)
	}
	return strings.Join(body, "\n")
}

// getConsumerPublisher returns a pair of consumer and publisher. What
// publisher sends to the exchange, the consumer will receive. If the queueName
// is empty, a random queueName is picked.
func getConsumerPublisher(t *testing.T, vh, exchange, queueName string, conf ...harego.ConfigFunc) (*harego.Consumer, *harego.Publisher) {
	t.Helper()
	var (
		adminURL string
		err      error
	)
	if queueName == "" {
		queueName = testament.RandomLowerString(20)
	}
	env, err := internal.GetEnv()
	require.NoError(t, err)
	apiAddress := strings.Split(env.RabbitMQAddr, ":")[0]
	adminPort := 15672
	if v, ok := os.LookupEnv("RABBITMQ_ADMIN_PORT"); ok {
		adminPort, err = strconv.Atoi(v)
		if err != nil {
			adminPort = 15672
		}
	}
	if vh != "" {
		adminURL = fmt.Sprintf("http://%s:%d/api/vhosts/%s", apiAddress, adminPort, vh)
		req, err := http.NewRequestWithContext(context.Background(), "PUT", adminURL, http.NoBody)
		require.NoError(t, err)
		req.SetBasicAuth(env.RabbitMQUser, env.RabbitMQPass)
		resp, err := http.DefaultClient.Do(req)
		if resp != nil && resp.Body != nil {
			defer func() {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}()
		}
		require.NoError(t, err)
	}

	url := fmt.Sprintf("amqp://%s:%s@%s/%s", env.RabbitMQUser, env.RabbitMQPass, env.RabbitMQAddr, vh)
	conf = append([]harego.ConfigFunc{
		harego.ExchangeName(exchange),
		harego.QueueName(queueName),
	}, conf...)

	var (
		pub  *harego.Publisher
		cons *harego.Consumer
		r    = &retry.Retry{
			Attempts: 20,
			Delay:    200 * time.Millisecond,
		}
	)

	err = r.Do(func() error {
		var err error
		pub, err = harego.NewPublisher(harego.URLConnector(url),
			conf...,
		)
		return err
	})
	require.NoError(t, err)

	err = r.Do(func() error {
		var err error
		cons, err = harego.NewConsumer(harego.URLConnector(url),
			conf...,
		)
		return err
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		pub.Close()
		cons.Close()
		assert.Eventually(t, func() bool {
			cons.Close()
			return true
		}, 2*time.Second, 10*time.Millisecond)
		urls := []string{
			fmt.Sprintf("http://%s:%d/api/exchanges/%s", apiAddress, adminPort, exchange),
		}
		urls = append(urls,
			fmt.Sprintf("http://%s:%d/api/queues/%s", apiAddress, adminPort, queueName),
		)

		if vh != "" {
			urls = append(urls, adminURL)
		}

		for _, url := range urls {
			func() {
				req, err := http.NewRequestWithContext(context.Background(), "DELETE", url, http.NoBody)
				require.NoError(t, err)
				req.SetBasicAuth(env.RabbitMQUser, env.RabbitMQPass)
				resp, err := http.DefaultClient.Do(req)
				if resp != nil && resp.Body != nil {
					defer func() {
						io.Copy(io.Discard, resp.Body)
						resp.Body.Close()
					}()
				}
				require.NoError(t, err)
			}()
		}
	})
	return cons, pub
}

// getContainer returns a new container running rabbimq that is ready for
// accepting connections.
func getContainer(t *testing.T) (container testcontainers.Container, addr string) {
	t.Helper()
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "rabbitmq:3-management-alpine",
		ExposedPorts: []string{"5672/tcp", "15672/tcp"},
		WaitingFor:   wait.ForListeningPort("5672/tcp"),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)

	ip, err := container.Host(ctx)
	require.NoError(t, err)

	port, err := container.MappedPort(ctx, "5672")
	require.NoError(t, err)

	t.Cleanup(func() {
		container.Terminate(ctx)
	})
	return container, fmt.Sprintf("amqp://%s:%s/", ip, port.Port())
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
