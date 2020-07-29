package harego_test

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/blokur/harego"
	"github.com/blokur/harego/internal"
	"github.com/spf13/viper"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func init() {
	viper.AutomaticEnv()
	rand.Seed(time.Now().UnixNano())

	internal.RabbitMQAddr = viper.GetString(internal.RabbitMQAddrName)
	internal.RabbitMQUser = viper.GetString(internal.RabbitMQUserName)
	internal.RabbitMQPass = viper.GetString(internal.RabbitMQPassName)
	internal.RabbitMQVirtual = viper.GetString(internal.RabbitMQVirtualName)
}

func randomString(count int) string {
	const runes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, count)
	for i := range b {
		b[i] = runes[rand.Intn(len(runes))]
	}
	return string(b)
}

func randomBody(lines int) string {
	body := make([]string, lines)
	for i := range body {
		body[i] = randomString(rand.Intn(100) + 10)
	}
	return strings.Join(body, "\n")
}

func getClient(t *testing.T) *harego.Client {
	t.Helper()
	exchange := "test." + randomString(20)
	queueName := "test." + randomString(20)
	return getNamedClient(t, exchange, queueName)
}

func getNamedClient(t *testing.T, exchange, queueName string) *harego.Client {
	t.Helper()
	url := fmt.Sprintf("amqp://%s:%s@%s/", internal.RabbitMQUser, internal.RabbitMQPass, internal.RabbitMQAddr)
	e, err := harego.NewClient(url,
		harego.ExchangeName(exchange),
		harego.QueueName(queueName),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		rabbit, err := amqp.Dial(url)
		require.NoErrorf(t, err, "connecting to RabbitMQ: %s", url)
		ch, err := rabbit.Channel()
		require.NoError(t, err)
		err = ch.ExchangeDelete(exchange, false, false)
		assert.NoError(t, err)
		_, err = ch.QueueDelete(queueName, false, false, true)
		assert.NoError(t, err)
		assert.NoError(t, rabbit.Close())
		e.Close()
	})
	return e
}

// getContainer returns a new container running rabbimq that is ready for
// accepting connections.
func getContainer(t *testing.T) (container testcontainers.Container, addr string) {
	t.Helper()
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "rabbitmq:3.8-management-alpine",
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
	_, err := container.Exec(ctx, []string{
		"rabbitmqctl",
		"stop_app",
	})
	require.NoError(t, err)

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_, err = container.Exec(ctx, []string{
			"rabbitmqctl",
			"start_app",
		})
		require.NoError(t, err)
	}()
}
