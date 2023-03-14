package harego_test

import (
	"context"
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
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/blokur/harego/v2"
	"github.com/blokur/harego/v2/internal"
)

func init() {
	viper.AutomaticEnv()

	internal.RabbitMQAddr = viper.GetString(internal.RabbitMQAddrName)
	internal.RabbitMQUser = viper.GetString(internal.RabbitMQUserName)
	internal.RabbitMQPass = viper.GetString(internal.RabbitMQPassName)
	internal.RabbitMQVirtual = viper.GetString(internal.RabbitMQVirtualName)
}

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
	apiAddress := strings.Split(internal.RabbitMQAddr, ":")[0]
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
		req.SetBasicAuth(internal.RabbitMQUser, internal.RabbitMQPass)
		resp, err := http.DefaultClient.Do(req)
		if resp != nil && resp.Body != nil {
			defer func() {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}()
		}
		require.NoError(t, err)
	}

	url := fmt.Sprintf("amqp://%s:%s@%s/%s", internal.RabbitMQUser, internal.RabbitMQPass, internal.RabbitMQAddr, vh)
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
				req.SetBasicAuth(internal.RabbitMQUser, internal.RabbitMQPass)
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
