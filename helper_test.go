package harego_test

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/blokur/harego"
	"github.com/blokur/harego/internal"
	"github.com/spf13/viper"
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

// getClient creates a client without a queue if the queueName is empty.
func getClient(t *testing.T, queueName string, conf ...harego.ConfigFunc) harego.Client {
	t.Helper()
	exchange := "test." + randomString(20)
	vh := "test." + randomString(20)
	return getNamedClient(t, vh, exchange, queueName, conf...)
}

// getNamedClient creates a client without a queue if the queueName is empty.
func getNamedClient(t *testing.T, vh, exchange, queueName string, conf ...harego.ConfigFunc) harego.Client {
	t.Helper()
	var (
		adminURL string
		err      error
	)
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
		req, err := http.NewRequest("PUT", adminURL, nil)
		require.NoError(t, err)
		req.SetBasicAuth(internal.RabbitMQUser, internal.RabbitMQPass)
		resp, err := http.DefaultClient.Do(req)
		if resp != nil && resp.Body != nil {
			defer func() {
				io.Copy(ioutil.Discard, resp.Body)
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
	e, err := harego.NewClient(url,
		conf...,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.Eventually(t, func() bool {
			e.Close()
			return true
		}, 2*time.Second, 10*time.Millisecond)
		urls := []string{
			fmt.Sprintf("http://%s:%d/api/exchanges/%s", apiAddress, adminPort, exchange),
		}
		if queueName != "" {
			urls = append(urls,
				fmt.Sprintf("http://%s:%d/api/queues/%s", apiAddress, adminPort, queueName),
			)
		}

		if vh != "" {
			urls = append(urls, adminURL)
		}

		for _, url := range urls {
			func() {
				req, err := http.NewRequest("DELETE", url, nil)
				require.NoError(t, err)
				req.SetBasicAuth(internal.RabbitMQUser, internal.RabbitMQPass)
				resp, err := http.DefaultClient.Do(req)
				if resp != nil && resp.Body != nil {
					defer func() {
						io.Copy(ioutil.Discard, resp.Body)
						resp.Body.Close()
					}()
				}
				require.NoError(t, err)
			}()
		}
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
		container.Exec(ctx, []string{
			"rabbitmqctl",
			"start_app",
		})
	}()
}
