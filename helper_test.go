package harego_test

import (
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
	rabbit, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/", internal.RabbitMQUser, internal.RabbitMQPass, internal.RabbitMQAddr))
	require.NoErrorf(t, err, "connecting to RabbitMQ: username: %q, password: %q, location: %s", internal.RabbitMQUser, internal.RabbitMQPass, internal.RabbitMQAddr)
	e, err := harego.NewClient(rabbit,
		harego.ExchangeName(exchange),
		harego.QueueName(queueName),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		rabbit, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/", internal.RabbitMQUser, internal.RabbitMQPass, internal.RabbitMQAddr))
		require.NoErrorf(t, err, "connecting to RabbitMQ: username: %q, password: %q, location: %s", internal.RabbitMQUser, internal.RabbitMQPass, internal.RabbitMQAddr)
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
