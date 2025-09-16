package harego_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"

	"github.com/blokur/testament"

	"github.com/blokur/harego/v2"
)

func TestIntegPublisher(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip()
	}

	t.Run("Publish", testIntegPublisherPublish)
	t.Run("PublishWorkers", testIntegPublisherPublishWorkers)
	t.Run("Reconnect", testIntegPublisherReconnect)
	t.Run("Close", testIntegPublisherClose)
}

func testIntegPublisherPublish(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		total   int
		workers int
	}{
		{1, 1},
		{10, 1},
		{100, 1},
		{1, 10},
		{10, 10},
		{100, 10},
		{1, 100},
		{10, 100},
		{20, 1000},
	}

	for _, testCase := range testCases {
		name := fmt.Sprintf("%dWorkers/%dMessages/", testCase.workers, testCase.total)
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			testIntegPublisherPublishConcurrent(t, testCase.total, testCase.workers)
		})
	}
}

func testIntegPublisherPublishConcurrent(t *testing.T, total, workers int) {
	t.Helper()

	exchange := "test." + testament.RandomString(20)
	queueName := "test." + testament.RandomString(20)
	routingKey := "test." + testament.RandomString(20)

	_, pub := getConsumerPublisher(t, exchange, queueName,
		harego.Workers(workers),
		harego.RoutingKey(routingKey),
	)

	var wGroup sync.WaitGroup

	wGroup.Add(total)

	for range total {
		go func() {
			defer wGroup.Done()

			err := pub.Publish(&amqp.Publishing{
				Body: []byte(randomBody(1)),
			})
			assert.NoError(t, err)
		}()
	}

	wGroup.Wait()
}

//nolint:dupl // This is not a duplicate of the CONSUMER.
func testIntegPublisherPublishWorkers(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("slow test")
	}

	var (
		exchange1 = "test." + testament.RandomString(20)
		exchange2 = "test." + testament.RandomString(20)
		queueName = "test." + testament.RandomString(20)
		err       error
		addr      string
		pub       *harego.Publisher
		cons      *harego.Consumer
	)

	err = retryConfig.Do(func() error {
		_, addr = getContainer(t)
		return nil
	}, func() error {
		_, pub, err = getConsumerPublisherWithAddr(t, addr, exchange1, queueName,
			harego.Workers(10),
		)

		return err
	}, func() error {
		cons, _, err = getConsumerPublisherWithAddr(t, addr, exchange2, queueName)
		return err
	})
	require.NoError(t, err)

	var mutex sync.RWMutex

	total := 1000
	want := make([]string, 0, total)
	got := make([]string, 0, total)

	for i := range total {
		msg := fmt.Sprintf("Message: [i:%d]", i)
		want = append(want, msg)
		err := pub.Publish(&amqp.Publishing{
			Body: []byte(msg),
		})
		require.NoError(t, err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	go func() {
		err := cons.Consume(ctx, func(msg *amqp.Delivery) (harego.AckType, time.Duration) {
			mutex.Lock()
			defer mutex.Unlock()

			got = append(got, string(msg.Body))

			return harego.AckTypeAck, 0
		})
		assert.ErrorIs(t, err, context.Canceled)
	}()

	assert.Eventually(t, func() bool {
		mutex.RLock()
		defer mutex.RUnlock()

		if len(want) == len(got) {
			cancel()
			return true
		}

		return false
	}, time.Minute, 10*time.Millisecond)

	mutex.RLock()
	defer mutex.RUnlock()

	if diff := cmp.Diff(want, got, testament.StringSliceComparer); diff != "" {
		t.Errorf("(-want +got):\\n%s", diff)
	}
}

//nolint:funlen // This is an integrateion test.
func testIntegPublisherReconnect(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("slow test")
	}

	var (
		total     = 200
		wGroup    sync.WaitGroup
		exchange  = "test." + testament.RandomString(20)
		queueName = "test." + testament.RandomString(20)
		container testcontainers.Container
		addr      string
		cons      *harego.Consumer
		pub       *harego.Publisher
		err       error
	)

	err = retryConfig.Do(func() error {
		container, addr = getContainer(t)
		return nil
	}, func() error {
		cons, pub, err = getConsumerPublisherWithAddr(t, addr, exchange, queueName, harego.RetryDelay(time.Second/2))
		return err
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// we want the goroutine to wait for us before publishing.
	restart := make(chan chan struct{})

	wGroup.Add(1)

	go func() {
		defer wGroup.Done()

		for {
			select {
			case ch := <-restart:
				//nolint:contextcheck // See func documentation
				restartRabbitMQ(t, container)
				close(ch)
			case <-ctx.Done():
				return
			}
		}
	}()

	wGroup.Add(1)

	var calls int32

	go func() {
		defer wGroup.Done()

		assert.NotPanics(t, func() {
			i := 0
			err := cons.Consume(ctx, func(*amqp.Delivery) (harego.AckType, time.Duration) {
				i++

				g := int(atomic.AddInt32(&calls, 1))
				if g >= total {
					cancel()
				}

				return harego.AckTypeAck, 0
			})
			assert.ErrorIs(t, err, context.Canceled)
		})
	}()

	var restarted bool

	for pubID := 0; pubID < total; pubID++ {
		if restarted {
			time.Sleep(time.Second / 5)

			pub, err = harego.NewPublisher(harego.URLConnector(addr),
				harego.ExchangeName(exchange),
			)
			if err == nil {
				restarted = false
			}

			pubID--

			continue
		}

		err = pub.Publish(&amqp.Publishing{
			Body: []byte(testament.RandomString(10)),
		})
		if err != nil {
			pubID--
		}

		if pubID > 0 && pubID%(total/5) == 0 {
			restarted = true

			msg := make(chan struct{})
			restart <- msg

			<-msg
		}
	}

	assert.Eventually(t, func() bool {
		wGroup.Wait()
		return true
	}, 2*time.Minute, 50*time.Millisecond)

	assert.EqualValues(t, total, atomic.LoadInt32(&calls))
}

func testIntegPublisherClose(t *testing.T) {
	t.Parallel()

	exchange := "test." + testament.RandomString(20)
	queueName := "test." + testament.RandomString(20)

	total := 10

	_, pub := getConsumerPublisher(t, exchange, queueName, harego.Workers(total))
	defer pub.Close()

	var wGroup sync.WaitGroup
	for range total - 1 {
		wGroup.Add(1)

		go func() {
			defer wGroup.Done()

			err := pub.Publish(&amqp.Publishing{
				Body: []byte(testament.RandomString(20)),
			})
			if err == nil {
				return
			}

			assert.True(t, errors.Is(err, harego.ErrClosed) || errors.Is(err, context.Canceled))
		}()
	}

	wGroup.Add(total / 2)

	for range total / 2 {
		go func() {
			defer wGroup.Done()

			err := pub.Close()
			if err == nil {
				return
			}

			assert.True(t, errors.Is(err, harego.ErrClosed) || errors.Is(err, context.Canceled))
		}()
	}

	assert.Eventually(t, func() bool {
		wGroup.Wait()
		return true
	}, 2*time.Minute, 30*time.Millisecond)
}
