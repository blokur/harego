//go:build integration

package harego_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arsham/retry/v2"
	"github.com/blokur/harego/v2"
	"github.com/blokur/testament"
	"github.com/google/go-cmp/cmp"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegPublisher(t *testing.T) {
	t.Parallel()
	t.Run("Publish", testIntegPublisherPublish)
	t.Run("PublishWorkers", testIntegPublisherPublishWorkers)
	t.Run("Reconnect", testIntegPublisherReconnect)
	t.Run("Close", testIntegPublisherClose)
}

func testIntegPublisherPublish(t *testing.T) {
	t.Parallel()
	tcs := []struct {
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
	for _, tc := range tcs {
		tc := tc
		name := fmt.Sprintf("%dWorkers/%dMessages/", tc.workers, tc.total)
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			testIntegPublisherPublishConcurrent(t, tc.total, tc.workers)
		})
	}
}

func testIntegPublisherPublishConcurrent(t *testing.T, total, workers int) {
	t.Helper()
	exchange := "test." + testament.RandomString(20)
	queueName := "test." + testament.RandomString(20)
	routingKey := "test." + testament.RandomString(20)
	vh := "test." + testament.RandomString(20)

	_, pub := getConsumerPublisher(t, vh, exchange, queueName,
		harego.Workers(workers),
		harego.RoutingKey(routingKey),
	)
	var wg sync.WaitGroup
	wg.Add(total)
	for i := 0; i < total; i++ {
		go func() {
			defer wg.Done()
			err := pub.Publish(&amqp.Publishing{
				Body: []byte(randomBody(1)),
			})
			assert.NoError(t, err)
		}()
	}
	wg.Wait()
}

func testIntegPublisherPublishWorkers(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	vh := "test." + testament.RandomString(20)
	exchange1 := "test." + testament.RandomString(20)
	exchange2 := "test." + testament.RandomString(20)
	queueName := "test." + testament.RandomString(20)

	_, pub := getConsumerPublisher(t, vh, exchange1, queueName,
		harego.Workers(10),
	)
	cons, _ := getConsumerPublisher(t, vh, exchange2, queueName)

	var (
		want []string
		mu   sync.RWMutex
		got  []string
	)
	total := 1000
	for i := 0; i < total; i++ {
		msg := fmt.Sprintf("Message: [i:%d]", i)
		want = append(want, msg)
		err := pub.Publish(&amqp.Publishing{
			Body: []byte(msg),
		})
		require.NoError(t, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		err := cons.Consume(ctx, func(msg *amqp.Delivery) (harego.AckType, time.Duration) {
			mu.Lock()
			defer mu.Unlock()
			got = append(got, string(msg.Body))
			return harego.AckTypeAck, 0
		})
		assert.ErrorIs(t, err, context.Canceled)
	}()
	assert.Eventually(t, func() bool {
		mu.RLock()
		defer mu.RUnlock()
		if len(want) == len(got) {
			cancel()
			return true
		}
		return false
	}, time.Minute, 10*time.Millisecond)
	mu.RLock()
	defer mu.RUnlock()
	if diff := cmp.Diff(want, got, testament.StringSliceComparer); diff != "" {
		t.Errorf("(-want +got):\\n%s", diff)
	}
}

func testIntegPublisherReconnect(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	var (
		total     = 200
		wg        sync.WaitGroup
		exchange  = "test." + testament.RandomString(20)
		queueName = "test." + testament.RandomString(20)
		pub       *harego.Publisher
		cons      *harego.Consumer
		r         = &retry.Retry{
			Delay:    time.Second,
			Attempts: 30,
		}
	)

	container, addr := getContainer(t)
	err := r.Do(func() error {
		var err error
		pub, err = harego.NewPublisher(harego.URLConnector(addr),
			harego.ExchangeName(exchange),
			harego.RetryDelay(500*time.Millisecond),
		)
		return err
	})
	require.NoError(t, err)

	err = r.Do(func() error {
		var err error
		cons, err = harego.NewConsumer(harego.URLConnector(addr),
			harego.ExchangeName(exchange),
			harego.QueueName(queueName),
			harego.RetryDelay(500*time.Millisecond),
		)
		return err
	})
	require.NoError(t, err)
	defer cons.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// we want the goroutine to wait for us before publishing.
	restart := make(chan chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case ch := <-restart:
				restartRabbitMQ(t, container)
				close(ch)
			case <-ctx.Done():
				return
			}
		}
	}()
	wg.Add(1)
	var calls int32
	go func() {
		defer wg.Done()
		assert.NotPanics(t, func() {
			i := 0
			err := cons.Consume(ctx, func(*amqp.Delivery) (harego.AckType, time.Duration) {
				i++
				g := atomic.AddInt32(&calls, 1)
				if g >= int32(total) {
					cancel()
				}
				return harego.AckTypeAck, 0
			})
			assert.ErrorIs(t, err, context.Canceled)
		})
	}()

	restarted := false

	for i := 0; i < total; i++ {
		if restarted {
			time.Sleep(time.Second / 5)
			pub, err = harego.NewPublisher(harego.URLConnector(addr),
				harego.ExchangeName(exchange),
			)
			if err == nil {
				restarted = false
			}
			i--
			continue
		}
		err := pub.Publish(&amqp.Publishing{
			Body: []byte(testament.RandomString(10)),
		})
		if err != nil {
			i--
		}
		if i > 0 && i%(total/5) == 0 {
			restarted = true
			msg := make(chan struct{})
			restart <- msg
			<-msg
		}
	}

	assert.Eventually(t, func() bool {
		wg.Wait()
		return true
	}, 2*time.Minute, 50*time.Millisecond)

	assert.EqualValues(t, total, atomic.LoadInt32(&calls))
}

func testIntegPublisherClose(t *testing.T) {
	t.Parallel()
	vh := "test." + testament.RandomString(20)
	exchange := "test." + testament.RandomString(20)
	queueName := "test." + testament.RandomString(20)

	total := 10
	_, pub := getConsumerPublisher(t, vh, exchange, queueName, harego.Workers(total))
	defer pub.Close()

	var wg sync.WaitGroup
	for i := 0; i < total-1; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := pub.Publish(&amqp.Publishing{
				Body: []byte(testament.RandomString(20)),
			})
			if err == nil {
				return
			}
			assert.True(t, errors.Is(err, harego.ErrClosed) || errors.Is(err, context.Canceled))
		}()
	}

	wg.Add(total / 2)
	for i := 0; i < total/2; i++ {
		go func() {
			defer wg.Done()
			err := pub.Close()
			if err == nil {
				return
			}
			assert.True(t, errors.Is(err, harego.ErrClosed) || errors.Is(err, context.Canceled))
		}()
	}

	assert.Eventually(t, func() bool {
		wg.Wait()
		return true
	}, 2*time.Minute, 30*time.Millisecond)
}
