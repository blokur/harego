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

func TestIntegConsumer(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip()
	}

	t.Run("Consume", testIntegConsumerConsume)
	t.Run("SeparatedConsumePublish", testIntegConsumerSeparatedConsumePublish)
	t.Run("UseSameQueue", testIntegConsumerUseSameQueue)
	t.Run("PublishWorkers", testIntegConsumerPublishWorkers)
	t.Run("Reconnect", testIntegConsumerReconnect)
	t.Run("Close", testIntegConsumerClose)
}

func testIntegConsumerConsume(t *testing.T) {
	t.Parallel()
	t.Run("Concurrent", testIntegConsumerConsumeConcurrent)
	t.Run("Nack", testIntegConsumerConsumeNack)
	t.Run("Reject", testIntegConsumerConsumeReject)
	t.Run("Requeue", testIntegConsumerConsumeRequeue)
}

func testIntegConsumerConsumeConcurrent(t *testing.T) {
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
		{20, 100},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%dWorkers/%dMessages/", tc.workers, tc.total), func(t *testing.T) {
			t.Parallel()
			testIntegConsumerConsumeConcurrentDo(t, tc.total, tc.workers)
		})
	}
}

func testIntegConsumerConsumeConcurrentDo(t *testing.T, total, workers int) {
	t.Helper()

	exchange := "test." + testament.RandomString(20)
	queueName := "test." + testament.RandomString(20)
	routingKey := "test." + testament.RandomString(20)

	cons, pub := getConsumerPublisher(t, exchange, queueName,
		harego.Workers(workers),
		harego.RoutingKey(routingKey),
	)

	var (
		muWant sync.RWMutex
		want   []string
		muGot  sync.RWMutex
		got    []string
		wGroup sync.WaitGroup
	)

	wGroup.Add(total)

	for i := range total {
		go func() {
			defer wGroup.Done()

			msg := fmt.Sprintf("Message %d", i)
			message := &amqp.Publishing{
				Body: []byte(msg),
			}
			err := pub.Publish(message)
			require.NoError(t, err)

			muWant.Lock()
			defer muWant.Unlock()

			want = append(want, msg)
		}()
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	go func() {
		err := cons.Consume(ctx, func(msg *amqp.Delivery) (harego.AckType, time.Duration) {
			muGot.Lock()
			defer muGot.Unlock()

			got = append(got, string(msg.Body))

			return harego.AckTypeAck, 0
		})
		assert.ErrorIs(t, err, context.Canceled)
	}()

	assert.Eventually(t, func() bool {
		wGroup.Wait()

		muGot.RLock()
		defer muGot.RUnlock()

		if len(got) == total {
			cancel()
			return true
		}

		return false
	}, 5*time.Minute, 30*time.Millisecond)

	muGot.RLock()
	defer muGot.RUnlock()

	muWant.RLock()
	defer muWant.RUnlock()

	if diff := cmp.Diff(want, got, testament.StringSliceComparer); diff != "" {
		t.Errorf("(-want +got):\\n%s", diff)
	}
}

func testIntegConsumerConsumeNack(t *testing.T) {
	t.Parallel()

	var (
		exchange     = "test." + testament.RandomString(20)
		queueName    = "test." + testament.RandomString(20)
		addr         string
		cons1, cons2 *harego.Consumer
		pub          *harego.Publisher
		err          error
	)

	err = retryConfig.Do(func() error {
		_, addr = getContainer(t)
		return nil
	}, func() error {
		cons1, err = harego.NewConsumer(harego.URLConnector(addr),
			harego.ExchangeName(exchange),
			harego.QueueName(queueName),
			harego.ConsumerName("cons1"),
			harego.Workers(2),
		)

		return err
	}, func() error {
		cons2, err = harego.NewConsumer(harego.URLConnector(addr),
			harego.ExchangeName(exchange),
			harego.QueueName(queueName),
			harego.ConsumerName("cons2"),
			harego.Workers(2),
		)

		return err
	}, func() error {
		pub, err = harego.NewPublisher(harego.URLConnector(addr),
			harego.ExchangeName(exchange),
			harego.QueueName(queueName),
			harego.Workers(2),
		)

		return err
	})
	require.NoError(t, err)

	original := randomBody(1)
	err = pub.Publish(&amqp.Publishing{
		Body: []byte(original),
	})
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		err = cons1.Consume(ctx, func(*amqp.Delivery) (harego.AckType, time.Duration) {
			select {
			case <-ctx.Done():
				t.Error("cons1 already done")
				return harego.AckTypeAck, 0
			default:
			}

			cancel()

			return harego.AckTypeNack, 0
		})
		assert.ErrorIs(t, err, context.Canceled)

		return true
	}, time.Minute, 10*time.Millisecond)
	cons1.Close()

	assert.Eventually(t, func() bool {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		err := cons2.Consume(ctx, func(msg *amqp.Delivery) (harego.AckType, time.Duration) {
			select {
			case <-ctx.Done():
				t.Error("cons2 already done")
				return harego.AckTypeAck, 0
			default:
			}

			assert.Equal(t, original, string(msg.Body))
			cancel()

			return harego.AckTypeAck, 0
		})
		assert.ErrorIs(t, err, context.Canceled)

		return true
	}, time.Minute, 10*time.Millisecond)
}

func testIntegConsumerConsumeReject(t *testing.T) {
	t.Parallel()

	exchange := "test." + testament.RandomString(20)
	queueName := "test." + testament.RandomString(20)

	cons1, pub := getConsumerPublisher(t, exchange, queueName)
	original := randomBody(1)
	err := pub.Publish(&amqp.Publishing{
		Body: []byte(original),
	})
	require.NoError(t, err)

	// I am measuring the time it takes to read on this machine.
	started := time.Now()
	assert.Eventually(t, func() bool {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		err = cons1.Consume(ctx, func(*amqp.Delivery) (harego.AckType, time.Duration) {
			cancel()
			return harego.AckTypeReject, 0
		})
		assert.ErrorIs(t, err, context.Canceled)

		return true
	}, time.Minute, 10*time.Millisecond)

	duration := time.Since(started)

	cons1.Close()

	cons2, _ := getConsumerPublisher(t, exchange, queueName)
	assert.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(t.Context(), duration+2*time.Second)
		defer cancel()

		cons2.Consume(ctx, func(msg *amqp.Delivery) (harego.AckType, time.Duration) {
			t.Errorf("didn't expect to receive %q", string(msg.Body))
			return harego.AckTypeReject, 0
		})

		return true
	}, duration+10*time.Second, 10*time.Millisecond)
}

func testIntegConsumerConsumeRequeue(t *testing.T) {
	t.Parallel()

	exchange := "test." + testament.RandomString(20)
	queueName := "test." + testament.RandomString(20)
	cons, pub := getConsumerPublisher(t, exchange, queueName)

	message := func(i int) string { return fmt.Sprintf("message #%d", i) }
	total := 100
	gotMsgs := make([]string, 0, total)
	wantMsgs := make([]string, 0, total)

	mid := total / 2

	for messageID := range total {
		msg := message(messageID)
		err := pub.Publish(&amqp.Publishing{
			Body: []byte(msg),
		})
		require.NoError(t, err)

		if messageID == mid {
			continue
		}

		wantMsgs = append(wantMsgs, msg)
	}

	wantMsg := message(mid)
	wantMsgs = append(wantMsgs, wantMsg)

	assert.Eventually(t, func() bool {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		counter := 0
		err := cons.Consume(ctx, func(msg *amqp.Delivery) (harego.AckType, time.Duration) {
			counter++
			if string(msg.Body) == wantMsg && counter < total-1 {
				return harego.AckTypeRequeue, 0
			}

			gotMsgs = append(gotMsgs, string(msg.Body))

			if counter > total {
				cancel()
			}

			return harego.AckTypeAck, 0
		})
		assert.ErrorIs(t, err, context.Canceled)

		return true
	}, time.Minute, 10*time.Millisecond)

	if diff := cmp.Diff(wantMsgs, gotMsgs); diff != "" {
		t.Errorf("(-want +got):\\n%s", diff)
	}
}

//nolint:funlen // This is an integration test.
func testIntegConsumerSeparatedConsumePublish(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("slow test")
	}

	exchange1 := "test." + testament.RandomString(20)
	exchange2 := "test." + testament.RandomString(21)
	queueName1 := "test." + testament.RandomString(20)
	queueName2 := "test." + testament.RandomString(21)

	var (
		pub1, pub2   *harego.Publisher
		cons1, cons2 *harego.Consumer
		addr         string
	)

	var err error

	err = retryConfig.Do(func() error {
		_, addr = getContainer(t)
		return nil
	}, func() error {
		_, pub1, err = getConsumerPublisherWithAddr(t, addr, exchange1, "")
		return err
	}, func() error {
		_, pub2, err = getConsumerPublisherWithAddr(t, addr, exchange2, "")
		return err
	}, func() error {
		cons1, _, err = getConsumerPublisherWithAddr(t, addr, exchange1, queueName1)
		return err
	}, func() error {
		cons2, _, err = getConsumerPublisherWithAddr(t, addr, exchange2, queueName2)
		return err
	})
	require.NoError(t, err)

	var mutex1, mutex2 sync.RWMutex

	total := 1000
	want1 := make([]string, 0, total)
	got1 := make([]string, 0, total)
	want2 := make([]string, 0, total)
	got2 := make([]string, 0, total)

	for queueID := range total {
		msg := fmt.Sprintf("Queue 1: [queueID:%d]", queueID)
		want1 = append(want1, msg)
		err := pub1.Publish(&amqp.Publishing{
			Body: []byte(msg),
		})
		require.NoError(t, err)

		msg = fmt.Sprintf("Queue 2: [queueID:%d]", queueID)
		want2 = append(want2, msg)
		err = pub2.Publish(&amqp.Publishing{
			Body: []byte(msg),
		})
		require.NoError(t, err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	go func() {
		err := cons1.Consume(ctx, func(msg *amqp.Delivery) (harego.AckType, time.Duration) {
			mutex1.Lock()
			defer mutex1.Unlock()

			got1 = append(got1, string(msg.Body))

			return harego.AckTypeAck, 0
		})
		assert.ErrorIs(t, err, context.Canceled)
	}()
	go func() {
		err := cons2.Consume(ctx, func(msg *amqp.Delivery) (harego.AckType, time.Duration) {
			mutex2.Lock()
			defer mutex2.Unlock()

			got2 = append(got2, string(msg.Body))

			return harego.AckTypeAck, 0
		})
		assert.ErrorIs(t, err, context.Canceled)
	}()

	assert.Eventually(t, func() bool {
		mutex1.RLock()
		defer mutex1.RUnlock()

		mutex2.RLock()
		defer mutex2.RUnlock()

		if len(want1) == len(got1) && len(want2) == len(got2) {
			cancel()
			return true
		}

		return false
	}, time.Minute, 100*time.Millisecond)
	cancel()

	if diff := cmp.Diff(want1, got1, testament.StringSliceComparer); diff != "" {
		t.Errorf("(-want +got):\\n%s", diff)
	}

	if diff := cmp.Diff(want2, got2, testament.StringSliceComparer); diff != "" {
		t.Errorf("(-want +got):\\n%s", diff)
	}
}

func testIntegConsumerUseSameQueue(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("slow test")
	}

	var (
		exchange1  = "test." + testament.RandomString(20)
		exchange2  = "test." + testament.RandomString(20)
		queueName  = "test." + testament.RandomString(20)
		addr       string
		pub1, pub2 *harego.Publisher
		cons       *harego.Consumer
		err        error
	)

	err = retryConfig.Do(func() error {
		_, addr = getContainer(t)
		return nil
	}, func() error {
		_, pub1, err = getConsumerPublisherWithAddr(t, addr, exchange1, queueName)
		return err
	}, func() error {
		cons, pub2, err = getConsumerPublisherWithAddr(t, addr, exchange2, queueName)
		return err
	})
	require.NoError(t, err)

	var mutex sync.RWMutex

	total := 1000
	want := make([]string, 0, total)
	got := make([]string, 0, total)

	for pubID := range total {
		msg := fmt.Sprintf("Publisher 1: [pubID:%d]", pubID)
		want = append(want, msg)
		err := pub1.Publish(&amqp.Publishing{
			Body: []byte(msg),
		})
		require.NoError(t, err)

		msg = fmt.Sprintf("Publisher 2: [pubID:%d]", pubID)
		want = append(want, msg)
		err = pub2.Publish(&amqp.Publishing{
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

//nolint:dupl // This is not a duplicate of the PUBLISHER.
func testIntegConsumerPublishWorkers(t *testing.T) {
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

func testIntegConsumerReconnect(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("slow test")
	}

	var (
		total     = 200
		wGroup    sync.WaitGroup
		exchange  = "test." + testament.RandomString(20)
		queueName = "test." + testament.RandomString(20)
		pub       *harego.Publisher
		cons      *harego.Consumer
		err       error
		addr      string
		container testcontainers.Container
	)

	err = retryConfig.Do(func() error {
		container, addr = getContainer(t)
		return nil
	}, func() error {
		pub, err = harego.NewPublisher(harego.URLConnector(addr),
			harego.ExchangeName(exchange),
			harego.RetryDelay(500*time.Millisecond),
		)

		return err
	}, func() error {
		cons, err = harego.NewConsumer(harego.URLConnector(addr),
			harego.ExchangeName(exchange),
			harego.QueueName(queueName),
			harego.RetryDelay(500*time.Millisecond),
		)

		return err
	})
	require.NoError(t, err)

	defer pub.Close()
	defer cons.Close()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	for range total {
		err := pub.Publish(&amqp.Publishing{
			Body: []byte(testament.RandomString(10)),
		})
		require.NoError(t, err)
	}

	restart := make(chan struct{})

	go func() {
		<-restart
		restartRabbitMQ(t, container)
	}()

	wGroup.Add(1)

	var calls int32

	go func() {
		defer wGroup.Done()

		assert.NotPanics(t, func() {
			err := cons.Consume(ctx, func(*amqp.Delivery) (harego.AckType, time.Duration) {
				numCalls := int(atomic.AddInt32(&calls, 1))
				if numCalls == total/5 {
					close(restart)
					time.Sleep(time.Second)
				}

				if numCalls >= total {
					cancel()
				}

				return harego.AckTypeAck, 0
			})
			assert.ErrorIs(t, err, context.Canceled)
		})
	}()

	assert.Eventually(t, func() bool {
		wGroup.Wait()
		return true
	}, 2*time.Minute, 50*time.Millisecond)

	assert.EqualValues(t, total, atomic.LoadInt32(&calls))
}

func testIntegConsumerClose(t *testing.T) {
	t.Parallel()

	exchange := "test." + testament.RandomString(20)
	queueName := "test." + testament.RandomString(20)

	total := 10

	_, broker := getConsumerPublisher(t, exchange, queueName, harego.Workers(total))
	defer broker.Close()

	var wGroup sync.WaitGroup
	for range total - 1 {
		wGroup.Add(1)

		go func() {
			defer wGroup.Done()

			err := broker.Publish(&amqp.Publishing{
				Body: []byte(testament.RandomString(20)),
			})
			if err == nil {
				return
			}

			assert.True(t, errors.Is(err, harego.ErrClosed) || errors.Is(err, context.Canceled), err)
		}()
	}

	wGroup.Add(total / 2)

	for range total / 2 {
		go func() {
			defer wGroup.Done()

			err := broker.Close()
			if err == nil {
				return
			}

			assert.True(t, errors.Is(err, harego.ErrClosed) || errors.Is(err, context.Canceled), err)
		}()
	}

	assert.Eventually(t, func() bool {
		wGroup.Wait()
		return true
	}, 2*time.Minute, 30*time.Millisecond)
}
