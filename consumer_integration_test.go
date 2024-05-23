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

func TestIntegConsumer(t *testing.T) {
	t.Parallel()
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
		{20, 100},
	}
	for _, tc := range tcs {
		tc := tc
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
	vh := "test." + testament.RandomString(20)

	cons, pub := getConsumerPublisher(t, vh, exchange, queueName,
		harego.Workers(workers),
		harego.RoutingKey(routingKey),
	)

	var (
		muWant sync.RWMutex
		want   []string
		muGot  sync.RWMutex
		got    []string
		wg     sync.WaitGroup
	)
	wg.Add(total)
	for i := 0; i < total; i++ {
		i := i
		go func() {
			defer wg.Done()
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
	ctx, cancel := context.WithCancel(context.Background())
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
		wg.Wait()
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
	exchange := "test." + testament.RandomString(20)
	queueName := "test." + testament.RandomString(20)
	vh := "test." + testament.RandomString(20)

	cons1, pub := getConsumerPublisher(t, vh, exchange, queueName,
		harego.ConsumerName("cons1"),
		harego.Workers(2),
	)
	cons2, _ := getConsumerPublisher(t, vh, exchange, queueName,
		harego.ConsumerName("cons2"),
		harego.Workers(2),
	)

	original := randomBody(1)
	err := pub.Publish(&amqp.Publishing{
		Body: []byte(original),
	})
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		ctx, cancel := context.WithCancel(context.Background())
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
		ctx, cancel := context.WithCancel(context.Background())
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
	vh := "test." + testament.RandomString(20)

	cons1, pub := getConsumerPublisher(t, vh, exchange, queueName)
	original := randomBody(1)
	err := pub.Publish(&amqp.Publishing{
		Body: []byte(original),
	})
	require.NoError(t, err)

	// I am measuring the time it takes to read on this machine.
	started := time.Now()
	assert.Eventually(t, func() bool {
		ctx, cancel := context.WithCancel(context.Background())
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

	cons2, _ := getConsumerPublisher(t, vh, exchange, queueName)
	assert.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), duration+2*time.Second)
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
	vh := "test." + testament.RandomString(20)
	cons, pub := getConsumerPublisher(t, vh, exchange, queueName)

	message := func(i int) string { return fmt.Sprintf("message #%d", i) }
	total := 100
	gotMsgs := make([]string, 0, total)
	wantMsgs := make([]string, 0, total)
	mid := total / 2
	for i := 0; i < total; i++ {
		msg := message(i)
		err := pub.Publish(&amqp.Publishing{
			Body: []byte(msg),
		})
		require.NoError(t, err)
		if i == mid {
			continue
		}
		wantMsgs = append(wantMsgs, msg)
	}
	wantMsg := message(mid)
	wantMsgs = append(wantMsgs, wantMsg)

	assert.Eventually(t, func() bool {
		ctx, cancel := context.WithCancel(context.Background())
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

func testIntegConsumerSeparatedConsumePublish(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	vh := "test." + testament.RandomString(20)
	exchange1 := "test." + testament.RandomString(20)
	exchange2 := "test." + testament.RandomString(21)
	queueName1 := "test." + testament.RandomString(20)
	queueName2 := "test." + testament.RandomString(21)

	_, pub1 := getConsumerPublisher(t, vh, exchange1, "")
	_, pub2 := getConsumerPublisher(t, vh, exchange2, "")
	cons1, _ := getConsumerPublisher(t, vh, exchange1, queueName1)
	cons2, _ := getConsumerPublisher(t, vh, exchange2, queueName2)

	var want1, want2 []string
	var (
		mu1  sync.RWMutex
		got1 []string
		mu2  sync.RWMutex
		got2 []string
	)
	total := 1000
	for i := 0; i < total; i++ {
		msg := fmt.Sprintf("Queue 1: [i:%d]", i)
		want1 = append(want1, msg)
		err := pub1.Publish(&amqp.Publishing{
			Body: []byte(msg),
		})
		require.NoError(t, err)

		msg = fmt.Sprintf("Queue 2: [i:%d]", i)
		want2 = append(want2, msg)
		err = pub2.Publish(&amqp.Publishing{
			Body: []byte(msg),
		})
		require.NoError(t, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		err := cons1.Consume(ctx, func(msg *amqp.Delivery) (harego.AckType, time.Duration) {
			mu1.Lock()
			defer mu1.Unlock()
			got1 = append(got1, string(msg.Body))
			return harego.AckTypeAck, 0
		})
		assert.ErrorIs(t, err, context.Canceled)
	}()
	go func() {
		err := cons2.Consume(ctx, func(msg *amqp.Delivery) (harego.AckType, time.Duration) {
			mu2.Lock()
			defer mu2.Unlock()
			got2 = append(got2, string(msg.Body))
			return harego.AckTypeAck, 0
		})
		assert.ErrorIs(t, err, context.Canceled)
	}()

	assert.Eventually(t, func() bool {
		mu1.RLock()
		defer mu1.RUnlock()
		mu2.RLock()
		defer mu2.RUnlock()
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
	if testing.Short() {
		t.Skip("slow test")
	}
	vh := "test." + testament.RandomString(20)
	exchange1 := "test." + testament.RandomString(20)
	exchange2 := "test." + testament.RandomString(20)
	queueName := "test." + testament.RandomString(20)

	_, pub1 := getConsumerPublisher(t, vh, exchange1, queueName)
	cons, pub2 := getConsumerPublisher(t, vh, exchange2, queueName)

	var (
		want []string
		mu   sync.RWMutex
		got  []string
	)
	total := 1000
	for i := 0; i < total; i++ {
		msg := fmt.Sprintf("Publisher 1: [i:%d]", i)
		want = append(want, msg)
		err := pub1.Publish(&amqp.Publishing{
			Body: []byte(msg),
		})
		require.NoError(t, err)

		msg = fmt.Sprintf("Publisher 2: [i:%d]", i)
		want = append(want, msg)
		err = pub2.Publish(&amqp.Publishing{
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

func testIntegConsumerPublishWorkers(t *testing.T) {
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

func testIntegConsumerReconnect(t *testing.T) {
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

	for i := 0; i < total; i++ {
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

	wg.Add(1)
	var calls int32
	go func() {
		defer wg.Done()
		assert.NotPanics(t, func() {
			err := cons.Consume(ctx, func(*amqp.Delivery) (harego.AckType, time.Duration) {
				g := atomic.AddInt32(&calls, 1)
				if g == int32(total/5) {
					close(restart)
					time.Sleep(time.Second)
				}
				if g >= int32(total) {
					cancel()
				}
				return harego.AckTypeAck, 0
			})
			assert.ErrorIs(t, err, context.Canceled)
		})
	}()

	assert.Eventually(t, func() bool {
		wg.Wait()
		return true
	}, 2*time.Minute, 50*time.Millisecond)

	assert.EqualValues(t, total, atomic.LoadInt32(&calls))
}

func testIntegConsumerClose(t *testing.T) {
	t.Parallel()
	vh := "test." + testament.RandomString(20)
	exchange := "test." + testament.RandomString(20)
	queueName := "test." + testament.RandomString(20)

	total := 10
	_, broker := getConsumerPublisher(t, vh, exchange, queueName, harego.Workers(total))
	defer broker.Close()

	var wg sync.WaitGroup
	for i := 0; i < total-1; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := broker.Publish(&amqp.Publishing{
				Body: []byte(testament.RandomString(20)),
			})
			if err == nil {
				return
			}
			assert.True(t, errors.Is(err, harego.ErrClosed) || errors.Is(err, context.Canceled), err)
		}()
	}

	wg.Add(total / 2)
	for i := 0; i < total/2; i++ {
		go func() {
			defer wg.Done()
			err := broker.Close()
			if err == nil {
				return
			}
			assert.True(t, errors.Is(err, harego.ErrClosed) || errors.Is(err, context.Canceled), err)
		}()
	}

	assert.Eventually(t, func() bool {
		wg.Wait()
		return true
	}, 2*time.Minute, 30*time.Millisecond)
}
