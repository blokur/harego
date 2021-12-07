//go:build integration
// +build integration

package harego_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blokur/harego"
	"github.com/blokur/testament"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegClient(t *testing.T) {
	t.Parallel()
	t.Run("Publish", testIntegClientPublish)
	t.Run("Consume", testIntegClientConsume)
	t.Run("SeparatedConsumePublish", testIntegClientSeparatedConsumePublish)
	t.Run("UseSameQueue", testIntegClientUseSameQueue)
	t.Run("PublishWorkers", testIntegClientPublishWorkers)
	t.Run("Reconnect", testIntegClientReconnect)
	t.Run("Close", testIntegClientClose)
}

func testIntegClientPublish(t *testing.T) {
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
		{100, 100},
	}
	for _, tc := range tcs {
		tc := tc
		name := fmt.Sprintf("%dWorkers/%dMessages/", tc.workers, tc.total)
		t.Run(name, func(t *testing.T) {
			testIntegClientPublishConcurrent(t, tc.total, tc.workers)
		})
	}
}

func testIntegClientPublishConcurrent(t *testing.T, total, workers int) {
	t.Parallel()
	c := getClient(t, "", harego.Workers(workers))
	var wg sync.WaitGroup
	wg.Add(total)
	for i := 0; i < total; i++ {
		go func() {
			defer wg.Done()
			err := c.Publish(&amqp.Publishing{
				Body: []byte(randomBody(1)),
			})
			assert.NoError(t, err)
		}()
	}
	wg.Wait()
}

func testIntegClientConsume(t *testing.T) {
	t.Run("Concurrent", testIntegClientConsumeConcurrent)
	t.Run("Nack", testIntegClientConsumeNack)
	t.Run("Reject", testIntegClientConsumeReject)
	t.Run("Requeue", testIntegClientConsumeRequeue)
}

func testIntegClientConsumeConcurrent(t *testing.T) {
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
		{100, 100},
	}
	for _, tc := range tcs {
		tc := tc
		t.Run(fmt.Sprintf("%dWorkers/%dMessages/", tc.workers, tc.total), func(t *testing.T) {
			testIntegClientConsumeConcurrentDo(t, tc.total, tc.workers)
		})
	}
}

func testIntegClientConsumeConcurrentDo(t *testing.T, total, workers int) {
	t.Parallel()
	exchange := "test." + randomString(20)
	queueName := "test." + randomString(20)
	routingKey := "test." + randomString(20)
	vh := "test." + randomString(20)

	pub := getNamedClient(t, vh, exchange, "",
		harego.Workers(workers),
		harego.RoutingKey(routingKey),
	)
	defer pub.Close()
	cons := getNamedClient(t, vh, exchange, queueName,
		harego.Workers(workers),
		harego.RoutingKey(routingKey),
	)
	defer cons.Close()

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
		testament.AssertInError(t, err, context.Canceled)
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
	assert.ElementsMatch(t, want, got)
}

func testIntegClientConsumeNack(t *testing.T) {
	t.Parallel()
	exchange := "test." + randomString(20)
	queueName := "test." + randomString(20)
	vh := "test." + randomString(20)

	pub := getNamedClient(t, vh, exchange, "")
	defer pub.Close()
	cons1 := getNamedClient(t, vh, exchange, queueName,
		harego.ConsumerName("cons1"),
		harego.Workers(2),
	)
	defer cons1.Close()
	cons2 := getNamedClient(t, vh, exchange, queueName,
		harego.ConsumerName("cons2"),
		harego.Workers(2),
	)
	defer cons2.Close()

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
		testament.AssertInError(t, err, context.Canceled)
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
		testament.AssertInError(t, err, context.Canceled)
		return true
	}, time.Minute, 10*time.Millisecond)
}

func testIntegClientConsumeReject(t *testing.T) {
	t.Parallel()
	exchange := "test." + randomString(20)
	queueName := "test." + randomString(20)
	vh := "test." + randomString(20)

	pub := getNamedClient(t, vh, exchange, "")
	defer pub.Close()
	cons1 := getNamedClient(t, vh, exchange, queueName)
	defer cons1.Close()

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
		testament.AssertInError(t, err, context.Canceled)
		return true
	}, time.Minute, 10*time.Millisecond)
	duration := time.Since(started)
	cons1.Close()

	cons2 := getNamedClient(t, vh, exchange, queueName)
	defer cons2.Close()
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

func testIntegClientConsumeRequeue(t *testing.T) {
	t.Parallel()
	exchange := "test." + randomString(20)
	queueName := "test." + randomString(20)
	vh := "test." + randomString(20)
	pub := getNamedClient(t, vh, exchange, "")
	defer pub.Close()
	cons := getNamedClient(t, vh, exchange, queueName)
	defer cons.Close()

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
		testament.AssertInError(t, err, context.Canceled)
		return true
	}, time.Minute, 10*time.Millisecond)

	assert.EqualValues(t, wantMsgs, gotMsgs)
}

func testIntegClientSeparatedConsumePublish(t *testing.T) {
	t.Parallel()
	vh := "test." + randomString(20)
	exchange1 := "test." + randomString(20)
	exchange2 := "test." + randomString(20)
	queueName1 := "test." + randomString(20)
	queueName2 := "test." + randomString(20)

	pub1 := getNamedClient(t, vh, exchange1, "")
	defer pub1.Close()
	pub2 := getNamedClient(t, vh, exchange2, "")
	defer pub2.Close()
	cons1 := getNamedClient(t, vh, exchange1, queueName1)
	defer cons1.Close()
	cons2 := getNamedClient(t, vh, exchange2, queueName2)
	defer cons2.Close()

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
		testament.AssertInError(t, err, context.Canceled)
	}()
	go func() {
		err := cons2.Consume(ctx, func(msg *amqp.Delivery) (harego.AckType, time.Duration) {
			mu2.Lock()
			defer mu2.Unlock()
			got2 = append(got2, string(msg.Body))
			return harego.AckTypeAck, 0
		})
		testament.AssertInError(t, err, context.Canceled)
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
	}, time.Minute, 10*time.Millisecond)
	mu1.RLock()
	defer mu1.RUnlock()
	mu2.RLock()
	defer mu2.RUnlock()
	assert.ElementsMatch(t, want1, got1)
	assert.ElementsMatch(t, want2, got2)
}

func testIntegClientUseSameQueue(t *testing.T) {
	t.Parallel()
	vh := "test." + randomString(20)
	exchange1 := "test." + randomString(20)
	exchange2 := "test." + randomString(20)
	queueName := "test." + randomString(20)

	pub1 := getNamedClient(t, vh, exchange1, queueName)
	defer pub1.Close()
	pub2 := getNamedClient(t, vh, exchange2, queueName)
	defer pub2.Close()
	cons := getNamedClient(t, vh, exchange2, queueName)
	defer cons.Close()

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
		testament.AssertInError(t, err, context.Canceled)
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
	assert.ElementsMatch(t, want, got)
}

func testIntegClientPublishWorkers(t *testing.T) {
	t.Parallel()
	vh := "test." + randomString(20)
	exchange1 := "test." + randomString(20)
	exchange2 := "test." + randomString(20)
	queueName := "test." + randomString(20)

	pub := getNamedClient(t, vh, exchange1, queueName,
		harego.Workers(10),
	)
	defer pub.Close()
	cons := getNamedClient(t, vh, exchange2, queueName)
	defer cons.Close()

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
		testament.AssertInError(t, err, context.Canceled)
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
	assert.ElementsMatch(t, want, got)
}

func testIntegClientReconnect(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	t.Run("Publish", testIntegClientReconnectPublish)
	t.Run("Consume", testIntegClientReconnectConsume)
}

func testIntegClientReconnectPublish(t *testing.T) {
	t.Parallel()
	exchange := "test." + randomString(20)
	queueName := "test." + randomString(20)
	container, addr := getContainer(t)
	pub, err := harego.NewClient(harego.URLConnector(addr),
		harego.ExchangeName(exchange),
	)
	require.NoError(t, err)

	cons, err := harego.NewClient(harego.URLConnector(addr),
		harego.ExchangeName(exchange),
		harego.QueueName(queueName),
	)
	require.NoError(t, err)
	defer cons.Close()

	var (
		want  []string
		mu    sync.RWMutex
		got   []string
		total = 1000
	)
	assert.Eventually(t, func() bool {
		for i := 0; i < total; i++ {
			msg := fmt.Sprintf("Message: [i:%d]", i)
			want = append(want, msg)
			err := pub.Publish(&amqp.Publishing{
				Body: []byte(msg),
			})
			require.NoError(t, err)
			if i%(total/5) == 0 {
				restartRabbitMQ(t, container)
				time.Sleep(time.Second)
			}
		}
		return true
	}, time.Minute, 10*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		err := cons.Consume(ctx, func(msg *amqp.Delivery) (harego.AckType, time.Duration) {
			mu.Lock()
			defer mu.Unlock()
			got = append(got, string(msg.Body))
			return harego.AckTypeAck, 0
		})
		testament.AssertInError(t, err, context.Canceled)
	}()
	assert.Eventually(t, func() bool {
		mu.RLock()
		defer mu.RUnlock()
		return len(want) == len(got)
	}, time.Minute, 10*time.Millisecond)

	assert.ElementsMatch(t, want, got)
}

func testIntegClientReconnectConsume(t *testing.T) {
	t.Parallel()
	var (
		total     = 10
		wg        sync.WaitGroup
		exchange  = "test." + randomString(20)
		queueName = "test." + randomString(20)
	)

	container, addr := getContainer(t)
	pub, err := harego.NewClient(harego.URLConnector(addr),
		harego.ExchangeName(exchange),
		// harego.Workers(10),
	)
	require.NoError(t, err)

	cons, err := harego.NewClient(harego.URLConnector(addr),
		harego.ExchangeName(exchange),
		harego.QueueName(queueName),
		// harego.Workers(3),
	)
	require.NoError(t, err)
	defer cons.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := 0; i < total; i++ {
		err := pub.Publish(&amqp.Publishing{
			Body: []byte(randomString(10)),
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
		require.NotPanics(t, func() {
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
			testament.AssertInError(t, err, context.Canceled)
		})
	}()

	assert.Eventually(t, func() bool {
		wg.Wait()
		return true
	}, time.Minute, 50*time.Millisecond)

	assert.EqualValues(t, total, atomic.LoadInt32(&calls))
}

func testIntegClientClose(t *testing.T) {
	t.Parallel()
	vh := "test." + randomString(20)
	exchange := "test." + randomString(20)
	queueName := "test." + randomString(20)

	total := 10
	broker := getNamedClient(t, vh, exchange, queueName, harego.Workers(total))
	defer broker.Close()

	var wg sync.WaitGroup
	for i := 0; i < total-1; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := broker.Publish(&amqp.Publishing{
				Body: []byte(randomString(20)),
			})
			if err == nil {
				return
			}
			assert.Contains(t, []error{harego.ErrClosed, context.Canceled}, errors.Cause(err))
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
			assert.Contains(t, []error{harego.ErrClosed, context.Canceled}, errors.Cause(err))
		}()
	}

	assert.Eventually(t, func() bool {
		wg.Wait()
		return true
	}, 2*time.Minute, 30*time.Millisecond)
}
