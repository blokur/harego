// +build integration

package harego_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/blokur/harego"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegClient(t *testing.T) {
	t.Run("Publish", testIntegClientPublish)
	t.Run("Consume", testIntegClientConsume)
	t.Run("Close", testIntegClientClose)
	t.Run("Reconnect", testIntegClientReconnect)
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
	c := getClient(t)
	harego.Workers(workers)(c)
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
	t.Run("DifferntQueues", testIntegClientConsumeDifferntQueues)
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

	pub := getNamedClient(t, exchange, queueName)
	harego.Workers(workers)(pub)
	cons := getNamedClient(t, exchange, queueName)
	harego.Workers(workers)(cons)

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
		err := cons.Consume(ctx, func(msg amqp.Delivery) (harego.AckType, time.Duration) {
			muGot.Lock()
			defer muGot.Unlock()
			got = append(got, string(msg.Body))
			return harego.AckTypeAck, 0
		})
		assert.EqualError(t, err, ctx.Err().Error())
	}()
	assert.Eventually(t, func() bool {
		muGot.RLock()
		defer muGot.RUnlock()
		muWant.RLock()
		defer muWant.RUnlock()
		if len(want) == len(got) {
			cancel()
			return true
		}
		return false
	}, time.Minute, 10*time.Millisecond)

	wg.Wait()
	muGot.RLock()
	defer muGot.RUnlock()
	muWant.RLock()
	defer muWant.RUnlock()
	assert.ElementsMatch(t, want, got)
}

func testIntegClientConsumeDifferntQueues(t *testing.T) {
	t.Parallel()
	exchange1 := "test." + randomString(20)
	exchange2 := "test." + randomString(20)
	queueName1 := "test." + randomString(20)
	queueName2 := "test." + randomString(20)

	pub1 := getNamedClient(t, exchange1, queueName1)
	pub2 := getNamedClient(t, exchange2, queueName2)
	cons1 := getNamedClient(t, exchange1, queueName1)
	cons2 := getNamedClient(t, exchange2, queueName2)

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
		err := pub1.Publish(&amqp.Publishing{
			Body: []byte(msg),
		})
		require.NoError(t, err)
		want1 = append(want1, msg)

		msg = fmt.Sprintf("Queue 2: [i:%d]", i)
		err = pub2.Publish(&amqp.Publishing{
			Body: []byte(msg),
		})
		require.NoError(t, err)
		want2 = append(want2, msg)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		err := cons1.Consume(ctx, func(msg amqp.Delivery) (harego.AckType, time.Duration) {
			fmt.Println("Queue 1 got:", string(msg.Body))
			mu1.Lock()
			defer mu1.Unlock()
			got1 = append(got1, string(msg.Body))
			return harego.AckTypeAck, 0
		})
		assert.EqualError(t, err, ctx.Err().Error())
	}()
	go func() {
		err := cons2.Consume(ctx, func(msg amqp.Delivery) (harego.AckType, time.Duration) {
			mu2.Lock()
			defer mu2.Unlock()
			fmt.Println("Queue 2 got:", string(msg.Body))
			got2 = append(got2, string(msg.Body))
			return harego.AckTypeAck, 0
		})
		assert.EqualError(t, err, ctx.Err().Error())
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
	}, time.Minute/20, 10*time.Millisecond)
	mu1.RLock()
	defer mu1.RUnlock()
	mu2.RLock()
	defer mu2.RUnlock()
	assert.ElementsMatch(t, want1, got1)
	assert.ElementsMatch(t, want2, got2)
}

func testIntegClientClose(t *testing.T) {
	t.Skip("not implemented")
}

func testIntegClientReconnect(t *testing.T) {
	t.Skip("not implemented")
}
