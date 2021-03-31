package harego_test

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/blokur/harego"
	"github.com/blokur/harego/mocks"
	"github.com/blokur/testament"
	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestConfigFunc(t *testing.T) {
	t.Parallel()
	tcs := map[string]harego.ConfigFunc{
		"QueueName":        harego.QueueName(randomString(10)),
		"QueueArgs":        harego.QueueArgs(map[string]interface{}{}),
		"RoutingKey":       harego.RoutingKey(randomString(10)),
		"Workers":          harego.Workers(rand.Intn(9) + 1),
		"WithDeliveryMode": harego.WithDeliveryMode(harego.DeliveryModePersistent),
		"PrefetchCount":    harego.PrefetchCount(rand.Intn(99)),
		"PrefetchSize":     harego.PrefetchSize(rand.Intn(99)),
		"WithExchangeType": harego.WithExchangeType(harego.ExchangeTypeFanout),
		"ExchangeName":     harego.ExchangeName(randomString(10)),
		"ConsumerName":     harego.ConsumerName(randomString(10)),
		"NotDurable":       harego.NotDurable,
		"AutoDelete":       harego.AutoDelete,
		"Internal":         harego.Internal,
		"NoWait":           harego.NoWait,
		"ExclusiveQueue":   harego.ExclusiveQueue,
	}
	for name, conf := range tcs {
		conf := conf
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			client, err := harego.NewClient(func() (harego.RabbitMQ, error) {
				return &mocks.RabbitMQSimple{}, nil
			}, conf)
			require.NoError(t, err)

			err = conf(client)
			testament.AssertInError(t, err, harego.ErrAlreadyConfigured)
		})
	}
	_, err := harego.NewClient(func() (harego.RabbitMQ, error) { return &mocks.RabbitMQSimple{}, nil },
		harego.QueueName(randomString(10)),
		harego.RoutingKey(randomString(10)),
		func(*harego.Client) error {
			return assert.AnError
		})
	testament.AssertInError(t, err, assert.AnError)
}

func TestNewClient(t *testing.T) {
	t.Parallel()
	t.Run("BadInput", testNewClientBadInput)
	t.Run("Channel", testNewClientChannel)
	t.Run("Qos", testNewClientQos)
	t.Run("ExchangeDeclare", testNewClientExchangeDeclare)
	t.Run("QueueDeclare", testNewClientQueueDeclare)
	t.Run("QueueBind", testNewClientQueueBind)
}

func testNewClientBadInput(t *testing.T) {
	t.Parallel()
	conn := func() (harego.RabbitMQ, error) {
		return &mocks.RabbitMQ{}, nil
	}
	tcs := []struct {
		msg  string
		conf []harego.ConfigFunc
	}{
		{"workers", []harego.ConfigFunc{
			harego.Workers(0),
		}},
		{"consumer name", []harego.ConfigFunc{
			harego.ConsumerName(""),
		}},
		{"exchange name", []harego.ConfigFunc{
			harego.ExchangeName(""),
		}},
		{"exchange type", []harego.ConfigFunc{
			harego.WithExchangeType(-1),
		}},
		{"exchange type", []harego.ConfigFunc{
			harego.WithExchangeType(9999999),
		}},
		{"prefetch size", []harego.ConfigFunc{
			harego.PrefetchSize(-1),
		}},
		{"delivery mode", []harego.ConfigFunc{
			harego.WithDeliveryMode(10),
		}},
	}
	for i, tc := range tcs {
		tc := tc
		name := fmt.Sprintf("%d_%s", i, tc.msg)
		t.Run(name, func(t *testing.T) {
			_, err := harego.NewClient(conn,
				tc.conf...,
			)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.msg)
		})
	}

	_, err := harego.NewClient(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection")

	_, err = harego.NewClient(func() (harego.RabbitMQ, error) {
		return nil, assert.AnError
	})
	require.Error(t, err)
	testament.AssertInError(t, err, assert.AnError)
}

func testNewClientChannel(t *testing.T) {
	t.Parallel()
	r := &mocks.RabbitMQ{}
	defer r.AssertExpectations(t)
	r.On("Channel").Return(nil, assert.AnError).Once()

	_, err := harego.NewClient(func() (harego.RabbitMQ, error) { return r, nil })
	testament.AssertInError(t, err, assert.AnError)
}

func testNewClientQos(t *testing.T) {
	t.Parallel()
	r := &mocks.RabbitMQ{}
	defer r.AssertExpectations(t)
	ch := &mocks.Channel{}
	defer ch.AssertExpectations(t)
	r.On("Channel").Return(ch, nil).Once()
	ch.On("Qos", mock.Anything, mock.Anything, mock.Anything).
		Return(assert.AnError).Once()

	_, err := harego.NewClient(func() (harego.RabbitMQ, error) { return r, nil })
	testament.AssertInError(t, err, assert.AnError)
}

func testNewClientExchangeDeclare(t *testing.T) {
	t.Parallel()
	r := &mocks.RabbitMQ{}
	defer r.AssertExpectations(t)
	ch := &mocks.Channel{}
	defer ch.AssertExpectations(t)
	r.On("Channel").Return(ch, nil).Once()
	ch.On("Qos", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).
		Return(assert.AnError).Once()

	_, err := harego.NewClient(func() (harego.RabbitMQ, error) { return r, nil })
	testament.AssertInError(t, err, assert.AnError)
}

func testNewClientQueueDeclare(t *testing.T) {
	t.Parallel()
	r := &mocks.RabbitMQ{}
	defer r.AssertExpectations(t)
	ch := &mocks.Channel{}
	defer ch.AssertExpectations(t)

	r.On("Channel").Return(ch, nil).Once()
	ch.On("Qos", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything).
		Return(amqp.Queue{}, assert.AnError).Once()

	_, err := harego.NewClient(func() (harego.RabbitMQ, error) { return r, nil },
		harego.QueueName(randomString(20)),
	)
	testament.AssertInError(t, err, assert.AnError)
}

func testNewClientQueueBind(t *testing.T) {
	t.Run("NoArgs", testNewClientQueueBindNoArgs)
	t.Run("Args", testNewClientQueueBindArgs)
}

func testNewClientQueueBindNoArgs(t *testing.T) {
	t.Parallel()
	r := &mocks.RabbitMQ{}
	defer r.AssertExpectations(t)
	ch := &mocks.Channel{}
	defer ch.AssertExpectations(t)
	prefetchCount := rand.Intn(9999)
	prefetchSize := rand.Intn(9999)
	queue := randomString(10)
	r.On("Channel").Return(ch, nil).Once()
	ch.On("Qos", prefetchCount, prefetchSize, mock.Anything).
		Return(nil).Once()
	ch.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("QueueDeclare", queue, false, true, mock.Anything,
		true, mock.Anything).
		Return(amqp.Queue{}, nil).Once()
	ch.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(assert.AnError).Once()

	_, err := harego.NewClient(func() (harego.RabbitMQ, error) { return r, nil },
		harego.PrefetchCount(prefetchCount),
		harego.PrefetchSize(prefetchSize),
		harego.QueueName(queue),
		harego.NotDurable,
		harego.AutoDelete,
		harego.NoWait,
	)
	testament.AssertInError(t, err, assert.AnError)
}

func testNewClientQueueBindArgs(t *testing.T) {
	t.Parallel()
	r := &mocks.RabbitMQ{}
	defer r.AssertExpectations(t)
	ch := &mocks.Channel{}
	defer ch.AssertExpectations(t)
	prefetchCount := rand.Intn(9999)
	prefetchSize := rand.Intn(9999)
	queue := randomString(10)
	args := map[string]interface{}{
		"arg1": "val1",
		"arg2": "val2",
	}

	r.On("Channel").Return(ch, nil).Once()
	ch.On("Qos", prefetchCount, prefetchSize, mock.Anything).
		Return(nil).Once()
	ch.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("QueueDeclare", queue, false, true, mock.Anything,
		true, mock.MatchedBy(func(a amqp.Table) bool {
			if diff := cmp.Diff(amqp.Table(args), a); diff != "" {
				t.Errorf("(-want +got):\n%s", diff)
			}
			return true
		})).
		Return(amqp.Queue{}, nil).Once()
	ch.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(assert.AnError).Once()

	_, err := harego.NewClient(func() (harego.RabbitMQ, error) { return r, nil },
		harego.PrefetchCount(prefetchCount),
		harego.PrefetchSize(prefetchSize),
		harego.QueueName(queue),
		harego.QueueArgs(args),
		harego.NotDurable,
		harego.AutoDelete,
		harego.NoWait,
	)
	testament.AssertInError(t, err, assert.AnError)
}

func TestClient(t *testing.T) {
	t.Run("Publish", testClientPublish)
	t.Run("Consume", testClientConsume)
	t.Run("Close", testClientClose)
}

func testClientPublish(t *testing.T) {
	t.Run("AlreadyClosed", testClientPublishAlreadyClosed)
	t.Run("PublishError", testClientPublishPublishError)
}

func testClientPublishAlreadyClosed(t *testing.T) {
	t.Parallel()
	r := &mocks.RabbitMQ{}
	defer r.AssertExpectations(t)
	ch := &mocks.Channel{}
	defer ch.AssertExpectations(t)
	r.On("Channel").Return(ch, nil).Once()
	ch.On("Qos", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("NotifyClose", mock.Anything).
		Return(make(chan *amqp.Error, 10)).Once()

	cl, err := harego.NewClient(func() (harego.RabbitMQ, error) { return r, nil })
	require.NoError(t, err)

	ch.On("Close").Return(nil).Once()
	r.On("Close").Return(nil).Once()
	err = cl.Close()
	assert.NoError(t, err)

	err = cl.Publish(&amqp.Publishing{})
	testament.AssertInError(t, err, harego.ErrClosed)
}

func testClientPublishPublishError(t *testing.T) {
	t.Parallel()
	r := &mocks.RabbitMQ{}
	defer r.AssertExpectations(t)
	ch := &mocks.Channel{}
	defer ch.AssertExpectations(t)
	r.On("Channel").Return(ch, nil).Once()
	ch.On("Qos", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()

	exchName := randomString(10)
	exchType := harego.ExchangeTypeFanout
	ch.On("ExchangeDeclare",
		mock.MatchedBy(func(name string) bool {
			assert.Contains(t, name, exchName)
			return true
		}),
		exchType.String(),
		true, true, true, true, mock.Anything,
	).Return(nil).Once()
	ch.On("NotifyClose", mock.Anything).
		Return(make(chan *amqp.Error, 10)).Once()

	cl, err := harego.NewClient(func() (harego.RabbitMQ, error) { return r, nil },
		harego.ExchangeName(exchName),
		harego.WithExchangeType(exchType),
		harego.AutoDelete,
		harego.Internal,
		harego.NoWait,
	)
	require.NoError(t, err)

	ch.On("Publish", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(assert.AnError).Once()
	err = cl.Publish(&amqp.Publishing{})
	testament.AssertInError(t, err, assert.AnError)
}

func testClientConsume(t *testing.T) {
	t.Run("QueueName", testClientConsumeQueueName)
	t.Run("ChannelError", testClientConsumeChannelError)
	t.Run("NilHandler", testClientConsumeNilHandler)
	t.Run("CancelledContext", testClientConsumeCancelledContext)
	t.Run("AlreadyClosed", testClientConsumeAlreadyClosed)
}

func testClientConsumeQueueName(t *testing.T) {
	t.Parallel()
	r := &mocks.RabbitMQ{}
	defer r.AssertExpectations(t)
	ch := &mocks.Channel{}
	defer ch.AssertExpectations(t)
	r.On("Channel").Return(ch, nil).Once()
	ch.On("Qos", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("NotifyClose", mock.Anything).
		Return(make(chan *amqp.Error, 10)).Once()

	consumerName := randomString(10)
	cl, err := harego.NewClient(func() (harego.RabbitMQ, error) { return r, nil },
		harego.ConsumerName(consumerName),
		harego.NoWait,
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = cl.Consume(ctx, func(*amqp.Delivery) (a harego.AckType, delay time.Duration) { return 0, 0 })
	testament.AssertInError(t, err, harego.ErrInput)
}

func testClientConsumeChannelError(t *testing.T) {
	t.Parallel()
	r := &mocks.RabbitMQ{}
	defer r.AssertExpectations(t)
	ch := &mocks.Channel{}
	defer ch.AssertExpectations(t)
	r.On("Channel").Return(ch, nil).Once()
	ch.On("Qos", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything).
		Return(amqp.Queue{}, nil).Once()
	ch.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("NotifyClose", mock.Anything).
		Return(make(chan *amqp.Error, 10)).Once()

	queueName := randomString(10)
	consumerName := randomString(10)
	cl, err := harego.NewClient(func() (harego.RabbitMQ, error) { return r, nil },
		harego.QueueName(queueName),
		harego.ConsumerName(consumerName),
		harego.NoWait,
	)
	require.NoError(t, err)

	ch.On("Consume",
		queueName,
		consumerName,
		mock.Anything, mock.Anything, mock.Anything,
		true,
		mock.Anything,
	).Return(nil, assert.AnError).Once()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = cl.Consume(ctx, func(*amqp.Delivery) (a harego.AckType, delay time.Duration) { return 0, 0 })
	testament.AssertInError(t, err, assert.AnError)
}

func testClientConsumeNilHandler(t *testing.T) {
	t.Parallel()
	r := &mocks.RabbitMQ{}
	defer r.AssertExpectations(t)
	ch := &mocks.Channel{}
	defer ch.AssertExpectations(t)
	r.On("Channel").Return(ch, nil).Once()
	ch.On("Qos", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything).
		Return(amqp.Queue{}, nil).Once()
	ch.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("NotifyClose", mock.Anything).
		Return(make(chan *amqp.Error, 10)).Once()

	cl, err := harego.NewClient(func() (harego.RabbitMQ, error) { return r, nil },
		harego.QueueName(randomString(10)),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = cl.Consume(ctx, nil)
	testament.AssertInError(t, err, harego.ErrNilHnadler)
}

func testClientConsumeCancelledContext(t *testing.T) {
	t.Parallel()
	r := &mocks.RabbitMQ{}
	defer r.AssertExpectations(t)
	ch := &mocks.Channel{}
	defer ch.AssertExpectations(t)
	r.On("Channel").Return(ch, nil).Once()
	ch.On("Qos", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything).
		Return(amqp.Queue{}, nil).Once()
	ch.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("NotifyClose", mock.Anything).
		Return(make(chan *amqp.Error, 10)).Once()

	cl, err := harego.NewClient(func() (harego.RabbitMQ, error) { return r, nil },
		harego.QueueName(randomString(10)),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	delivery := make(chan amqp.Delivery, 1)
	delivery <- amqp.Delivery{Body: []byte("first message")}

	ch.On("Consume", mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Return((<-chan amqp.Delivery)(delivery), nil).Once() //nolint:gocritic // otherwise the mock breaks.

	var wg sync.WaitGroup
	wg.Add(1)
	err = cl.Consume(ctx, func(d *amqp.Delivery) (a harego.AckType, delay time.Duration) {
		select {
		case <-ctx.Done():
			t.Errorf("didn't expect to receive a call for: %q", d.Body)
			return 0, 0
		default:
		}
		cancel()
		delivery <- amqp.Delivery{Body: []byte("second message")}
		wg.Done()
		return 0, 0
	})
	testament.AssertInError(t, err, context.Canceled)

	assert.Eventually(t, func() bool {
		wg.Wait()
		return true
	}, 10*time.Second, 10*time.Millisecond)
}

func testClientConsumeAlreadyClosed(t *testing.T) {
	t.Parallel()
	r := &mocks.RabbitMQ{}
	defer r.AssertExpectations(t)
	ch := &mocks.Channel{}
	defer ch.AssertExpectations(t)
	r.On("Channel").Return(ch, nil).Once()
	ch.On("Qos", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything).
		Return(amqp.Queue{}, nil).Once()
	ch.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("NotifyClose", mock.Anything).
		Return(make(chan *amqp.Error, 10)).Once()

	cl, err := harego.NewClient(func() (harego.RabbitMQ, error) { return r, nil },
		harego.QueueName(randomString(10)),
	)
	require.NoError(t, err)

	ch.On("Close").Return(nil)
	r.On("Close").Return(nil)
	err = cl.Close()
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = cl.Consume(ctx, func(d *amqp.Delivery) (a harego.AckType, delay time.Duration) {
		t.Errorf("didn't expect to receive a call for: %q", d.Body)
		return 0, 0
	})
	testament.AssertInError(t, err, harego.ErrClosed)
}

func testClientClose(t *testing.T) {
	t.Run("AlreadyClosed", testClientCloseAlreadyClosed)
	t.Run("Errors", testClientCloseErrors)
	t.Run("MultipleTimes", testClientCloseMultipleTimes)
	t.Run("PublishNotPanic", testClientClosePublishNotPanic)
}

func testClientCloseAlreadyClosed(t *testing.T) {
	t.Parallel()
	r := &mocks.RabbitMQ{}
	defer r.AssertExpectations(t)
	ch := &mocks.Channel{}
	defer ch.AssertExpectations(t)
	r.On("Channel").Return(ch, nil).Once()
	ch.On("Qos", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("NotifyClose", mock.Anything).
		Return(make(chan *amqp.Error, 10)).Once()

	cl, err := harego.NewClient(func() (harego.RabbitMQ, error) { return r, nil })
	require.NoError(t, err)

	ch.On("Close").Return(nil)
	r.On("Close").Return(nil)
	err = cl.Close()
	assert.NoError(t, err)

	err = cl.Close()
	testament.AssertInError(t, err, harego.ErrClosed)
}

func testClientCloseErrors(t *testing.T) {
	err1 := errors.New(randomString(10))
	err2 := errors.New(randomString(10))

	tcs := map[string]struct {
		channelErr error
		connErr    error
		wantErrs   []error
	}{
		"no error":    {nil, nil, nil},
		"chan error":  {err1, nil, []error{err1}},
		"conn error":  {nil, err1, []error{err1}},
		"both errors": {err1, err2, []error{err1, err2}},
	}
	for name, tc := range tcs {
		tc := tc
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			r := &mocks.RabbitMQ{}
			defer r.AssertExpectations(t)
			ch := &mocks.Channel{}
			defer ch.AssertExpectations(t)
			r.On("Channel").Return(ch, nil).Once()
			ch.On("Qos", mock.Anything, mock.Anything, mock.Anything).
				Return(nil).Once()
			ch.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
				mock.Anything, mock.Anything, mock.Anything).
				Return(nil).Once()
			ch.On("NotifyClose", mock.Anything).
				Return(make(chan *amqp.Error, 10)).Once()

			cl, err := harego.NewClient(func() (harego.RabbitMQ, error) { return r, nil })
			require.NoError(t, err)

			ch.On("Close").Return(tc.channelErr)
			r.On("Close").Return(tc.connErr)
			err = cl.Close()
			for _, e := range tc.wantErrs {
				testament.AssertInError(t, err, e)
			}
		})
	}
}

func testClientCloseMultipleTimes(t *testing.T) {
	t.Parallel()
	cl, err := harego.NewClient(func() (harego.RabbitMQ, error) {
		return &mocks.RabbitMQSimple{}, nil
	})
	require.NoError(t, err)

	err = cl.Close()
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		err = cl.Close()
		testament.AssertInError(t, err, harego.ErrClosed)
	}
}

func testClientClosePublishNotPanic(t *testing.T) {
	t.Parallel()
	r := &mocks.RabbitMQSimple{}

	total := 100
	cl, err := harego.NewClient(func() (harego.RabbitMQ, error) { return r, nil },
		harego.Workers(total),
	)
	require.NoError(t, err)

	var wg sync.WaitGroup
	for i := 0; i < total-1; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NotPanics(t, func() {
				cl.Publish(&amqp.Publishing{})
			})
		}()
	}

	err = cl.Close()
	assert.NoError(t, err)
	assert.Eventually(t, func() bool {
		wg.Wait()
		return true
	}, 120*time.Second, 10*time.Millisecond)
}
