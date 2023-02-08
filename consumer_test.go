package harego_test

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/blokur/testament"
	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/blokur/harego/v2"
	"github.com/blokur/harego/v2/mocks"
)

func TestNewConsumer(t *testing.T) {
	t.Parallel()
	t.Run("BadInput", testNewConsumerBadInput)
	t.Run("Channel", testNewConsumerChannel)
	t.Run("Qos", testNewConsumerQos)
	t.Run("QueueDeclare", testNewConsumerQueueDeclare)
	t.Run("QueueBind", testNewConsumerQueueBind)
}

func testNewConsumerBadInput(t *testing.T) {
	t.Parallel()
	conn := func() (harego.RabbitMQ, error) {
		return &mocks.RabbitMQ{}, nil
	}
	tcs := []struct {
		msg  string
		conf []harego.ConfigFunc
	}{
		{"workers", []harego.ConfigFunc{
			harego.QueueName(testament.RandomLowerString(10)),
			harego.Workers(0),
		}},
		{"consumer name", []harego.ConfigFunc{
			harego.QueueName(testament.RandomLowerString(10)),
			harego.ConsumerName(""),
		}},
		{"prefetch size", []harego.ConfigFunc{
			harego.QueueName(testament.RandomLowerString(10)),
			harego.PrefetchSize(-1),
		}},
		{"delivery mode", []harego.ConfigFunc{
			harego.QueueName(testament.RandomLowerString(10)),
			harego.WithDeliveryMode(10),
		}},
		{"queue name", []harego.ConfigFunc{}},
	}

	for i, tc := range tcs {
		tc := tc
		name := fmt.Sprintf("%d_%s", i, tc.msg)
		t.Run(name, func(t *testing.T) {
			_, err := harego.NewConsumer(conn,
				tc.conf...,
			)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.msg)
		})
	}

	_, err := harego.NewConsumer(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection")

	_, err = harego.NewConsumer(func() (harego.RabbitMQ, error) {
		return nil, assert.AnError
	}, harego.QueueName(testament.RandomLowerString(10)))
	require.Error(t, err)
	assert.ErrorIs(t, err, assert.AnError)
}

func testNewConsumerChannel(t *testing.T) {
	t.Parallel()
	r := mocks.NewRabbitMQ(t)
	r.On("Channel").Return(nil, assert.AnError).Once()

	_, err := harego.NewConsumer(func() (harego.RabbitMQ, error) { return r, nil },
		harego.QueueName(testament.RandomLowerString(10)),
	)
	assert.ErrorIs(t, err, assert.AnError)
}

func testNewConsumerQos(t *testing.T) {
	t.Parallel()
	r := mocks.NewRabbitMQ(t)
	ch := mocks.NewChannel(t)

	r.On("Channel").Return(ch, nil).Once()
	ch.On("Qos", mock.Anything, mock.Anything, mock.Anything).
		Return(assert.AnError).Once()

	_, err := harego.NewConsumer(func() (harego.RabbitMQ, error) { return r, nil },
		harego.QueueName(testament.RandomLowerString(10)),
	)
	assert.ErrorIs(t, err, assert.AnError)
}

func testNewConsumerQueueDeclare(t *testing.T) {
	t.Parallel()
	r := mocks.NewRabbitMQ(t)
	ch := mocks.NewChannel(t)

	r.On("Channel").Return(ch, nil).Once()
	ch.On("Qos", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything).
		Return(amqp.Queue{}, assert.AnError).Once()

	_, err := harego.NewConsumer(func() (harego.RabbitMQ, error) { return r, nil },
		harego.QueueName(testament.RandomString(20)),
	)
	assert.ErrorIs(t, err, assert.AnError)
}

func testNewConsumerQueueBind(t *testing.T) {
	t.Parallel()
	t.Run("NoArgs", testNewConsumerQueueBindNoArgs)
	t.Run("Args", testNewConsumerQueueBindArgs)
}

func testNewConsumerQueueBindNoArgs(t *testing.T) {
	t.Parallel()
	r := mocks.NewRabbitMQ(t)
	ch := mocks.NewChannel(t)

	prefetchCount := rand.Intn(9999)
	prefetchSize := rand.Intn(9999)
	queue := testament.RandomString(10)

	r.On("Channel").Return(ch, nil).Once()
	ch.On("Qos", prefetchCount, prefetchSize, mock.Anything).
		Return(nil).Once()
	ch.On("QueueDeclare", queue, false, true, mock.Anything,
		true, mock.Anything).
		Return(amqp.Queue{}, nil).Once()
	ch.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(assert.AnError).Once()

	_, err := harego.NewConsumer(func() (harego.RabbitMQ, error) { return r, nil },
		harego.PrefetchCount(prefetchCount),
		harego.PrefetchSize(prefetchSize),
		harego.QueueName(queue),
		harego.NotDurable,
		harego.AutoDelete,
		harego.NoWait,
	)
	assert.ErrorIs(t, err, assert.AnError)
}

func testNewConsumerQueueBindArgs(t *testing.T) {
	t.Parallel()
	r := mocks.NewRabbitMQ(t)
	ch := mocks.NewChannel(t)

	prefetchCount := rand.Intn(9999)
	prefetchSize := rand.Intn(9999)
	queue := testament.RandomString(10)
	args := map[string]interface{}{
		"arg1": "val1",
		"arg2": "val2",
	}

	r.On("Channel").Return(ch, nil).Once()
	ch.On("Qos", prefetchCount, prefetchSize, mock.Anything).
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

	_, err := harego.NewConsumer(func() (harego.RabbitMQ, error) { return r, nil },
		harego.PrefetchCount(prefetchCount),
		harego.PrefetchSize(prefetchSize),
		harego.QueueName(queue),
		harego.QueueArgs(args),
		harego.NotDurable,
		harego.AutoDelete,
		harego.NoWait,
	)
	assert.ErrorIs(t, err, assert.AnError)
}

func TestClient(t *testing.T) {
	t.Parallel()
	t.Run("Consume", testClientConsume)
	t.Run("Close", testClientClose)
}

func testClientConsume(t *testing.T) {
	t.Parallel()
	t.Run("ChannelError", testClientConsumeChannelError)
	t.Run("NilHandler", testClientConsumeNilHandler)
	t.Run("CancelledContext", testClientConsumeCancelledContext)
	t.Run("AlreadyClosed", testClientConsumeAlreadyClosed)
}

func testClientConsumeChannelError(t *testing.T) {
	t.Parallel()
	r := mocks.NewRabbitMQ(t)
	ch := mocks.NewChannel(t)

	r.On("Channel").Return(ch, nil)
	ch.On("Qos", mock.Anything, mock.Anything, mock.Anything).
		Return(nil)
	ch.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything).
		Return(amqp.Queue{}, nil).Once()
	ch.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("NotifyClose", mock.Anything).
		Return(make(chan *amqp.Error, 10))

	queueName := testament.RandomString(10)
	consumerName := testament.RandomString(10)
	cons, err := harego.NewConsumer(func() (harego.RabbitMQ, error) { return r, nil },
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

	err = cons.Consume(ctx, func(*amqp.Delivery) (a harego.AckType, delay time.Duration) { return 0, 0 })
	assert.ErrorIs(t, err, assert.AnError)
}

func testClientConsumeNilHandler(t *testing.T) {
	t.Parallel()
	r := mocks.NewRabbitMQ(t)
	ch := mocks.NewChannel(t)

	r.On("Channel").Return(ch, nil)
	ch.On("Qos", mock.Anything, mock.Anything, mock.Anything).
		Return(nil)
	ch.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything).
		Return(amqp.Queue{}, nil).Once()
	ch.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("NotifyClose", mock.Anything).
		Return(make(chan *amqp.Error, 10))

	cons, err := harego.NewConsumer(func() (harego.RabbitMQ, error) { return r, nil },
		harego.QueueName(testament.RandomString(10)),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = cons.Consume(ctx, nil)
	assert.ErrorIs(t, err, harego.ErrNilHnadler)
}

func testClientConsumeCancelledContext(t *testing.T) {
	t.Parallel()
	r := mocks.NewRabbitMQ(t)
	ch := mocks.NewChannel(t)

	r.On("Channel").Return(ch, nil)
	ch.On("Qos", mock.Anything, mock.Anything, mock.Anything).
		Return(nil)
	ch.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).
		Return(nil)
	ch.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything).
		Return(amqp.Queue{}, nil).Once()
	ch.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("NotifyClose", mock.Anything).
		Return(make(chan *amqp.Error, 10))

	cons, err := harego.NewConsumer(func() (harego.RabbitMQ, error) { return r, nil },
		harego.QueueName(testament.RandomString(10)),
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

	err = cons.Consume(ctx, func(d *amqp.Delivery) (a harego.AckType, delay time.Duration) {
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
	assert.ErrorIs(t, err, context.Canceled)

	assert.Eventually(t, func() bool {
		wg.Wait()
		return true
	}, 10*time.Second, 10*time.Millisecond)
}

func testClientConsumeAlreadyClosed(t *testing.T) {
	t.Parallel()
	r := mocks.NewRabbitMQ(t)
	ch := mocks.NewChannel(t)

	r.On("Channel").Return(ch, nil)
	ch.On("Qos", mock.Anything, mock.Anything, mock.Anything).
		Return(nil)
	ch.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything).
		Return(amqp.Queue{}, nil).Once()
	ch.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("NotifyClose", mock.Anything).
		Return(make(chan *amqp.Error, 10))

	cons, err := harego.NewConsumer(func() (harego.RabbitMQ, error) { return r, nil },
		harego.QueueName(testament.RandomString(10)),
	)
	require.NoError(t, err)

	ch.On("Close").Return(nil)
	r.On("Close").Return(nil)
	err = cons.Close()
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = cons.Consume(ctx, func(d *amqp.Delivery) (a harego.AckType, delay time.Duration) {
		t.Errorf("didn't expect to receive a call for: %q", d.Body)
		return 0, 0
	})
	assert.ErrorIs(t, err, harego.ErrClosed)
}

func testClientClose(t *testing.T) {
	t.Parallel()
	t.Run("AlreadyClosed", testClientCloseAlreadyClosed)
	t.Run("Errors", testClientCloseErrors)
	t.Run("MultipleTimes", testClientCloseMultipleTimes)
}

func testClientCloseAlreadyClosed(t *testing.T) {
	t.Parallel()
	r := mocks.NewRabbitMQ(t)
	ch := mocks.NewChannel(t)

	r.On("Channel").Return(ch, nil)
	ch.On("Qos", mock.Anything, mock.Anything, mock.Anything).
		Return(nil)
	ch.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything).
		Return(amqp.Queue{}, nil).Once()
	ch.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("NotifyClose", mock.Anything).
		Return(make(chan *amqp.Error, 10))

	cons, err := harego.NewConsumer(func() (harego.RabbitMQ, error) { return r, nil },
		harego.QueueName(testament.RandomLowerString(10)),
	)
	require.NoError(t, err)

	ch.On("Close").Return(nil)
	r.On("Close").Return(nil)
	err = cons.Close()
	assert.NoError(t, err)

	err = cons.Close()
	assert.ErrorIs(t, err, harego.ErrClosed)
}

func testClientCloseErrors(t *testing.T) {
	t.Parallel()
	err1 := errors.New(testament.RandomString(10))
	err2 := errors.New(testament.RandomString(10))

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
			r := mocks.NewRabbitMQ(t)
			ch := mocks.NewChannel(t)

			r.On("Channel").Return(ch, nil)
			ch.On("Qos", mock.Anything, mock.Anything, mock.Anything).
				Return(nil)
			ch.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
				mock.Anything, mock.Anything, mock.Anything).
				Return(nil).Once()
			ch.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
				mock.Anything, mock.Anything).
				Return(amqp.Queue{}, nil).Once()
			ch.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return(nil).Once()
			ch.On("NotifyClose", mock.Anything).
				Return(make(chan *amqp.Error, 10))

			cons, err := harego.NewConsumer(func() (harego.RabbitMQ, error) { return r, nil },
				harego.QueueName(testament.RandomLowerString(10)),
			)
			require.NoError(t, err)

			ch.On("Close").Return(tc.channelErr)
			r.On("Close").Return(tc.connErr)
			err = cons.Close()

			for _, e := range tc.wantErrs {
				assert.ErrorIsf(t, err, e, "want %v, got %v", e, err)
			}
		})
	}
}

func testClientCloseMultipleTimes(t *testing.T) {
	t.Parallel()
	cons, err := harego.NewConsumer(func() (harego.RabbitMQ, error) {
		return &mocks.RabbitMQSimple{}, nil
	}, harego.QueueName(testament.RandomLowerString(10)))
	require.NoError(t, err)

	err = cons.Close()
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		err = cons.Close()
		assert.ErrorIs(t, err, harego.ErrClosed)
	}
}
