package harego_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/blokur/testament"

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
	testCases := []struct {
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

	for idx, testCase := range testCases {
		name := fmt.Sprintf("%d_%s", idx, testCase.msg)
		t.Run(name, func(t *testing.T) {
			_, err := harego.NewConsumer(conn,
				testCase.conf...,
			)
			require.Error(t, err)
			assert.Contains(t, err.Error(), testCase.msg)
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
	rabbitCli := mocks.NewRabbitMQ(t)
	channel := mocks.NewChannel(t)

	rabbitCli.On("Channel").Return(channel, nil).Once()
	channel.On("Qos", mock.Anything, mock.Anything, mock.Anything).
		Return(assert.AnError).Once()

	_, err := harego.NewConsumer(func() (harego.RabbitMQ, error) { return rabbitCli, nil },
		harego.QueueName(testament.RandomLowerString(10)),
	)
	assert.ErrorIs(t, err, assert.AnError)
}

func testNewConsumerQueueDeclare(t *testing.T) {
	t.Parallel()
	rabbitCli := mocks.NewRabbitMQ(t)
	channel := mocks.NewChannel(t)

	rabbitCli.On("Channel").Return(channel, nil)
	channel.On("Qos", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	channel.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	channel.On("NotifyClose", mock.Anything).Return(make(chan *amqp.Error, 10))
	channel.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything).
		Return(amqp.Queue{}, assert.AnError).Once()

	_, err := harego.NewConsumer(func() (harego.RabbitMQ, error) { return rabbitCli, nil },
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
	rabbitCli := mocks.NewRabbitMQ(t)
	channel := mocks.NewChannel(t)

	prefetchCount := rand.IntN(9999)
	prefetchSize := rand.IntN(9999)
	queue := testament.RandomString(10)

	rabbitCli.On("Channel").Return(channel, nil)
	channel.On("Qos", prefetchCount, prefetchSize, mock.Anything).
		Return(nil).Once()
	channel.On("QueueDeclare", queue, false, true, mock.Anything,
		true, mock.Anything).
		Return(amqp.Queue{}, nil).Once()
	channel.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	channel.On("NotifyClose", mock.Anything).Return(make(chan *amqp.Error, 10))
	channel.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(assert.AnError).Once()

	_, err := harego.NewConsumer(func() (harego.RabbitMQ, error) { return rabbitCli, nil },
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
	rabbitCli := mocks.NewRabbitMQ(t)
	channel := mocks.NewChannel(t)

	prefetchCount := rand.IntN(9999)
	prefetchSize := rand.IntN(9999)
	queue := testament.RandomString(10)
	args := map[string]interface{}{
		"arg1": "val1",
		"arg2": "val2",
	}

	rabbitCli.On("Channel").Return(channel, nil)
	channel.On("Qos", prefetchCount, prefetchSize, mock.Anything).
		Return(nil).Once()
	channel.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	channel.On("NotifyClose", mock.Anything).Return(make(chan *amqp.Error, 10))
	channel.On("QueueDeclare", queue, false, true, mock.Anything,
		true, mock.MatchedBy(func(a amqp.Table) bool {
			if diff := cmp.Diff(amqp.Table(args), a); diff != "" {
				t.Errorf("(-want +got):\n%s", diff)
			}

			return true
		})).
		Return(amqp.Queue{}, nil).Once()
	channel.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(assert.AnError).Once()

	_, err := harego.NewConsumer(func() (harego.RabbitMQ, error) { return rabbitCli, nil },
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
	t.Run("HandlePanics", testClientConsumeHandlePanics)
}

func testClientConsumeChannelError(t *testing.T) {
	t.Parallel()
	rabbitCli := mocks.NewRabbitMQ(t)
	channel := mocks.NewChannel(t)

	rabbitCli.On("Channel").Return(channel, nil)
	channel.On("Qos", mock.Anything, mock.Anything, mock.Anything).
		Return(nil)
	channel.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	channel.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything).
		Return(amqp.Queue{}, nil).Once()
	channel.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	channel.On("NotifyClose", mock.Anything).Return(make(chan *amqp.Error, 10))

	queueName := testament.RandomString(10)
	consumerName := testament.RandomString(10)
	cons, err := harego.NewConsumer(func() (harego.RabbitMQ, error) { return rabbitCli, nil },
		harego.QueueName(queueName),
		harego.ConsumerName(consumerName),
		harego.NoWait,
	)
	require.NoError(t, err)

	channel.On("Consume",
		queueName,
		consumerName,
		mock.Anything, mock.Anything, mock.Anything,
		true,
		mock.Anything,
	).Return(nil, assert.AnError).Once()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	err = cons.Consume(ctx, func(*amqp.Delivery) (harego.AckType, time.Duration) { return harego.AckTypeAck, 0 })
	assert.ErrorIs(t, err, assert.AnError)
}

func testClientConsumeNilHandler(t *testing.T) {
	t.Parallel()
	rabbitCli := mocks.NewRabbitMQ(t)
	channel := getPassingChannel(t, 1)
	rabbitCli.On("Channel").Return(channel, nil)

	cons, err := harego.NewConsumer(func() (harego.RabbitMQ, error) { return rabbitCli, nil },
		harego.QueueName(testament.RandomString(10)),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	err = cons.Consume(ctx, nil)
	assert.ErrorIs(t, err, harego.ErrNilHnadler)
}

func testClientConsumeCancelledContext(t *testing.T) {
	t.Parallel()
	rabbitCli := mocks.NewRabbitMQ(t)
	channel := mocks.NewChannel(t)

	rabbitCli.On("Channel").Return(channel, nil)
	channel.On("Qos", mock.Anything, mock.Anything, mock.Anything).
		Return(nil)
	channel.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).
		Return(nil)
	channel.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything).
		Return(amqp.Queue{}, nil).Once()
	channel.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	channel.On("NotifyClose", mock.Anything).
		Return(make(chan *amqp.Error, 10))

	cons, err := harego.NewConsumer(func() (harego.RabbitMQ, error) { return rabbitCli, nil },
		harego.QueueName(testament.RandomString(10)),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	delivery := make(chan amqp.Delivery, 1)
	delivery <- amqp.Delivery{Body: []byte("first message")}

	channel.On("Consume", mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Return((<-chan amqp.Delivery)(delivery), nil).Once()

	var wGroup sync.WaitGroup

	wGroup.Add(1)

	err = cons.Consume(ctx, func(d *amqp.Delivery) (harego.AckType, time.Duration) {
		select {
		case <-ctx.Done():
			t.Errorf("didn't expect to receive a call for: %q", d.Body)
			return harego.AckTypeAck, 0
		default:
		}

		cancel()

		delivery <- amqp.Delivery{Body: []byte("second message")}

		wGroup.Done()

		return harego.AckTypeAck, 0
	})
	require.ErrorIs(t, err, context.Canceled)

	assert.Eventually(t, func() bool {
		wGroup.Wait()
		return true
	}, 10*time.Second, 10*time.Millisecond)
}

func testClientConsumeAlreadyClosed(t *testing.T) {
	t.Parallel()
	rabbitCli := mocks.NewRabbitMQ(t)
	channel := getPassingChannel(t, 1)
	rabbitCli.On("Channel").Return(channel, nil)
	rabbitCli.On("Close").Return(nil)

	cons, err := harego.NewConsumer(func() (harego.RabbitMQ, error) { return rabbitCli, nil },
		harego.QueueName(testament.RandomString(10)),
	)
	require.NoError(t, err)

	err = cons.Close()
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	err = cons.Consume(ctx, func(d *amqp.Delivery) (harego.AckType, time.Duration) {
		t.Errorf("didn't expect to receive a call for: %q", d.Body)
		return harego.AckTypeAck, 0
	})
	assert.ErrorIs(t, err, harego.ErrClosed)
}

func testClientConsumeHandlePanics(t *testing.T) {
	t.Parallel()
	t.Run("DefaultLogger", testClientConsumeHandlePanicsDefaultLogger)
	t.Run("WithLogger", testClientConsumeHandlePanicsWithLogger)
}

func testClientConsumeHandlePanicsDefaultLogger(t *testing.T) {
	t.Parallel()
	rabbitcli := mocks.NewRabbitMQ(t)
	channel := getPassingChannel(t, 1)
	rabbitcli.On("Channel").Return(channel, nil)
	rabbitcli.On("Close").Return(nil)

	cons, err := harego.NewConsumer(func() (harego.RabbitMQ, error) { return rabbitcli, nil },
		harego.QueueName(testament.RandomString(10)),
	)
	require.NoError(t, err)

	defer func() {
		err := cons.Close()
		assert.NoError(t, err)
	}()

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	err = cons.Consume(ctx, func(*amqp.Delivery) (harego.AckType, time.Duration) {
		defer cancel()

		panic(assert.AnError)
	})
	assert.ErrorIs(t, err, context.Canceled)
}

func testClientConsumeHandlePanicsWithLogger(t *testing.T) {
	t.Parallel()
	rabbitCli := mocks.NewRabbitMQ(t)
	channel := getPassingChannel(t, 1)
	rabbitCli.On("Channel").Return(channel, nil)
	rabbitCli.On("Close").Return(nil)

	logger := newMockLogger()
	cons, err := harego.NewConsumer(func() (harego.RabbitMQ, error) { return rabbitCli, nil },
		harego.QueueName(testament.RandomString(10)),
		harego.Logger(logger.logger),
	)
	require.NoError(t, err)

	defer func() {
		err := cons.Close()
		assert.NoError(t, err)
	}()

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	cons.Consume(ctx, func(*amqp.Delivery) (harego.AckType, time.Duration) {
		defer cancel()

		panic(assert.AnError)
	})

	logger.isInError(t, assert.AnError)
}

func testClientClose(t *testing.T) {
	t.Parallel()
	t.Run("AlreadyClosed", testClientCloseAlreadyClosed)
	t.Run("Errors", testClientCloseErrors)
	t.Run("MultipleTimes", testClientCloseMultipleTimes)
}

func testClientCloseAlreadyClosed(t *testing.T) {
	t.Parallel()
	rabbitCli := mocks.NewRabbitMQ(t)
	channel := getPassingChannel(t, 1)
	rabbitCli.On("Channel").Return(channel, nil)
	rabbitCli.On("Close").Return(nil)

	cons, err := harego.NewConsumer(func() (harego.RabbitMQ, error) { return rabbitCli, nil },
		harego.QueueName(testament.RandomLowerString(10)),
	)
	require.NoError(t, err)

	err = cons.Close()
	require.NoError(t, err)

	err = cons.Close()
	assert.ErrorIs(t, err, harego.ErrClosed)
}

func testClientCloseErrors(t *testing.T) {
	t.Parallel()

	err1 := errors.New(testament.RandomString(10))
	err2 := errors.New(testament.RandomString(10))

	testCases := map[string]struct {
		channelErr error
		connErr    error
		wantErrs   []error
	}{
		"no error":    {nil, nil, nil},
		"chan error":  {err1, nil, []error{err1}},
		"conn error":  {nil, err1, []error{err1}},
		"both errors": {err1, err2, []error{err1, err2}},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			rabbitCli := mocks.NewRabbitMQ(t)
			channel := mocks.NewChannel(t)

			rabbitCli.On("Channel").Return(channel, nil)
			channel.On("Qos", mock.Anything, mock.Anything, mock.Anything).
				Return(nil)
			channel.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
				mock.Anything, mock.Anything, mock.Anything).
				Return(nil).Once()
			channel.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
				mock.Anything, mock.Anything).
				Return(amqp.Queue{}, nil).Once()
			channel.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return(nil).Once()
			channel.On("NotifyClose", mock.Anything).
				Return(make(chan *amqp.Error, 10))

			cons, err := harego.NewConsumer(func() (harego.RabbitMQ, error) { return rabbitCli, nil },
				harego.QueueName(testament.RandomLowerString(10)),
			)
			require.NoError(t, err)

			channel.On("Close").Return(testCase.channelErr)
			rabbitCli.On("Close").Return(testCase.connErr)

			err = cons.Close()

			for _, e := range testCase.wantErrs {
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

	for range 100 {
		err = cons.Close()
		assert.ErrorIs(t, err, harego.ErrClosed)
	}
}
