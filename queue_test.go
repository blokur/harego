package harego_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/blokur/harego"
	"github.com/blokur/harego/mocks"
	"github.com/blokur/testament"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestNewClient(t *testing.T) {
	t.Run("BadInput", testNewClientBadInput)
	t.Run("Channel", testNewClientChannel)
	t.Run("Qos", testNewClientQos)
	t.Run("QueueDeclare", testNewClientQueueDeclare)
	t.Run("ExchangeDeclare", testNewClientExchangeDeclare)
	t.Run("QueueBind", testNewClientQueueBind)
}

func testNewClientBadInput(t *testing.T) {
	t.Parallel()
	conn := harego.Connection(&mocks.RabbitMQ{})
	tcs := []struct {
		msg  string
		conf []harego.ConfigFunc
	}{
		{"connection", []harego.ConfigFunc{}},
		{"workers", []harego.ConfigFunc{
			conn,
			harego.Workers(0),
		}},
		{"consumer name", []harego.ConfigFunc{
			conn,
			harego.ConsumerName(""),
		}},
		{"queue name", []harego.ConfigFunc{
			conn,
			harego.QueueName(""),
		}},
		{"exchange name", []harego.ConfigFunc{
			conn,
			harego.ExchangeName(""),
		}},
		{"exchange type", []harego.ConfigFunc{
			conn,
			harego.WithExchangeType(-1),
		}},
		{"exchange type", []harego.ConfigFunc{
			conn,
			harego.WithExchangeType(9999999),
		}},
		{"prefetch count", []harego.ConfigFunc{
			conn,
			harego.PrefetchCount(0),
		}},
		{"prefetch size", []harego.ConfigFunc{
			conn,
			harego.PrefetchSize(-1),
		}},
		{"delivery mode", []harego.ConfigFunc{
			conn,
			harego.WithDeliveryMode(10),
		}},
	}
	for i, tc := range tcs {
		tc := tc
		name := fmt.Sprintf("%d_%s", i, tc.msg)
		t.Run(name, func(t *testing.T) {
			_, err := harego.NewClient("",
				tc.conf...,
			)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.msg)
		})
	}
}

func testNewClientChannel(t *testing.T) {
	t.Parallel()
	r := &mocks.RabbitMQ{}
	defer r.AssertExpectations(t)
	r.On("Channel").Return(nil, assert.AnError).Once()
	_, err := harego.NewClient("",
		harego.Connection(r),
	)
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

	_, err := harego.NewClient("",
		harego.Connection(r),
	)
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
	ch.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything).
		Return(amqp.Queue{}, assert.AnError).Once()

	_, err := harego.NewClient("",
		harego.Connection(r),
	)
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
	ch.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything).
		Return(amqp.Queue{}, nil).Once()
	ch.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).
		Return(assert.AnError).Once()

	_, err := harego.NewClient("",
		harego.Connection(r),
	)
	testament.AssertInError(t, err, assert.AnError)
}

func testNewClientQueueBind(t *testing.T) {
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
	ch.On("QueueDeclare", queue, true, true, mock.Anything,
		true, mock.Anything).
		Return(amqp.Queue{}, nil).Once()
	ch.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(assert.AnError).Once()

	_, err := harego.NewClient("",
		harego.Connection(r),
		harego.PrefetchCount(prefetchCount),
		harego.PrefetchSize(prefetchSize),
		harego.QueueName(queue),
		harego.Durable,
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
	ch.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything).
		Return(amqp.Queue{}, nil).Once()
	ch.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("NotifyClose", mock.Anything).
		Return(make(chan *amqp.Error, 10)).Once()

	cl, err := harego.NewClient("",
		harego.Connection(r),
	)
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
	ch.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything).
		Return(amqp.Queue{}, nil).Once()

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
	ch.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("NotifyClose", mock.Anything).
		Return(make(chan *amqp.Error, 10)).Once()

	cl, err := harego.NewClient("",
		harego.Connection(r),
		harego.ExchangeName(exchName),
		harego.WithExchangeType(exchType),
		harego.Durable,
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
	t.Run("ChannelError", testClientConsumeChannelError)
	t.Run("NilHandler", testClientConsumeNilHandler)
	t.Run("CancelledContext", testClientConsumeCancelledContext)
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
	ch.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything).
		Return(amqp.Queue{}, nil).Once()
	ch.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("NotifyClose", mock.Anything).
		Return(make(chan *amqp.Error, 10)).Once()

	queueName := randomString(10)
	consumerName := randomString(10)
	cl, err := harego.NewClient("",
		harego.Connection(r),
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
	ch.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything).
		Return(amqp.Queue{}, nil).Once()
	ch.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("NotifyClose", mock.Anything).
		Return(make(chan *amqp.Error, 10)).Once()

	cl, err := harego.NewClient("",
		harego.Connection(r),
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
	ch.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything).
		Return(amqp.Queue{}, nil).Once()
	ch.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("NotifyClose", mock.Anything).
		Return(make(chan *amqp.Error, 10)).Once()

	cl, err := harego.NewClient("",
		harego.Connection(r),
	)
	require.NoError(t, err)

	delivery := make(chan amqp.Delivery)
	go func() {
		delivery <- amqp.Delivery{Body: []byte("first message")}
	}()

	ch.On("Consume", mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Return((<-chan amqp.Delivery)(delivery), nil).Once() //nolint:gocritic // otherwise the mock breaks.

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = cl.Consume(ctx, func(d *amqp.Delivery) (a harego.AckType, delay time.Duration) {
		select {
		case <-ctx.Done():
			t.Errorf("didn't expect to receive a call for: %q", d.Body)
			return 0, 0
		default:
		}
		cancel()
		go func() {
			delivery <- amqp.Delivery{Body: []byte("second message")}
		}()
		return 0, 0
	})
	testament.AssertInError(t, err, context.Canceled)

	assert.Eventually(t, func() bool {
		// the message should not be read from the handler,  and when it is not read,
		// it needs a release.
		<-delivery
		return true
	}, 10*time.Second, time.Millisecond)
}

func testClientClose(t *testing.T) {
	t.Run("AlreadyClosed", testClientCloseAlreadyClosed)
	t.Run("Errors", testClientCloseErrors)
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
	ch.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything).
		Return(amqp.Queue{}, nil).Once()
	ch.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("NotifyClose", mock.Anything).
		Return(make(chan *amqp.Error, 10)).Once()

	cl, err := harego.NewClient("",
		harego.Connection(r),
	)
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
			ch.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
				mock.Anything, mock.Anything).
				Return(amqp.Queue{}, nil).Once()
			ch.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
				mock.Anything, mock.Anything, mock.Anything).
				Return(nil).Once()
			ch.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return(nil).Once()
			ch.On("NotifyClose", mock.Anything).
				Return(make(chan *amqp.Error, 10)).Once()

			cl, err := harego.NewClient("",
				harego.Connection(r),
			)
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
