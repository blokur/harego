package harego_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/blokur/harego/v2"
	"github.com/blokur/harego/v2/mocks"
	"github.com/blokur/testament"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestNewPublisher(t *testing.T) {
	t.Parallel()
	t.Run("BadInput", testNewPublisherBadInput)
	t.Run("Channel", testNewPublisherChannel)
	t.Run("ExchangeDeclare", testNewPublisherExchangeDeclare)
}

func testNewPublisherBadInput(t *testing.T) {
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
		{"exchange name", []harego.ConfigFunc{
			harego.ExchangeName(""),
		}},
		{"exchange type", []harego.ConfigFunc{
			harego.WithExchangeType(-1),
		}},
		{"exchange type", []harego.ConfigFunc{
			harego.WithExchangeType(9999999),
		}},
	}

	for i, tc := range tcs {
		tc := tc
		name := fmt.Sprintf("%d_%s", i, tc.msg)
		t.Run(name, func(t *testing.T) {
			_, err := harego.NewPublisher(conn,
				tc.conf...,
			)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.msg)
		})
	}

	_, err := harego.NewPublisher(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection")

	_, err = harego.NewPublisher(func() (harego.RabbitMQ, error) {
		return nil, assert.AnError
	})
	require.Error(t, err)
	testament.AssertInError(t, err, assert.AnError)
}

func testNewPublisherChannel(t *testing.T) {
	t.Parallel()
	r := mocks.NewRabbitMQ(t)
	r.On("Channel").Return(nil, assert.AnError).Once()
	r.On("Close").Return(nil).Once()

	_, err := harego.NewPublisher(func() (harego.RabbitMQ, error) { return r, nil })
	testament.AssertInError(t, err, assert.AnError)
}

func testNewPublisherExchangeDeclare(t *testing.T) {
	t.Parallel()
	r := mocks.NewRabbitMQ(t)
	ch := mocks.NewChannel(t)

	r.On("Channel").Return(ch, nil).Once()
	r.On("Close").Return(nil).Once()
	ch.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).
		Return(assert.AnError).Once()

	_, err := harego.NewPublisher(func() (harego.RabbitMQ, error) { return r, nil })
	testament.AssertInError(t, err, assert.AnError)
}

func TestPublisher(t *testing.T) {
	t.Parallel()
	t.Run("Publish", testPublisherPublish)
	t.Run("Close", testPublisherClose)
}

func testPublisherPublish(t *testing.T) {
	t.Parallel()
	t.Run("AlreadyClosed", testPublisherPublishAlreadyClosed)
	t.Run("PublishError", testPublisherPublishPublishError)
}

func testPublisherPublishAlreadyClosed(t *testing.T) {
	t.Parallel()
	r := mocks.NewRabbitMQ(t)
	ch := mocks.NewChannel(t)

	r.On("Channel").Return(ch, nil).Once()
	ch.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("NotifyClose", mock.Anything).
		Return(make(chan *amqp.Error, 10)).Once()

	pub, err := harego.NewPublisher(func() (harego.RabbitMQ, error) { return r, nil })
	require.NoError(t, err)

	ch.On("Close").Return(nil).Once()
	r.On("Close").Return(nil).Once()
	err = pub.Close()
	require.NoError(t, err)

	err = pub.Publish(&amqp.Publishing{})
	assert.ErrorIs(t, err, harego.ErrClosed)
}

func testPublisherPublishPublishError(t *testing.T) {
	t.Parallel()
	r := mocks.NewRabbitMQ(t)
	ch := mocks.NewChannel(t)

	r.On("Channel").Return(ch, nil).Once()

	exchName := testament.RandomString(10)
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

	pub, err := harego.NewPublisher(func() (harego.RabbitMQ, error) { return r, nil },
		harego.ExchangeName(exchName),
		harego.WithExchangeType(exchType),
		harego.AutoDelete,
		harego.Internal,
		harego.NoWait,
	)
	require.NoError(t, err)

	ch.On("Publish", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(assert.AnError).Once()

	err = pub.Publish(&amqp.Publishing{})
	testament.AssertInError(t, err, assert.AnError)
}

func testPublisherClose(t *testing.T) {
	t.Parallel()
	t.Run("AlreadyClosed", testPublisherCloseAlreadyClosed)
	t.Run("Errors", testPublisherCloseErrors)
	t.Run("MultipleTimes", testPublisherCloseMultipleTimes)
	t.Run("PublishNotPanic", testPublisherClosePublishNotPanic)
}

func testPublisherCloseAlreadyClosed(t *testing.T) {
	t.Parallel()
	r := mocks.NewRabbitMQ(t)
	ch := mocks.NewChannel(t)

	r.On("Channel").Return(ch, nil).Once()
	ch.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	ch.On("NotifyClose", mock.Anything).
		Return(make(chan *amqp.Error, 10)).Once()

	pub, err := harego.NewPublisher(func() (harego.RabbitMQ, error) { return r, nil })
	require.NoError(t, err)

	ch.On("Close").Return(nil)
	r.On("Close").Return(nil)
	err = pub.Close()
	require.NoError(t, err)

	err = pub.Close()
	testament.AssertInError(t, err, harego.ErrClosed)
}

func testPublisherCloseErrors(t *testing.T) {
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

			r.On("Channel").Return(ch, nil).Once()
			ch.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
				mock.Anything, mock.Anything, mock.Anything).
				Return(nil).Once()
			ch.On("NotifyClose", mock.Anything).
				Return(make(chan *amqp.Error, 10)).Once()

			pub, err := harego.NewPublisher(func() (harego.RabbitMQ, error) { return r, nil })
			require.NoError(t, err)

			ch.On("Close").Return(tc.channelErr)
			r.On("Close").Return(tc.connErr)
			err = pub.Close()

			for _, e := range tc.wantErrs {
				testament.AssertInError(t, err, e)
			}
		})
	}
}

func testPublisherCloseMultipleTimes(t *testing.T) {
	t.Parallel()
	pub, err := harego.NewPublisher(func() (harego.RabbitMQ, error) {
		return &mocks.RabbitMQSimple{}, nil
	})
	require.NoError(t, err)

	err = pub.Close()
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		err = pub.Close()
		testament.AssertInError(t, err, harego.ErrClosed)
	}
}

func testPublisherClosePublishNotPanic(t *testing.T) {
	t.Parallel()
	r := &mocks.RabbitMQSimple{}

	total := 100
	pub, err := harego.NewPublisher(func() (harego.RabbitMQ, error) { return r, nil },
		harego.Workers(total),
	)
	require.NoError(t, err)

	var wg sync.WaitGroup
	for i := 0; i < total-1; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NotPanics(t, func() {
				pub.Publish(&amqp.Publishing{})
			})
		}()
	}

	err = pub.Close()
	require.NoError(t, err)
	assert.Eventually(t, func() bool {
		wg.Wait()
		return true
	}, 120*time.Second, 10*time.Millisecond)
}
