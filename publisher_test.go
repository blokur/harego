package harego_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/blokur/testament"

	"github.com/blokur/harego/v2"
	"github.com/blokur/harego/v2/mocks"
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
	testCases := []struct {
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

	for i, testCase := range testCases {
		name := fmt.Sprintf("%d_%s", i, testCase.msg)
		t.Run(name, func(t *testing.T) {
			_, err := harego.NewPublisher(conn,
				testCase.conf...,
			)
			require.Error(t, err)
			assert.Contains(t, err.Error(), testCase.msg)
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
	rabbitCli := mocks.NewRabbitMQ(t)
	ch := mocks.NewChannel(t)

	rabbitCli.On("Channel").Return(ch, nil).Once()
	rabbitCli.On("Close").Return(nil).Once()
	ch.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).
		Return(assert.AnError).Once()

	_, err := harego.NewPublisher(func() (harego.RabbitMQ, error) { return rabbitCli, nil })
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
	rabbitCli := mocks.NewRabbitMQ(t)
	channel := mocks.NewChannel(t)

	rabbitCli.On("Channel").Return(channel, nil).Once()
	channel.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	channel.On("NotifyClose", mock.Anything).
		Return(make(chan *amqp.Error, 10)).Once()

	pub, err := harego.NewPublisher(func() (harego.RabbitMQ, error) { return rabbitCli, nil })
	require.NoError(t, err)

	channel.On("Close").Return(nil).Once()
	rabbitCli.On("Close").Return(nil).Once()

	err = pub.Close()
	require.NoError(t, err)

	err = pub.Publish(&amqp.Publishing{})
	assert.ErrorIs(t, err, harego.ErrClosed)
}

func testPublisherPublishPublishError(t *testing.T) {
	t.Parallel()
	rabbitCli := mocks.NewRabbitMQ(t)
	channel := mocks.NewChannel(t)

	rabbitCli.On("Channel").Return(channel, nil).Once()

	exchName := testament.RandomString(10)
	exchType := harego.ExchangeTypeFanout

	channel.On("ExchangeDeclare",
		mock.MatchedBy(func(name string) bool {
			assert.Contains(t, name, exchName)
			return true
		}),
		exchType.String(),
		true, true, true, true, mock.Anything,
	).Return(nil).Once()

	channel.On("NotifyClose", mock.Anything).
		Return(make(chan *amqp.Error, 10)).Once()

	pub, err := harego.NewPublisher(func() (harego.RabbitMQ, error) { return rabbitCli, nil },
		harego.ExchangeName(exchName),
		harego.WithExchangeType(exchType),
		harego.AutoDelete,
		harego.Internal,
		harego.NoWait,
	)
	require.NoError(t, err)

	channel.On("Publish", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
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
	rabbitCli := mocks.NewRabbitMQ(t)
	channel := mocks.NewChannel(t)

	rabbitCli.On("Channel").Return(channel, nil).Once()
	channel.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()
	channel.On("NotifyClose", mock.Anything).
		Return(make(chan *amqp.Error, 10)).Once()

	pub, err := harego.NewPublisher(func() (harego.RabbitMQ, error) { return rabbitCli, nil })
	require.NoError(t, err)

	channel.On("Close").Return(nil)
	rabbitCli.On("Close").Return(nil)

	err = pub.Close()
	require.NoError(t, err)

	err = pub.Close()
	testament.AssertInError(t, err, harego.ErrClosed)
}

func testPublisherCloseErrors(t *testing.T) {
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

			rabbitCli.On("Channel").Return(channel, nil).Once()
			channel.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
				mock.Anything, mock.Anything, mock.Anything).
				Return(nil).Once()
			channel.On("NotifyClose", mock.Anything).
				Return(make(chan *amqp.Error, 10)).Once()

			pub, err := harego.NewPublisher(func() (harego.RabbitMQ, error) { return rabbitCli, nil })
			require.NoError(t, err)

			channel.On("Close").Return(testCase.channelErr)
			rabbitCli.On("Close").Return(testCase.connErr)

			err = pub.Close()

			for _, e := range testCase.wantErrs {
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

	for range 100 {
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

	var wGroup sync.WaitGroup
	for range total - 1 {
		wGroup.Add(1)

		go func() {
			defer wGroup.Done()

			assert.NotPanics(t, func() {
				pub.Publish(&amqp.Publishing{})
			})
		}()
	}

	err = pub.Close()
	require.NoError(t, err)
	assert.Eventually(t, func() bool {
		wGroup.Wait()
		return true
	}, 120*time.Second, 10*time.Millisecond)
}
