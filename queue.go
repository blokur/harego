package harego

import (
	"context"
	"sync"
	"time"

	"github.com/blokur/harego/internal"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

// Exchange is a concurrent safe construct for publishing a message to an
// exchange. It creates multiple workers for safe communication. Zero value is
// not usable.
//nolint:maligned // most likely not an issue, but cleaner this way.
type Exchange struct {
	conn         RabbitMQ
	workers      int
	consumerName string

	// queue properties.
	queueName  string
	routingKey string

	// exchange properties.
	exchName   string
	exchType   ExchangeType
	durable    bool
	autoDelete bool
	autoAck    bool
	internal   bool
	noWait     bool

	// message properties.
	prefetchCount int
	prefetchSize  int
	deliveryMode  DeliveryMode

	pubChBuff int
	pubCh     chan *publishMsg
	channel   Channel
	once      sync.Once
	cancel    func()
	closed    bool
}

type publishMsg struct {
	msg   *amqp.Publishing
	errCh chan error
}

// NewExchange returns an Exchange instance on the default exchange.
func NewExchange(conn *amqp.Connection, conf ...ConfigFunc) (*Exchange, error) {
	ctx, cancel := context.WithCancel(context.Background())
	e := &Exchange{
		conn:          &rabbitWrapper{conn: conn},
		exchName:      "default",
		queueName:     "harego",
		workers:       1,
		exchType:      ExchangeTypeTopic,
		cancel:        cancel,
		deliveryMode:  DeliveryModePersistent,
		prefetchCount: 1,
		prefetchSize:  0,
		durable:       true,
		pubChBuff:     10,
		consumerName:  internal.GetRandomName(),
	}
	for _, c := range conf {
		c(e)
	}
	err := e.validate()
	if err != nil {
		return nil, err
	}
	e.channel, err = e.conn.Channel()
	if err != nil {
		return nil, err
	}
	err = e.channel.ExchangeDeclare(
		e.exchName,
		e.exchType.String(),
		e.durable,
		e.autoDelete,
		e.internal,
		e.noWait,
		nil,
	)
	if err != nil {
		return nil, err
	}
	// to make sure rabbitmq is fair on workers.
	err = e.channel.Qos(e.prefetchCount, e.prefetchSize, true)
	if err != nil {
		return nil, err
	}
	e.pubCh = make(chan *publishMsg, e.pubChBuff)
	err = e.publishWorkers(ctx)
	if err != nil {
		return nil, err
	}
	return e, nil
}

// Publish sends the msg to the queue.
func (e *Exchange) Publish(msg *amqp.Publishing) error {
	if e.closed {
		return ErrClosed
	}
	err := make(chan error)
	e.pubCh <- &publishMsg{
		msg:   msg,
		errCh: err,
	}
	return <-err
}

func (e *Exchange) publishWorkers(ctx context.Context) error {
	err := e.channel.QueueBind(e.queueName, e.routingKey, e.exchName, e.noWait, nil)
	if err != nil {
		return err
	}
	fn := func(msg *amqp.Publishing) error {
		_, err := e.getQueue()
		if err != nil {
			return err
		}
		msg.DeliveryMode = uint8(e.deliveryMode)
		err = e.channel.Publish(e.queueName, e.routingKey, false, false, *msg)
		return err
	}

	go func() {
		for msg := range e.pubCh {
			select {
			case <-ctx.Done():
				return
			default:
			}
			msg.errCh <- fn(msg.msg)
		}
	}()
	return nil
}

func (e *Exchange) getQueue() (amqp.Queue, error) {
	return e.channel.QueueDeclare(
		e.queueName,
		e.durable,
		e.autoDelete,
		false,
		e.noWait,
		nil,
	)
}

// Consume calls the handler on each message and stops handling messages when
// the context is done. If the handler returns false, the message is returned
// back to the queue.
func (e *Exchange) Consume(ctx context.Context, handler HandlerFunc) error {
	if e.closed {
		return ErrClosed
	}

	ctx, e.cancel = context.WithCancel(ctx)
	_, err := e.getQueue()
	if err != nil {
		return err
	}
	msgs, err := e.channel.Consume(
		e.queueName,
		e.consumerName,
		e.autoAck,
		false,
		false,
		e.noWait,
		nil,
	)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	wg.Add(e.workers)
	for i := 0; i < e.workers; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case msg := <-msgs:
					a, delay := handler(msg)
					switch a {
					case AckTypeAck:
						time.Sleep(delay)
						msg.Ack(false)
					case AckTypeNack:
						time.Sleep(delay)
						msg.Nack(false, true)
					case AckTypeReject:
						time.Sleep(delay)
						msg.Reject(false)
						// case AckTypeRequeue:
						// 	msg.Reject(false)
						// 	e.pubCh <- msg
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	wg.Wait()
	return nil
}

// Close closes the channel and the connection.
func (e *Exchange) Close() error {
	e.once.Do(func() {
		e.cancel()
		e.closed = true
	})
	var err *multierror.Error
	if e.channel != nil {
		er := e.channel.Close()
		if er != nil {
			err = multierror.Append(err, er)
		}
		e.channel = nil
	}
	if e.conn != nil {
		er := e.conn.Close()
		if er != nil {
			err = multierror.Append(err, er)
		}
		e.conn = nil
	}
	return err.ErrorOrNil()
}

func (e *Exchange) validate() error {
	if e.conn == nil {
		return errors.Wrap(ErrInput, "empty RabbitMQ connection")
	}
	if e.workers < 1 {
		return errors.Wrapf(ErrInput, "not enough workers: %d", e.workers)
	}
	if e.consumerName == "" {
		return errors.Wrap(ErrInput, "empty consumer name")
	}
	if e.queueName == "" {
		return errors.Wrap(ErrInput, "empty queue name")
	}
	if e.exchName == "" {
		return errors.Wrap(ErrInput, "empty exchange name")
	}
	if !e.exchType.IsValid() {
		return errors.Wrapf(ErrInput, "exchange type: %q", e.exchType.String())
	}
	if e.prefetchCount < 1 {
		return errors.Wrapf(ErrInput, "not enough prefetch count: %d", e.prefetchCount)
	}
	if e.prefetchSize < 0 {
		return errors.Wrapf(ErrInput, "not enough prefetch size: %d", e.prefetchSize)
	}
	if !e.deliveryMode.IsValid() {
		return errors.Wrapf(ErrInput, "delivery mode: %q", e.deliveryMode.String())
	}
	return nil
}
