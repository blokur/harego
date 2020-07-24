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

// Client is a concurrent safe construct for publishing a message to an
// exchange. It creates multiple workers for safe communication. Zero value is
// not usable.
//nolint:maligned // most likely not an issue, but cleaner this way.
type Client struct {
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
	queue     amqp.Queue
	once      sync.Once
	cancel    func()
	closed    bool
}

type publishMsg struct {
	msg   *amqp.Publishing
	errCh chan error
}

// NewClient returns an Client instance on the default exchange.
func NewClient(conn *amqp.Connection, conf ...ConfigFunc) (*Client, error) {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Client{
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
	for _, cnf := range conf {
		cnf(c)
	}
	err := c.validate()
	if err != nil {
		return nil, errors.Wrap(err, "validating configuration")
	}
	c.channel, err = c.conn.Channel()
	if err != nil {
		return nil, errors.Wrap(err, "creating channel")
	}
	// to make sure rabbitmq is fair on workers.
	err = c.channel.Qos(c.prefetchCount, c.prefetchSize, true)
	if err != nil {
		return nil, errors.Wrap(err, "setting Qos")
	}
	c.queue, err = c.channel.QueueDeclare(
		c.queueName,
		c.durable,
		c.autoDelete,
		false,
		c.noWait,
		nil,
	)
	if err != nil {
		return nil, errors.Wrap(err, "declaring queue")
	}
	c.pubCh = make(chan *publishMsg, c.pubChBuff)
	err = c.publishWorkers(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "starting workers")
	}
	return c, nil
}

// Publish sends the msg to the queue.
func (c *Client) Publish(msg *amqp.Publishing) error {
	if c.closed {
		return ErrClosed
	}
	err := make(chan error)
	c.pubCh <- &publishMsg{
		msg:   msg,
		errCh: err,
	}
	return <-err
}

func (c *Client) publishWorkers(ctx context.Context) error {
	err := c.channel.ExchangeDeclare(
		c.exchName,
		c.exchType.String(),
		c.durable,
		c.autoDelete,
		c.internal,
		c.noWait,
		nil,
	)
	if err != nil {
		return errors.Wrap(err, "declaring exchange")
	}
	err = c.channel.QueueBind(c.queueName, c.routingKey, c.exchName, c.noWait, nil)
	if err != nil {
		return errors.Wrap(err, "binding queue")
	}
	fn := func(msg *amqp.Publishing) error {
		msg.DeliveryMode = uint8(c.deliveryMode)
		err = c.channel.Publish(c.exchName, c.routingKey, false, false, *msg)
		return errors.Wrap(err, "publishing message")
	}

	go func() {
		for msg := range c.pubCh {
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

// Consume is a bloking call that passes each message to the handler and stops
// handling messages when the context is done. If the handler returns false, the
// message is returned back to the queue. If the context is cancelled, the
// Client remains operational but no messages will be deliverd to this
// handler.
func (c *Client) Consume(ctx context.Context, handler HandlerFunc) error {
	if c.closed {
		return ErrClosed
	}
	ctx, c.cancel = context.WithCancel(ctx)
	msgs, err := c.channel.Consume(
		c.queueName,
		c.consumerName,
		c.autoAck,
		false,
		false,
		c.noWait,
		nil,
	)
	if err != nil {
		return errors.Wrap(err, "getting consume channel")
	}
	var wg sync.WaitGroup
	wg.Add(c.workers)
	for i := 0; i < c.workers; i++ {
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
						// 	c.pubCh <- msg
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	wg.Wait()
	return ctx.Err()
}

// Close closes the channel and the connection.
func (c *Client) Close() error {
	c.once.Do(func() {
		c.cancel()
		c.closed = true
	})
	var err *multierror.Error
	if c.channel != nil {
		er := c.channel.Close()
		if er != nil {
			err = multierror.Append(err, er)
		}
		c.channel = nil
	}
	if c.conn != nil {
		er := c.conn.Close()
		if er != nil {
			err = multierror.Append(err, er)
		}
		c.conn = nil
	}
	return err.ErrorOrNil()
}

// func (c *Client) Clone()

func (c *Client) validate() error {
	if c.conn == nil {
		return errors.Wrap(ErrInput, "empty RabbitMQ connection")
	}
	if c.workers < 1 {
		return errors.Wrapf(ErrInput, "not enough workers: %d", c.workers)
	}
	if c.consumerName == "" {
		return errors.Wrap(ErrInput, "empty consumer name")
	}
	if c.queueName == "" {
		return errors.Wrap(ErrInput, "empty queue name")
	}
	if c.exchName == "" {
		return errors.Wrap(ErrInput, "empty exchange name")
	}
	if !c.exchType.IsValid() {
		return errors.Wrapf(ErrInput, "exchange type: %q", c.exchType.String())
	}
	if c.prefetchCount < 1 {
		return errors.Wrapf(ErrInput, "not enough prefetch count: %d", c.prefetchCount)
	}
	if c.prefetchSize < 0 {
		return errors.Wrapf(ErrInput, "not enough prefetch size: %d", c.prefetchSize)
	}
	if !c.deliveryMode.IsValid() {
		return errors.Wrapf(ErrInput, "delivery mode: %q", c.deliveryMode.String())
	}
	return nil
}
