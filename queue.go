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

// Client is a concurrent safe construct for publishing a message to exchanges,
// and consuming messages from queues. It creates multiple workers for safe
// communication. Zero value is not usable.
// nolint:maligned // most likely not an issue, but cleaner this way.
type Client struct {
	connector    Connector
	workers      int
	consumerName string
	retryDelay   time.Duration

	mu      sync.RWMutex
	conn    RabbitMQ
	channel Channel
	queue   amqp.Queue

	// queue properties.
	queueName  string
	routingKey string
	exclusive  bool

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
	consumeCh chan amqp.Delivery // used for consuming messages and setup Consume loop.
	once      sync.Once
	ctx       context.Context // for turning off the client.
	cancel    func()
	closed    bool
	started   bool
}

type publishMsg struct {
	msg   *amqp.Publishing
	errCh chan error
}

// NewClient returns a Client capable of publishing and consuming messages. The
// default Client config uses the "default" exchange of the "topic" type, both
// exchange and queues will be marked as "durable", messages will be
// persistent, and the consumer gets a random name. The connector value should
// provide a live connection. The connector value is used during reconnection
// process.
func NewClient(connector Connector, conf ...ConfigFunc) (*Client, error) {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Client{
		connector:    connector,
		exchName:     "default",
		workers:      1,
		exchType:     ExchangeTypeTopic,
		deliveryMode: DeliveryModePersistent,
		durable:      true,
		pubChBuff:    10,
		consumerName: internal.GetRandomName(),
		retryDelay:   100 * time.Millisecond,
		ctx:          ctx,
		cancel:       cancel,
	}
	for _, cnf := range conf {
		err := cnf(c)
		if err != nil {
			return nil, err
		}
	}
	if c.prefetchCount < c.workers {
		c.prefetchCount = c.workers
	}
	err := c.validate()
	if err != nil {
		return nil, errors.Wrap(err, "validating configuration")
	}
	c.conn, err = c.connector()
	if err != nil {
		return nil, errors.Wrap(err, "getting a connection to the broker")
	}
	err = c.setupChannel()
	if err != nil {
		return nil, errors.Wrap(err, "setting up a channel")
	}
	err = c.setupQueue()
	if err != nil {
		return nil, errors.Wrap(err, "setting up a queue")
	}

	if c.pubChBuff == 0 {
		c.pubChBuff = 1
	}
	c.pubCh = make(chan *publishMsg, c.workers*c.pubChBuff)
	for i := 0; i < c.workers; i++ {
		c.publishWorker(ctx)
	}
	c.registerReconnect(ctx)
	c.started = true
	return c, nil
}

// Publish sends the msg to the broker via the next available workers.
func (c *Client) Publish(msg *amqp.Publishing) error {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return ErrClosed
	}
	c.mu.RUnlock()
	err := make(chan error)
	c.pubCh <- &publishMsg{
		msg:   msg,
		errCh: err,
	}
	return <-err
}

func (c *Client) publishWorker(ctx context.Context) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	go func() {
		for msg := range c.pubCh {
			select {
			case <-ctx.Done():
				msg.errCh <- ctx.Err()
				return
			default:
			}
			msg.msg.DeliveryMode = uint8(c.deliveryMode)
			c.mu.RLock()
			if c.channel == nil {
				c.mu.RUnlock()
				msg.errCh <- ErrClosed
				return
			}
			err := c.channel.Publish(
				c.exchName,
				c.routingKey,
				false,
				false,
				*msg.msg,
			)
			c.mu.RUnlock()
			msg.errCh <- errors.Wrap(err, "publishing message")
		}
	}()
}

// Consume is a bloking call that passes each message to the handler and stops
// handling messages when the context is done. If the handler returns false,
// the message is returned back to the queue. If the context is cancelled, the
// Client remains operational but no messages will be deliverd to this handler.
// Consume returns an error if you don't specify a queue name.
func (c *Client) Consume(ctx context.Context, handler HandlerFunc) error {
	if c.closed {
		return ErrClosed
	}
	if handler == nil {
		return ErrNilHnadler
	}
	if c.queueName == "" {
		return errors.Wrap(ErrInput, "empty queue name")
	}

	c.mu.Lock()
	c.ctx, c.cancel = context.WithCancel(ctx)
	// consumeCh also signals we are in Consume mode.
	c.consumeCh = make(chan amqp.Delivery, c.workers*c.pubChBuff)
	c.mu.Unlock()
	err := c.setupConsumeCh(c.ctx)
	if err != nil {
		return errors.Wrap(err, "setting up consume process")
	}

	go func() {
		<-c.ctx.Done()
		close(c.consumeCh)
	}()
	var wg sync.WaitGroup
	wg.Add(c.workers)
	for i := 0; i < c.workers; i++ {
		go func() {
			defer wg.Done()
			c.consumeLoop(handler)
		}()
	}
	wg.Wait()
	return c.ctx.Err()
}

func (c *Client) consumeLoop(handler HandlerFunc) {
	for msg := range c.consumeCh {
		msg := msg
		a, delay := handler(&msg)
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
		case AckTypeRequeue:
			time.Sleep(delay)
			err := c.Publish(&amqp.Publishing{
				Body:            msg.Body,
				Headers:         msg.Headers,
				ContentType:     msg.ContentType,
				ContentEncoding: msg.ContentEncoding,
				DeliveryMode:    msg.DeliveryMode,
				Priority:        msg.Priority,
				CorrelationId:   msg.CorrelationId,
				ReplyTo:         msg.ReplyTo,
				Expiration:      msg.Expiration,
				MessageId:       msg.MessageId,
				Timestamp:       msg.Timestamp,
				Type:            msg.Type,
				UserId:          msg.UserId,
				AppId:           msg.AppId,
			})
			if err != nil {
				msg.Nack(false, true)
				continue
			}
			msg.Ack(false)
		}
	}
}

// Close closes the channel and the connection. A closed client is not usable.
func (c *Client) Close() error {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return ErrClosed
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()
	c.once.Do(func() {
		c.closed = true
		c.cancel()
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

func (c *Client) validate() error {
	if c.connector == nil {
		return errors.Wrap(ErrInput, "empty connection function (Connector)")
	}
	if c.workers < 1 {
		return errors.Wrapf(ErrInput, "not enough workers: %d", c.workers)
	}
	if c.consumerName == "" {
		return errors.Wrap(ErrInput, "empty consumer name")
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

func (c *Client) registerReconnect(ctx context.Context) {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return
	}
	c.mu.RUnlock()
	ch := c.channel.NotifyClose(make(chan *amqp.Error))
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-ch:
			if c.closed {
				return
			}
			c.mu.Lock()
			defer c.mu.Unlock()
			if c.channel != nil {
				c.channel.Close()
				c.channel = nil
			}
			if c.conn != nil {
				c.conn.Close()
				c.conn = nil
			}
			c.keepConnecting()
			go c.registerReconnect(ctx)
		}
	}()
}

func (c *Client) keepConnecting() {
	channel := func() error {
		var err error
		c.channel, err = c.conn.Channel()
		return errors.Wrap(err, "opening a new channel")
	}
	var err error
	for {
		time.Sleep(c.retryDelay)
		if c.closed {
			return
		}
		err = c.dial()
		if err != nil {
			continue
		}
		err = channel()
		if err != nil {
			continue
		}
		err = c.setupChannel()
		if err != nil {
			continue
		}
		err = c.setupQueue()
		if err != nil {
			continue
		}
		err = c.setupConsumeCh(c.ctx)
		if err != nil {
			continue
		}
		return
	}
}

func (c *Client) dial() error {
	// already reconnected
	if c.conn != nil {
		return nil
	}
	conn, err := c.connector()
	if err != nil {
		return errors.Wrap(err, "getting a connection to the broker")
	}
	c.conn = conn
	return nil
}

func (c *Client) setupChannel() error {
	var err error
	c.channel, err = c.conn.Channel()
	if err != nil {
		return errors.Wrap(err, "creating channel")
	}
	// to make sure rabbitmq is fair on workers.
	err = c.channel.Qos(c.prefetchCount, c.prefetchSize, true)
	if err != nil {
		return errors.Wrap(err, "setting Qos")
	}

	err = c.channel.ExchangeDeclare(
		c.exchName,
		c.exchType.String(),
		c.durable,
		c.autoDelete,
		c.internal,
		c.noWait,
		nil,
	)
	return errors.Wrap(err, "declaring exchange")
}

func (c *Client) setupQueue() error {
	if c.queueName == "" {
		return nil
	}
	var err error
	c.queue, err = c.channel.QueueDeclare(
		c.queueName,
		c.durable,
		c.autoDelete,
		c.exclusive,
		c.noWait,
		nil,
	)
	if err != nil {
		return errors.Wrap(err, "declaring queue")
	}
	err = c.channel.QueueBind(
		c.queueName,
		c.routingKey,
		c.exchName,
		c.noWait,
		nil,
	)
	return errors.Wrap(err, "binding queue")
}

func (c *Client) setupConsumeCh(ctx context.Context) error {
	if c.consumeCh == nil {
		return nil
	}
	msgs, err := c.channel.Consume(
		c.queueName,
		c.consumerName,
		c.autoAck,
		c.exclusive,
		false,
		c.noWait,
		nil,
	)
	if err != nil {
		return errors.Wrap(err, "getting consume channel")
	}
	go func() {
		for msg := range msgs {
			select {
			case <-ctx.Done():
				return
			default:
			}
			c.consumeCh <- msg
		}
	}()
	return nil
}
