package harego

import (
	"context"
	"strconv"
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
	url          string
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
	once      sync.Once
	cancel    func()
	closed    bool
}

type publishMsg struct {
	msg   *amqp.Publishing
	errCh chan error
}

// NewClient returns an Client instance on the default exchange. You should
// provide a valid url for reconnection. If you pass a Connection config
// function, it will initiate it for the first time, not when reconnecting; make
// sure you also provide a valid url as well.
func NewClient(url string, conf ...ConfigFunc) (*Client, error) {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Client{
		url:           url,
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
		retryDelay:    100 * time.Millisecond,
	}
	for _, cnf := range conf {
		cnf(c)
	}
	err := c.validate()
	if err != nil {
		return nil, errors.Wrap(err, "validating configuration")
	}
	if c.conn == nil {
		conn, err := amqp.Dial(url)
		if err != nil {
			return nil, errors.Wrapf(err, "dialling %q", url)
		}
		c.conn = &rabbitWrapper{conn}
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
	if c.pubChBuff == 0 {
		c.pubChBuff = 1
	}
	c.pubCh = make(chan *publishMsg, c.workers*c.pubChBuff)
	for i := 0; i < c.workers; i++ {
		err = c.publishWorker(ctx, i)
		if err != nil {
			return nil, errors.Wrap(err, "starting workers")
		}
	}
	c.registerReconnect(ctx)
	return c, nil
}

// Publish sends the msg to the broker on one of the workers.
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

func (c *Client) publishWorker(ctx context.Context, i int) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	exchName := c.exchName + "_" + strconv.Itoa(i)
	err := c.channel.ExchangeDeclare(
		exchName,
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
	err = c.channel.QueueBind(
		c.queueName,
		c.routingKey,
		exchName,
		c.noWait,
		nil,
	)
	if err != nil {
		return errors.Wrap(err, "binding queue")
	}
	go func() {
		for msg := range c.pubCh {
			select {
			case <-ctx.Done():
				return
			default:
			}
			msg.msg.DeliveryMode = uint8(c.deliveryMode)
			c.mu.RLock()
			err = c.channel.Publish(
				exchName,
				c.routingKey,
				false,
				false,
				*msg.msg,
			)
			c.mu.RUnlock()
			msg.errCh <- errors.Wrap(err, "publishing message")
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
	if handler == nil {
		return ErrNilHnadler
	}
	ctx, c.cancel = context.WithCancel(ctx)
	c.mu.RLock()
	msgs, err := c.channel.Consume(
		c.queueName,
		c.consumerName,
		c.autoAck,
		false,
		false,
		c.noWait,
		nil,
	)
	c.mu.RUnlock()
	if err != nil {
		return errors.Wrap(err, "getting consume channel")
	}
	gotMsgs := make(chan amqp.Delivery, c.workers*c.pubChBuff)
	go func() {
		<-ctx.Done()
		close(gotMsgs)
	}()

	go func() {
		for msg := range msgs {
			select {
			case <-ctx.Done():
				return
			default:
			}
			gotMsgs <- msg
		}
	}()
	var wg sync.WaitGroup
	wg.Add(c.workers)
	for i := 0; i < c.workers; i++ {
		go func() {
			defer wg.Done()
			c.consumeLoop(gotMsgs, handler)
		}()
	}
	wg.Wait()
	return ctx.Err()
}

func (c *Client) consumeLoop(msgs <-chan amqp.Delivery, handler HandlerFunc) {
	for msg := range msgs {
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
			switch err {
			case nil:
				msg.Ack(false)
			default:
				msg.Nack(false, true)
			}
		}
	}
}

// Close closes the channel and the connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
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

func (c *Client) validate() error {
	if c.conn == nil && c.url == "" {
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

func (c *Client) registerReconnect(ctx context.Context) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ch := c.channel.NotifyClose(make(chan *amqp.Error))
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-ch:
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

			dial := func() error {
				// already reconnected
				if c.conn != nil {
					return nil
				}
				conn, err := amqp.Dial(c.url)
				if err != nil {
					return err
				}
				c.conn = &rabbitWrapper{conn}
				return nil
			}
			channel := func() error {
				var err error
				c.channel, err = c.conn.Channel()
				return err
			}
			var err error
			for {
				time.Sleep(c.retryDelay)
				err = dial()
				if err == nil {
					err = channel()
					if err == nil {
						break
					}
				}
			}
			go c.registerReconnect(ctx)
		}
	}()
}
