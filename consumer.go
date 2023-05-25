package harego

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime/debug"
	"sync"
	"time"

	"github.com/go-logr/logr"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Consumer is a concurrent safe construct for consuming messages from queues.
// It creates multiple workers for safe communication. Zero value is not
// usable, therefore you should construct a usable Consumer by calling the
// NewConsumer constructor.
// nolint:govet // most likely not an issue, but cleaner this way.
type Consumer struct {
	connector    Connector
	workers      int
	consumerName string
	retryDelay   time.Duration
	publisher    *Publisher // used for requeueing messages.
	logger       logr.Logger

	mu      sync.RWMutex
	conn    RabbitMQ
	channel Channel
	queue   amqp.Queue
	msgs    <-chan amqp.Delivery // for cleaning up

	// queue properties.
	queueName  string
	routingKey string
	exclusive  bool
	queueArgs  amqp.Table

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

	chBuff    int
	consumeCh chan amqp.Delivery // used for consuming messages and setup Consume loop.
	once      sync.Once
	ctx       context.Context // for turning off the Consumer.
	cancel    func()
	closed    bool

	panicHandler PanicHandler
}

// NewConsumer returns a Consumer capable of publishing and consuming messages.
// The default Consumer config uses the "default" exchange of the "topic" type,
// both exchange and queues will be marked as "durable", messages will be
// persistent, and the consumer gets a random name. The connector value should
// provide a live connection. The connector value is used during reconnection
// process.
func NewConsumer(connector Connector, conf ...ConfigFunc) (*Consumer, error) {
	cnf := defaultConfig()
	for _, fn := range conf {
		fn(cnf)
	}

	c := cnf.consumer()
	if c.chBuff == 0 {
		c.chBuff = 1
	}
	c.connector = connector
	c.ctx, c.cancel = context.WithCancel(c.ctx)

	if c.prefetchCount < c.workers {
		c.prefetchCount = c.workers
	}
	err := c.validate()
	if err != nil {
		return nil, fmt.Errorf("validating configuration: %w", err)
	}

	c.conn, err = c.connector()
	if err != nil {
		return nil, fmt.Errorf("getting a connection to the broker: %w", err)
	}

	_, err = c.setupChannel()
	if err != nil {
		return nil, fmt.Errorf("setting up a channel: %w", err)
	}

	c.publisher, err = NewPublisher(connector, conf...)
	if err != nil {
		return nil, fmt.Errorf("setting up requeue: %w", err)
	}

	err = c.setupQueue()
	if err != nil {
		return nil, fmt.Errorf("setting up a queue: %w", err)
	}

	c.registerReconnect(c.ctx)
	c.logger = c.logger.
		WithName("consume").
		WithName(c.exchName).
		WithName(c.queueName)

	if c.panicHandler == nil {
		c.panicHandler = func(msg *amqp.Delivery, r any) (a AckType, delay time.Duration) {
			err := fmt.Errorf("panic: %v", r)
			c.logger.WithValues("message_id", msg.MessageId).Error(err, string(debug.Stack()))
			return AckTypeRequeue, time.Second
		}
	}
	return c, nil
}

// Consume is a bloking call that passes each message to the handler and stops
// handling messages when the context is done. If the handler returns false,
// the message is returned back to the queue. If the context is cancelled, the
// Consumer remains operational but no messages will be deliverd to this
// handler. Consume returns an error if you don't specify a queue name.
func (c *Consumer) Consume(ctx context.Context, handler HandlerFunc) error {
	if c.closed {
		return ErrClosed
	}
	if handler == nil {
		return ErrNilHnadler
	}

	c.mu.Lock()
	c.ctx, c.cancel = context.WithCancel(ctx)
	c.consumeCh = make(chan amqp.Delivery, c.workers*c.chBuff)
	err := c.setupConsumeCh()
	if err != nil {
		c.mu.Unlock()
		return fmt.Errorf("setting up consume process: %w", err)
	}
	c.mu.Unlock()

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

func (c *Consumer) logErr(err error, msg string) {
	if err != nil {
		c.logger.Error(err, msg)
	}
}

func (c *Consumer) consumeLoop(handler HandlerFunc) {
	h := func(msg *amqp.Delivery) (a AckType, delay time.Duration) {
		defer func() {
			if r := recover(); r != nil {
				a, delay = c.panicHandler(msg, r)
			}
		}()
		return handler(msg)
	}
	for msg := range c.consumeCh {
		msg := msg
		a, delay := h(&msg)
		switch a {
		case AckTypeAck:
			time.Sleep(delay)
			c.logErr(msg.Ack(false), "Ack message")
		case AckTypeNack:
			time.Sleep(delay)
			c.logErr(msg.Nack(false, true), "Nack message")
		case AckTypeReject:
			time.Sleep(delay)
			c.logErr(msg.Reject(false), "Reject message")
		case AckTypeRequeue:
			time.Sleep(delay)
			err := c.publisher.Publish(&amqp.Publishing{
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
				c.logErr(msg.Nack(false, true), "Requeue message")
				continue
			}
			c.logErr(msg.Ack(false), "Ack message")
		}
	}
}

// Close closes the channel and the connection. A closed Consumer is not
// usable.
// nolint:dupl // They are quite different.
func (c *Consumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}

	c.once.Do(func() {
		c.closed = true
		c.cancel()
	})
	var err error
	if c.channel != nil && !reflect.ValueOf(c.channel).IsNil() {
		er := c.channel.Close()
		if er != nil {
			err = fmt.Errorf("closing channel: %w", er)
		}
		c.channel = nil
	}
	if c.conn != nil {
		er := c.conn.Close()
		if er != nil {
			er = fmt.Errorf("closing connection: %w", er)
			err = errors.Join(err, er)
		}
		c.conn = nil
	}
	return err
}

func (c *Consumer) validate() error {
	if c.connector == nil {
		return fmt.Errorf("empty connection function (Connector): %w", ErrInput)
	}
	if c.workers < 1 {
		return fmt.Errorf("not enough workers: %d: %w", c.workers, ErrInput)
	}
	if c.consumerName == "" {
		return fmt.Errorf("empty consumer name: %w", ErrInput)
	}
	if c.queueName == "" {
		return fmt.Errorf("empty queue name: %w", ErrInput)
	}
	if c.exchName == "" {
		return fmt.Errorf("empty exchange name: %w", ErrInput)
	}
	if c.prefetchCount < 1 {
		return fmt.Errorf("not enough prefetch count: %d: %w", c.prefetchCount, ErrInput)
	}
	if c.prefetchSize < 0 {
		return fmt.Errorf("not enough prefetch size: %d: %w", c.prefetchSize, ErrInput)
	}
	if !c.deliveryMode.IsValid() {
		return fmt.Errorf("delivery mode: %q: %w", c.deliveryMode.String(), ErrInput)
	}
	return nil
}

func (c *Consumer) registerReconnect(ctx context.Context) {
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
		case err := <-ch:
			c.logger.Info("closed consumer", "err", err)
			c.mu.Lock()
			if c.closed {
				c.mu.Unlock()
				return
			}
			if c.msgs != nil {
				// We should clean up the channel otherwise it will block on other
				// channels reading from the same connection.
				for range c.msgs { //nolint:revive // this is on purpose.
				}
			}
			if c.channel != nil {
				c.logErr(c.channel.Close(), "Closing channel")
				c.channel = nil
			}
			if c.conn != nil {
				c.logErr(c.conn.Close(), "Closing connection")
				c.conn = nil
			}
			c.mu.Unlock()
			c.keepConnecting()
			go c.registerReconnect(ctx)
		}
	}()
}

func (c *Consumer) keepConnecting() {
	// In each step we create a connection, we want to clean up if any of the
	// consequent step fails.
	var cleanups []func() error
	for {
		for _, fn := range cleanups {
			c.logErr(fn(), "Cleaning up")
		}
		cleanups = make([]func() error, 0, 2)
		time.Sleep(c.retryDelay)
		c.mu.RLock()
		if c.closed {
			c.mu.RUnlock()
			return
		}
		c.mu.RUnlock()

		cleanup, err := c.dial()
		if err != nil {
			c.logger.V(1).Info("dial up", "err", err)
			continue
		}
		cleanups = append(cleanups, cleanup)

		cleanup, err = c.setupChannel()
		if err != nil {
			c.logger.V(1).Info("setting up a channel", "err", err)
			continue
		}
		cleanups = append(cleanups, cleanup)

		err = c.setupQueue()
		if err != nil {
			c.logger.V(1).Info("setting up the queue", "err", err)
			continue
		}
		err = c.setupConsumeCh()
		if err != nil {
			c.logger.V(1).Info("setting up the consumer channel", "err", err)
			continue
		}
		c.logger.Info("Reconnected consumer")
		return
	}
}

func (c *Consumer) dial() (func() error, error) {
	// already reconnected
	if c.conn != nil {
		return nil, nil
	}
	conn, err := c.connector()
	if err != nil {
		return nil, fmt.Errorf("getting a connection to the broker: %w", err)
	}
	c.conn = conn
	return conn.Close, nil
}

func (c *Consumer) setupChannel() (func() error, error) {
	var err error
	c.channel, err = c.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("creating channel: %w", err)
	}
	// to make sure rabbitmq is fair on workers.
	err = c.channel.Qos(c.prefetchCount, c.prefetchSize, true)
	if err != nil {
		return nil, fmt.Errorf("setting Qos: %w", err)
	}
	return c.channel.Close, nil
}

func (c *Consumer) setupQueue() error {
	var err error
	c.queue, err = c.channel.QueueDeclare(
		c.queueName,
		c.durable,
		c.autoDelete,
		c.exclusive,
		c.noWait,
		c.queueArgs,
	)
	if err != nil {
		return fmt.Errorf("declaring queue: %w", err)
	}
	err = c.channel.QueueBind(
		c.queueName,
		c.routingKey,
		c.exchName,
		c.noWait,
		nil,
	)
	if err != nil {
		return fmt.Errorf("binding queue: %w", err)
	}
	return nil
}

func (c *Consumer) setupConsumeCh() error {
	if c.consumeCh == nil {
		return nil
	}
	var err error
	c.msgs, err = c.channel.Consume(
		c.queueName,
		c.consumerName,
		c.autoAck,
		c.exclusive,
		false,
		c.noWait,
		nil,
	)
	if err != nil {
		return fmt.Errorf("getting consume channel: %w", err)
	}
	go func() {
		for msg := range c.msgs {
			select {
			case <-c.ctx.Done():
				return
			default:
			}
			c.consumeCh <- msg
		}
	}()
	return nil
}
