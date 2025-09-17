package harego

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Publisher is a concurrent safe construct for publishing a message to exchanges,
// and consuming messages from queues. It creates multiple workers for safe
// communication. Zero value is not usable.
type Publisher struct {
	connector  Connector
	workers    int
	retryDelay time.Duration
	logger     logr.Logger

	mu       sync.RWMutex
	conn     RabbitMQ
	channels map[Channel]struct{}

	// queue properties.
	routingKey string
	exclusive  bool
	queueArgs  amqp.Table

	// exchange properties.
	exchName   string
	exchType   ExchangeType
	durable    bool
	autoDelete bool
	internal   bool
	noWait     bool

	// message properties.
	prefetchCount int
	prefetchSize  int
	deliveryMode  DeliveryMode

	chBuff int
	pubCh  chan *publishMsg
	once   sync.Once
	ctx    context.Context //nolint:containedctx // for turning off the client.
	cancel func()
	closed bool
}

type publishMsg struct {
	msg   *amqp.Publishing
	errCh chan error
}

// NewPublisher returns a Client capable of publishing and consuming messages.
// The default Client config uses the "default" exchange of the "topic" type,
// both exchange and queues will be marked as "durable", messages will be
// persistent, and the consumer gets a random name. The connector value should
// provide a live connection. The connector value is used during reconnection
// process.
func NewPublisher(connector Connector, conf ...ConfigFunc) (*Publisher, error) {
	cnf := defaultConfig()
	for _, fn := range conf {
		fn(cnf)
	}

	pub := cnf.publisher()
	pub.connector = connector
	pub.ctx, pub.cancel = context.WithCancel(pub.ctx)

	err := pub.validate()
	if err != nil {
		return nil, fmt.Errorf("validating configuration: %w", err)
	}

	pub.conn, err = pub.connector()
	if err != nil {
		return nil, fmt.Errorf("getting a connection to the broker: %w", err)
	}

	if pub.chBuff == 0 {
		pub.chBuff = 1
	}

	pub.pubCh = make(chan *publishMsg, pub.workers*pub.chBuff)

	pub.channels = make(map[Channel]struct{}, pub.workers)

	for range pub.workers {
		pub.mu.Lock()

		channel, err := pub.newChannel()
		if err != nil {
			pub.mu.Unlock()
			return nil, fmt.Errorf("setting up a channel: %w", err)
		}

		pub.mu.Unlock()

		pub.channels[channel] = struct{}{}
		pub.publishWorker(channel)
		pub.registerReconnect(channel)
	}

	pub.logger = pub.logger.
		WithName("publish").
		WithName(pub.exchName)

	return pub, nil
}

// Publish sends the msg to the broker via the next available workers.
func (p *Publisher) Publish(msg *amqp.Publishing) error {
	p.mu.RLock()

	if p.closed {
		p.mu.RUnlock()
		return ErrClosed
	}

	p.mu.RUnlock()

	err := make(chan error)
	p.pubCh <- &publishMsg{
		msg:   msg,
		errCh: err,
	}

	return <-err
}

// Close closes the channel and the connection. A closed client is not usable.
func (p *Publisher) Close() error {
	p.mu.RLock()

	if p.closed {
		p.mu.RUnlock()
		return ErrClosed
	}

	p.mu.RUnlock()

	p.mu.Lock()
	defer p.mu.Unlock()

	p.once.Do(func() {
		p.closed = true
		p.cancel()
	})

	var err error

	for channel := range p.channels {
		er := channel.Close()
		if er != nil {
			err = errors.Join(err, er)
		}

		delete(p.channels, channel)
	}

	if p.conn != nil {
		er := p.conn.Close()
		if er != nil {
			err = errors.Join(err, er)
		}

		p.conn = nil
	}

	return err
}

// acquireNewChannel closes the channel and starts a new one if the publisher
// is not closed.
//
//nolint:ireturn // This is a wrapper around conn.Channel()
func (p *Publisher) acquireNewChannel(channel Channel) Channel {
	p.logErr(channel.Close(), "closing channel", "channel")
	p.mu.Lock()
	delete(p.channels, channel)

	for {
		if p.closed {
			p.mu.Unlock()
			return nil
		}

		var err error

		channel, err = p.newChannel()
		if err == nil {
			break
		}

		time.Sleep(p.retryDelay)
	}

	p.channels[channel] = struct{}{}
	p.mu.Unlock()
	p.publishWorker(channel)
	p.registerReconnect(channel)
	p.logger.Info("Reconnected publisher")

	return channel
}

func (p *Publisher) publishWorker(channel Channel) {
	go func() {
		for msg := range p.pubCh {
			select {
			case <-p.ctx.Done():
				msg.errCh <- p.ctx.Err()
				return
			default:
			}

			msg.msg.DeliveryMode = uint8(p.deliveryMode)
			p.mu.RLock()

			if channel == nil {
				p.mu.RUnlock()

				msg.errCh <- ErrClosed

				return
			}

			err := channel.Publish(
				p.exchName,
				p.routingKey,
				false,
				false,
				*msg.msg,
			)
			p.mu.RUnlock()

			if errors.Is(err, amqp.ErrClosed) {
				p.pubCh <- msg

				ch := p.acquireNewChannel(channel)
				p.publishWorker(ch)

				return
			}

			if err != nil {
				err = fmt.Errorf("publishing message: %w", err)
			}

			msg.errCh <- err
		}
	}()
}

func (p *Publisher) validate() error {
	if p.connector == nil {
		return fmt.Errorf("empty connection function (Connector): %w", ErrInput)
	}

	if p.workers < 1 {
		return fmt.Errorf("not enough workers: %d: %w", p.workers, ErrInput)
	}

	if p.exchName == "" {
		return fmt.Errorf("empty exchange name: %w", ErrInput)
	}

	if !p.exchType.IsValid() {
		return fmt.Errorf("exchange type: %q: %w", p.exchType.String(), ErrInput)
	}

	if !p.deliveryMode.IsValid() {
		return fmt.Errorf("delivery mode: %q: %w", p.deliveryMode.String(), ErrInput)
	}

	return nil
}

func (p *Publisher) logErr(err error, msg, section string) {
	if err != nil {
		p.logger.Error(err, msg, "type", "publisher", "section", section)
	}
}

func (p *Publisher) registerReconnect(channel Channel) {
	p.mu.RLock()

	if p.closed {
		p.mu.RUnlock()
		return
	}

	p.mu.RUnlock()

	const maxNotifCount = 2

	errCh := channel.NotifyClose(make(chan *amqp.Error, maxNotifCount))

	go func() {
		select {
		case <-p.ctx.Done():
			return
		case err := <-errCh:
			p.logErr(err, "closed publisher", "connection")
			p.mu.RLock()

			if p.closed {
				p.mu.RUnlock()
				return
			}

			p.mu.RUnlock()

			p.logErr(channel.Close(), "closing channel", "channel")

			p.mu.Lock()

			if p.conn != nil {
				p.logErr(p.conn.Close(), "closing connection", "connection")
				p.conn = nil
			}

			p.mu.Unlock()

			channel := p.keepConnecting()
			if channel == nil {
				p.logErr(nil, "unable to reconnect", "registerReconnect")

				return
			}

			p.logger.Info("reconnected")

			go p.registerReconnect(channel)
		}
	}()
}

//nolint:ireturn // This is a wrapper around the conn.Channel func.
func (p *Publisher) keepConnecting() Channel {
	// In each step we create a connection, we want to clean up if any of the
	// consequent step fails.
	type cleanup map[string]func() error

	var cleanups []cleanup

	for {
		for _, cln := range cleanups {
			for section, fn := range cln {
				p.logErr(fn(), "Cleaning up", section)
			}
		}

		const expectedCleanupCount = 1

		cleanups = make([]cleanup, 0, expectedCleanupCount)

		time.Sleep(p.retryDelay)
		p.mu.RLock()

		if p.closed {
			p.mu.RUnlock()
			return nil
		}

		p.mu.RUnlock()

		dialCleanup, err := p.dial()
		if err != nil {
			p.logger.V(1).Info("dial up", "err", err.Error())
			continue
		}

		cleanups = append(cleanups, cleanup{"dial": dialCleanup})

		newCh, err := p.conn.Channel()
		if err != nil {
			p.logger.V(1).Info("setting up a channel", "err", err.Error())
			continue
		}

		return newCh
	}
}

// dial requires the mutex to be locked.
func (p *Publisher) dial() (func() error, error) {
	if p.conn != nil {
		// already reconnected
		return p.conn.Close, nil
	}

	conn, err := p.connector()
	if err != nil {
		return nil, fmt.Errorf("getting a connection to the broker: %w", err)
	}

	p.conn = conn

	return conn.Close, nil
}

// newChannel requires the mutex to be locked.
//
//nolint:ireturn // This is a wrapper around the conn.Channel func.
func (p *Publisher) newChannel() (Channel, error) {
	cleanup, err := p.dial()
	if err != nil {
		return nil, err
	}

	channel, err := p.conn.Channel()
	if err != nil {
		p.logErr(cleanup(), "creating channel", "channel")
		return nil, fmt.Errorf("creating channel: %w", err)
	}

	err = channel.ExchangeDeclare(
		p.exchName,
		p.exchType.String(),
		p.durable,
		p.autoDelete,
		p.internal,
		p.noWait,
		nil,
	)
	if err != nil {
		p.logErr(cleanup(), "declaring exchange", "exchange")
		return nil, fmt.Errorf("declaring exchange: %w", err)
	}

	return channel, nil
}
