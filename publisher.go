package harego

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Publisher is a concurrent safe construct for publishing a message to exchanges,
// and consuming messages from queues. It creates multiple workers for safe
// communication. Zero value is not usable.
// nolint:govet // most likely not an issue, but cleaner this way.
type Publisher struct {
	connector    Connector
	workers      int
	consumerName string
	retryDelay   time.Duration
	logger       logger

	mu       sync.RWMutex
	conn     RabbitMQ
	channels map[Channel]struct{}

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
	internal   bool
	noWait     bool

	// message properties.
	prefetchCount int
	prefetchSize  int
	deliveryMode  DeliveryMode

	chBuff int
	pubCh  chan *publishMsg
	once   sync.Once
	ctx    context.Context // for turning off the client.
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

	p := cnf.publisher()
	p.connector = connector
	p.ctx, p.cancel = context.WithCancel(context.Background())

	err := p.validate()
	if err != nil {
		return nil, fmt.Errorf("validating configuration: %w", err)
	}

	p.conn, err = p.connector()
	if err != nil {
		return nil, fmt.Errorf("getting a connection to the broker: %w", err)
	}

	if p.chBuff == 0 {
		p.chBuff = 1
	}
	p.pubCh = make(chan *publishMsg, p.workers*p.chBuff)
	p.channels = make(map[Channel]struct{}, p.workers)
	for i := 0; i < p.workers; i++ {
		ch, err := p.newChannel()
		if err != nil {
			return nil, fmt.Errorf("setting up a channel: %w", err)
		}

		p.channels[ch] = struct{}{}
		p.publishWorker(ch)
		p.registerReconnect(ch)
	}
	return p, nil
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

func (p *Publisher) publishWorker(ch Channel) {
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
			if ch == nil {
				p.mu.RUnlock()
				msg.errCh <- ErrClosed
				return
			}
			err := ch.Publish(
				p.exchName,
				p.routingKey,
				false,
				false,
				*msg.msg,
			)
			p.mu.RUnlock()
			if err != nil {
				err = fmt.Errorf("publishing message: %w", err)
			}
			msg.errCh <- err
		}
	}()
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
	for ch := range p.channels {
		er := ch.Close()
		if er != nil {
			err = errors.Join(err, er)
		}
		delete(p.channels, ch)
	}
	if p.conn != nil {
		er := p.conn.Close()
		if er != nil {
			err = multierror.Append(err, er)
		}
		p.conn = nil
	}
	return err
}

func (p *Publisher) validate() error {
	if p.connector == nil {
		return fmt.Errorf("empty connection function (Connector): %w", ErrInput)
	}
	if p.workers < 1 {
		return fmt.Errorf("not enough workers: %d: %w", p.workers, ErrInput)
	}
	if p.consumerName == "" {
		return fmt.Errorf("empty consumer name: %w", ErrInput)
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

func (p *Publisher) logErr(err error) {
	if err != nil {
		p.logger.Warn(err)
	}
}

func (p *Publisher) registerReconnect(ch Channel) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return
	}
	p.mu.RUnlock()
	errCh := ch.NotifyClose(make(chan *amqp.Error))
	go func() {
		select {
		case <-p.ctx.Done():
			return
		case <-errCh:
			if p.closed {
				return
			}

			p.logErr(ch.Close())

			p.mu.Lock()
			if p.conn != nil {
				p.logErr(p.conn.Close())
				p.conn = nil
			}
			p.mu.Unlock()
			ch := p.keepConnecting()
			if ch == nil {
				return
			}
			go p.registerReconnect(ch)
		}
	}()
}

func (p *Publisher) keepConnecting() Channel {
	for {
		time.Sleep(p.retryDelay)
		p.mu.RLock()
		if p.closed {
			p.mu.RUnlock()
			return nil
		}
		p.mu.RUnlock()
		err := p.dial()
		if err != nil {
			continue
		}
		newCh, err := p.conn.Channel()
		if err != nil {
			continue
		}
		return newCh
	}
}

func (p *Publisher) dial() error {
	// already reconnected
	if p.conn != nil {
		return nil
	}
	conn, err := p.connector()
	if err != nil {
		return fmt.Errorf("getting a connection to the broker: %w", err)
	}
	p.conn = conn
	return nil
}

func (p *Publisher) newChannel() (Channel, error) {
	ch, err := p.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("creating channel: %w", err)
	}
	// to make sure rabbitmq is fair on workers.
	err = ch.Qos(p.prefetchCount, p.prefetchSize, true)
	if err != nil {
		return nil, fmt.Errorf("setting Qos: %w", err)
	}

	err = ch.ExchangeDeclare(
		p.exchName,
		p.exchType.String(),
		p.durable,
		p.autoDelete,
		p.internal,
		p.noWait,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("declaring exchange: %w", err)
	}
	return ch, nil
}
