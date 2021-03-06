package subscriber

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/streadway/amqp"
)

var reconnInterval = 3 * time.Second

// AMQPSubscriber represents a subscriber, which consumes messages from AMQP
type AMQPSubscriber struct {
	*Endpoint
	mu  sync.RWMutex
	wg  *sync.WaitGroup
	log logger

	name    string
	conn    *amqp.Connection
	channel *amqp.Channel
	exec    ActionFunc

	closed int32 // accessed atomically (non-zero means we're closed)
	ctx    context.Context
	cancel context.CancelFunc
}

func newAMQPSubscriber(name string, ep Endpoint, setup *Setup, logger logger) *AMQPSubscriber {
	ctx, cancel := context.WithCancel(context.Background())
	return &AMQPSubscriber{
		name:     name,
		Endpoint: &ep,
		exec:     setup.ActionFunc,
		log:      logger,
		ctx:      ctx,
		cancel:   cancel,
		wg:       &sync.WaitGroup{},
	}
}

func (sub *AMQPSubscriber) isClosed() bool {
	return atomic.LoadInt32(&sub.closed) != 0
}

// Close closes the subscriber gracefully, it blocks until all messages are handled
func (sub *AMQPSubscriber) Close() {
	if atomic.AddInt32(&sub.closed, 1) == 1 {
		sub.cancel()

		sub.mu.Lock()
		if sub.conn != nil {
			sub.conn.Close()
			sub.conn = nil
			sub.channel = nil
		}
		sub.mu.Unlock()

		sub.wg.Wait()
		sub.log.Infof(" [-] Subscriber %s is now safe to shutdown", sub.name)
	}
}

func (sub *AMQPSubscriber) connect() error {
	sub.mu.Lock()
	defer sub.mu.Unlock()
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s", sub.AMQP.URI))
	if err != nil {
		sub.log.Fatalf("Failed to connect to RabbitMQ, %v", err)
	}
	sub.conn = conn

	channel, err := conn.Channel()
	if err != nil {
		sub.log.Fatalf("Failed to open a channel, %v", err)
	}
	sub.channel = channel

	// Declare new exchange
	if err := channel.ExchangeDeclare(
		sub.AMQP.ExchangeName,
		sub.AMQP.Type,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		sub.log.Warnf("exchange.declare: %v", err)

		// Reopen a channel because of `ExchangeDeclare` will close the channel when errors returned
		channel, err = conn.Channel()
		if err != nil {
			log.Fatalf("Failed to open a channel, %v", err)
		}
		sub.channel = channel
	}

	// Declare queue and make binding
	if _, err := channel.QueueDeclare(sub.AMQP.QueueName, true, false, sub.AMQP.Exclusive, false, nil); err != nil {
		sub.log.Fatalf("queue.declare: %v", err)
	}
	if sub.AMQP.ExchangeName != "" {
		for i := range sub.AMQP.RouteKey {
			if err := channel.QueueBind(sub.AMQP.QueueName, sub.AMQP.RouteKey[i], sub.AMQP.ExchangeName, false, nil); err != nil {
				sub.log.Fatalf("queue.bind: %v", err)
			}
		}
	}
	return nil
}

func (sub *AMQPSubscriber) getConn() *amqp.Connection {
	sub.mu.RLock()
	defer sub.mu.RUnlock()
	return sub.conn
}

func (sub *AMQPSubscriber) getChannel() *amqp.Channel {
	sub.mu.RLock()
	defer sub.mu.RUnlock()
	return sub.channel
}

func (sub *AMQPSubscriber) reconnect() {
	if conn := sub.getConn(); conn != nil {
		if err := conn.Close(); err != nil {
			sub.log.Errorf("Subscriber %s failed to close connection: %s", sub.name, err)
		}
	}

	if sub.isClosed() {
		return
	}

	if err := sub.connect(); err != nil {
		log.Fatalf("Failed to reconnect: %v", err)
	}
	sub.log.Infof("Subscriber %s reconnect successfully", sub.name)
}

func (sub *AMQPSubscriber) consume() (<-chan amqp.Delivery, error) {
	ch := sub.getChannel()
	if ch == nil {
		return nil, fmt.Errorf("Subscriber %v consumes an empty channel", sub.name)
	}

	return ch.Consume(
		sub.AMQP.QueueName, // queue
		"",                 // consumer
		sub.AMQP.Ack,       // auto-ack
		sub.AMQP.Exclusive, // exclusive
		false,              // no-local
		false,              // no-wait
		nil,                // args
	)
}

// Run starts the subscriber and blocks until the subscriber is closed
func (sub *AMQPSubscriber) Run() {
	if conn := sub.getConn(); conn == nil {
		sub.connect()
	}

	for {
		if sub.isClosed() {
			return
		}

		deliveries, err := sub.consume()
		if err != nil {
			sub.log.Errorf("Failed to register a consumer: %v", err)

			select {
			case <-time.After(reconnInterval):
			case <-sub.ctx.Done():
				return
			}
			continue
		}

		go func() {
			for delivery := range deliveries {
				sub.wg.Add(1)
				go func(delivery amqp.Delivery) {
					sub.exec(delivery)
					sub.wg.Done()
				}(delivery)
			}
		}()

		terminated := make(chan struct{}, 1)
		go func() {
			var err *amqp.Error
			select {
			case err = <-sub.getConn().NotifyClose(make(chan *amqp.Error)):
			case err = <-sub.getChannel().NotifyClose(make(chan *amqp.Error)):
			}
			sub.log.Errorf("Subscriber %s's channel/connection closed: %s", sub.name, err)
			terminated <- struct{}{}
		}()

		select {
		case <-terminated:
			time.Sleep(reconnInterval)
			sub.reconnect()
			break
		case <-sub.ctx.Done():
			sub.channel.Close()
			return
		}
	}
}
