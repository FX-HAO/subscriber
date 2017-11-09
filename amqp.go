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

// AMQPSubsriber represents a subscriber to receive messages from AMQP
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

func (sub *AMQPSubscriber) Close() {
	if atomic.AddInt32(&sub.closed, 1) == 1 {
		sub.cancel()

		sub.mu.Lock()
		if sub.conn != nil {
			sub.conn = nil
			sub.channel = nil
		}
		sub.mu.Unlock()

		sub.wg.Wait()
		sub.log.Infof(" [-] Subscriber %s is now safe to shutdown", sub.name)
	}
}

func (sub *AMQPSubscriber) Run() {
	tempDelay := 1 * time.Second // how long to sleep on accept failure

	for {
		sub.mu.Lock()
		if sub.isClosed() {
			sub.mu.Unlock()
			return
		}

		if sub.conn == nil {
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
			if _, err := channel.QueueDeclare(sub.AMQP.QueueName, true, false, false, false, nil); err != nil {
				sub.log.Fatalf("queue.declare: %v", err)
			}
			if sub.AMQP.ExchangeName != "" {
				for i := range sub.AMQP.RouteKey {
					if err := channel.QueueBind(sub.AMQP.QueueName, sub.AMQP.RouteKey[i], sub.AMQP.ExchangeName, false, nil); err != nil {
						sub.log.Fatalf("queue.bind: %v", err)
					}
				}
			}
		}
		sub.mu.Unlock()

		deliveries, err := sub.channel.Consume(
			sub.AMQP.QueueName, // queue
			"",                 // consumer
			sub.AMQP.Ack,       // auto-ack
			false,              // exclusive
			false,              // no-local
			false,              // no-wait
			nil,                // args
		)
		if err != nil {
			sub.log.Errorf("Failed to register a consumer: %v", err)
		}
		terminated := make(chan bool, 1)
		go func() {
			for delivery := range deliveries {
				sub.wg.Add(1)
				go func(delivery amqp.Delivery) {
					args := []interface{}{delivery}
					sub.exec(args...)
					sub.wg.Done()
				}(delivery)
			}
			terminated <- true
		}()

		select {
		case <-terminated:
			break
		case <-sub.ctx.Done():
			sub.channel.Close()
			return
		}

		sub.mu.Lock()
		sub.conn = nil
		sub.mu.Unlock()
		sub.log.Errorf("Subscriber %s accepts terminated, retring in %v", sub.name, tempDelay)
		select {
		case <-time.After(tempDelay):
		case <-sub.ctx.Done():
			return
		}
	}
}
