package subscriber

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

// AMQPSubsriber represents a subscriber to receive messages from AMQP
type AMQPSubscriber struct {
	*Endpoint
	log     logger
	mu      sync.RWMutex
	conn    *amqp.Connection
	channel *amqp.Channel
	exec    ActionFunc

	drain bool
}

func newAMQPSubscriber(ep Endpoint, setup *Setup, logger logger) *AMQPSubscriber {
	return &AMQPSubscriber{
		Endpoint: &ep,
		exec:     setup.ActionFunc,
		log:      logger,
	}
}

func (sub *AMQPSubscriber) Close() {
	sub.mu.Lock()
	defer sub.mu.Unlock()
	if !sub.drain && sub.conn != nil {
		sub.conn.Close()
		sub.conn = nil
		sub.channel = nil
	}
}

func (sub *AMQPSubscriber) Run() {
	tempDelay := 1 * time.Second // how long to sleep on accept failure

	for {
		sub.mu.Lock()
		if sub.drain {
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
		for delivery := range deliveries {
			go func(delivery amqp.Delivery) {
				args := []interface{}{delivery}
				sub.exec(args...)
			}(delivery)
		}

		sub.log.Errorf("Accept terminated, retring in %v", tempDelay)
		sub.conn = nil
		time.Sleep(tempDelay)
	}
}
