package subscriber

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/go-redis/redis"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

func mockLogger() logger {
	return logrus.New()
}

func mockSubscriberManager() *SubscriberManager {
	return NewSubscriberManager(mockLogger())
}

func ExampleSubscriberManager() {
	logger := logrus.New()
	subMgr := NewSubscriberManager(logger)
	subMgr.Register(
		"TestAMQPSubscriber",
		&Setup{
			URL: "amqp://root:root@rabbitmq:5672/test.amqp.exchange1/test.amqp.queue1?route=foo&route=bar&ack=false&type=direct",
			ActionFunc: func(args ...interface{}) {
				delivery := args[0].(amqp.Delivery)
				delivery.Ack(false)
			},
		},
	)
	subMgr.Register(
		"TestRedisSubscriber",
		&Setup{
			URL: "redis://:password@redis:6379/?channel=foo&channel=bar",
			ActionFunc: func(args ...interface{}) {
				message := args[0].(*redis.Message).Payload
				fmt.Println(message)
			},
		},
	)
	subMgr.Run()

	// Stop the subscribers
	subMgr.GracefulStop()
}

func TestAMQPSubscriber(t *testing.T) {
	amqpAddr := os.Getenv("AMQP_ADDR")
	if amqpAddr == "" {
		t.Skip("skipping test if `AMQP_ADDR` is absent in the environment.")
	}

	subMgr := mockSubscriberManager()
	subMgr.Register(
		"TestAMQPSubscriber",
		&Setup{
			URL:        fmt.Sprintf("amqp://%s/test.amqp.exchange/test.amqp.queue?route=#&ack=true&type=fanout", amqpAddr),
			ActionFunc: func(args ...interface{}) {},
		},
	)
	subMgr.Run()

	time.Sleep(3 * time.Second)
	sub := subMgr.subs["TestAMQPSubscriber"].(*AMQPSubscriber)
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s", sub.AMQP.URI))
	if err != nil {
		sub.log.Fatalf("Failed to connect to RabbitMQ, %v", err)
	}
	channel, err := conn.Channel()
	if err != nil {
		sub.log.Fatalf("Failed to open a channel, %v", err)
	}
	if err := channel.ExchangeDeclarePassive(
		sub.AMQP.ExchangeName,
		sub.AMQP.Type,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		t.Errorf("The exchange should be existed, got: %v, want: %v", err, nil)
	} else {
		channel.ExchangeDelete(sub.AMQP.ExchangeName, false, false)
	}
	if _, err := channel.QueueDeclarePassive(
		sub.AMQP.QueueName,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		t.Errorf("The queue should be existed, got: %v, want: %v", err, nil)
	} else {
		channel.QueueDelete(sub.AMQP.QueueName, false, false, false)
	}
	subMgr.GracefulStop()
}

func TestRedisSubscriber(t *testing.T) {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		t.Skip("skipping test if `REDIS_ADDR` is absent in the environment.")
	}

	message := ""
	subMgr := mockSubscriberManager()
	subMgr.Register(
		"TestRedisSubscriber",
		&Setup{
			URL: fmt.Sprintf("redis://%s/?channel=test", redisAddr),
			ActionFunc: func(args ...interface{}) {
				message = args[0].(*redis.Message).Payload
			},
		},
	)
	subMgr.Run()

	time.Sleep(3 * time.Second)
	sub := subMgr.subs["TestRedisSubscriber"].(*RedisSubscriber)
	client := redis.NewClient(&redis.Options{
		Addr:     sub.Redis.Addr,
		Password: sub.Redis.Password, // no password set
	})
	if err := client.Publish("test", "hello").Err(); err != nil {
		t.Errorf("Failed to publish messages: %v", err)
	}
	time.Sleep(3 * time.Second)

	if message != "hello" {
		t.Errorf("Fail to handle the message, got: %v, want: %v", message, "hello")
	}
	subMgr.GracefulStop()
}

func TestParseAMQPEndpoint(t *testing.T) {
	amqpURL := "amqp://guest:guest@rabbitmq:5672/test.amqp.exchange/test.amqp.queue?route=#&ack=true&type=fanout"
	ep, err := parseEndpoint(amqpURL)
	if err != nil {
		t.Errorf("Fail to parse amqpURL, got: %v, want: %v", err, nil)
	}
	if ep.Protocol != AMQP {
		t.Errorf("The protocol is wrong, got: %v, want: %v", AMQP, ep.Protocol)
	}
	if len(ep.AMQP.RouteKey) != 1 && ep.AMQP.RouteKey[0] != "#" {
		t.Errorf("Fail to parse RouteKey, got: %v, want: %v", ep.AMQP.RouteKey, []string{"#"})
	}
	if !ep.AMQP.Ack {
		t.Errorf("Fail to parse Ack, got: %v, want: %v", ep.AMQP.Ack, true)
	}
	if ep.AMQP.ExchangeName != "test.amqp.exchange" {
		t.Errorf("Fail to parse ExchangeName, got: %v, want: %v", ep.AMQP.ExchangeName, "test.amqp.exchange")
	}
	if ep.AMQP.QueueName != "test.amqp.queue" {
		t.Errorf("Fail to parse QueueName, got: %v, want: %v", ep.AMQP.QueueName, "test.amqp.queue")
	}
	if ep.AMQP.URI != "guest:guest@rabbitmq:5672" {
		t.Errorf("Fail to parse URI, got: %v, want: %v", ep.AMQP.URI, "guest:guest@rabbitmq:5672")
	}
	if ep.AMQP.Type != "fanout" {
		t.Errorf("Fail to parse Type, got: %v, want: %v", ep.AMQP.Type, "fanout")
	}
}

func TestParseRedisEndpoint(t *testing.T) {
	redisURL := "redis://:root@redis:6379/?channel=sub1&sub2"
	ep, err := parseEndpoint(redisURL)
	if err != nil {
		t.Errorf("Fail to parse redisURL, got: %v, want: %v", err, nil)
	}
	if ep.Protocol != Redis {
		t.Errorf("The protocol is wrong, got: %v, want: %v", Redis, ep.Protocol)
	}
	if ep.Redis.Addr != "redis:6379" {
		t.Errorf("Fail to parse Addr, got: %v, want: %v", ep.Redis.Addr, "redis:6379")
	}
	if ep.Redis.Password != "root" {
		t.Errorf("Fail to parse password, got: %v, want: %v", ep.Redis.Password, "root")
	}
	if len(ep.Redis.Channels) != 2 && ep.Redis.Channels[0] != "sub1" && ep.Redis.Channels[1] != "sub2" {
		t.Errorf("Fail to parse channels, got: %v, want: %v", ep.Redis.Channels, []string{"sub1", "sub2"})
	}
}
