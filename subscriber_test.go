package subscriber

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

func mockLogger() logger {
	return logrus.New()
}

func mockSubscriberManager() *SubscriberManager {
	return NewSubscriberManager(mockLogger())
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
			Url:        fmt.Sprintf("amqp://%s/test.amqp.exchange/test.amqp.queue?route=#&ack=true&type=fanout", amqpAddr),
			ActionFunc: func(args ...interface{}) {},
		},
	)
	subMgr.Run()

	time.Sleep(time.Second)
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
}

func TestParseEndpoint(t *testing.T) {
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
