# subscriber

[![GoDoc](https://godoc.org/github.com/FX-HAO/subscriber?status.svg)](http://godoc.org/github.com/FX-HAO/subscriber)
[![Build Status](https://travis-ci.org/FX-HAO/subscriber.svg?branch=master)](https://travis-ci.org/FX-HAO/subscriber)
[![Test Coverage](https://api.codeclimate.com/v1/badges/d56ab630a64c030488c4/test_coverage)](https://codeclimate.com/github/FX-HAO/subscriber/test_coverage)

Subscriber is a simple implementation of the subscribers in Pub/Sub pattern. It's easy to use and provide you most of the basic functionality you need.

## Usage

```go
import (
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

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
```

## Roadmap
- [x] Add support for AMQP
- [x] Add support for Redis
