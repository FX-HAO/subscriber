package subscriber

import (
	"errors"
	"net/url"
	"strconv"
	"strings"
	"sync"
)

// EndpointProtocol is the type of protocol that the endpoint represents.
type EndpointProtocol string

const (
	Redis = EndpointProtocol("redis") // Redis
	AMQP  = EndpointProtocol("amqp")  // AMQP
)

// ActionFunc is the function that hanlding messages
// args is composed of context-related parameters
//
// amqp
// args[0] should be amqp.Delivery
//
// redis
// args[0] should be ...
type ActionFunc func(args ...interface{})

type Setup struct {
	ActionFunc ActionFunc
	Url        string
}

type logger interface {
	Debug(args ...interface{})
	Debugf(format string, args ...interface{})
	Info(args ...interface{})
	Infof(format string, args ...interface{})
	Warn(args ...interface{})
	Warnf(format string, args ...interface{})
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
	Panic(args ...interface{})
	Panicf(format string, args ...interface{})
}

// Endpoint represents an endpoint
type Endpoint struct {
	Protocol EndpointProtocol
	Original string
	Redis    struct {
		Host    string
		Port    int
		Channel string
	}
	AMQP struct {
		URI          string
		ExchangeName string
		QueueName    string
		RouteKey     []string
		Ack          bool
		Type         string
	}
}

type Subscriber interface {
	Run()
	Close()
}

// SubscriberManager is a manager to control subscribers
type SubscriberManager struct {
	mu   sync.RWMutex
	subs map[string]Subscriber
	log  logger
}

// NewSubscriberManager creates a mangager
func NewSubscriberManager(log logger) *SubscriberManager {
	sm := &SubscriberManager{
		subs: make(map[string]Subscriber),
		log:  log,
	}
	return sm
}

// Run starts the subscribers that the manager controls
func (sm *SubscriberManager) Run() {
	for name, sub := range sm.subs {
		go func(name string, sub Subscriber) {
			sm.log.Infof(" [-] Subscriber %s is going to initialize", name)
			sub.Run()
		}(name, sub)
	}
}

// GracefulStop stops the manager gracefully. It stops the subscribers from
// accepting new messages and blocks until all the pending messages are
// finished.
func (sm *SubscriberManager) GracefulStop() {
	for _, ep := range sm.subs {
		ep.Close()
	}
}

func (sm *SubscriberManager) Register(name string, setup *Setup) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if _, ok := sm.subs[name]; ok {
		return errors.New("The subscriber %s has been registered before")
	}
	ep, error := parseEndpoint(setup.Url)
	if error != nil {
		return error
	}
	var conn Subscriber
	switch ep.Protocol {
	default:
		return errors.New("invalid protocol")
	case Redis:
		conn = newRedisSubscriber(name, ep, setup, sm.log)
	case AMQP:
		conn = newAMQPSubscriber(name, ep, setup, sm.log)
	}
	sm.subs[name] = conn
	return nil
}

// Validate validates if a url is valid
func (sm *SubscriberManager) Validate(url string) error {
	_, err := parseEndpoint(url)
	return err
}

func parseEndpoint(s string) (Endpoint, error) {
	var endpoint Endpoint
	endpoint.Original = s
	switch {
	default:
		return endpoint, errors.New("unknown scheme")
	case strings.HasPrefix(s, "redis:"):
		endpoint.Protocol = Redis
	case strings.HasPrefix(s, "amqp:"):
		endpoint.Protocol = AMQP
	}

	s = s[strings.Index(s, ":")+1:]
	if !strings.HasPrefix(s, "//") {
		return endpoint, errors.New("missing the two slashes")
	}

	sqp := strings.Split(s[2:], "?")
	sp := strings.Split(sqp[0], "/")
	s = sp[0]
	if s == "" {
		return endpoint, errors.New("missing host")
	}

	if endpoint.Protocol == Redis {
		dp := strings.Split(s, ":")
		switch len(dp) {
		default:
			return endpoint, errors.New("invalid redis url")
		case 1:
			endpoint.Redis.Host = dp[0]
			endpoint.Redis.Port = 6379
		case 2:
			endpoint.Redis.Host = dp[0]
			n, err := strconv.ParseUint(dp[1], 10, 16)
			if err != nil {
				return endpoint, errors.New("invalid redis url port")
			}
			endpoint.Redis.Port = int(n)
		}

		if len(sp) > 1 {
			var err error
			endpoint.Redis.Channel, err = url.QueryUnescape(sp[1])
			if err != nil {
				return endpoint, errors.New("invalid redis channel name")
			}
		}
	}

	// Basic AMQP connection strings
	// amqp://guest:guest@localhost:5672/<exchange_name>/<queue_name>/?params=value
	//
	// Default params are:
	//
	// Mandatory - false
	// Immeditate - false
	// Durable - true
	// Routing-Key - ''
	// Auto-Ack - false
	// Type - direct
	//
	// - "route" - [string] routing key
	// - "ack"   - [bool] auto ack
	// - "type"  - [string] queue type
	//
	if endpoint.Protocol == AMQP {
		// Bind connection information
		endpoint.AMQP.URI = s

		// Bind queue name
		if len(sp) > 2 {
			var err error
			endpoint.AMQP.ExchangeName, err = url.QueryUnescape(sp[1])
			if err != nil {
				return endpoint, errors.New("invalid AMQP exchange name")
			}
			endpoint.AMQP.QueueName, err = url.QueryUnescape(sp[2])
			if err != nil {
				return endpoint, errors.New("invalid AMQP queue name")
			}
		} else {
			return endpoint, errors.New("missing the exchange or queue name")
		}

		// Parsing additional attributes
		if len(sqp) > 1 {
			m, err := url.ParseQuery(sqp[1])
			if err != nil {
				return endpoint, errors.New("invalid AMQP url")
			}
			for key, val := range m {
				if len(val) == 0 {
					continue
				}
				switch key {
				case "route":
					endpoint.AMQP.RouteKey = val
				case "ack":
					if val[0] == "false" {
						endpoint.AMQP.Ack = false
					} else {
						endpoint.AMQP.Ack = true
					}
				case "type":
					if val[0] == "fanout" {
						endpoint.AMQP.Type = "fanout"
					} else {
						endpoint.AMQP.Type = "direct"
					}
				}
			}
		}

		if endpoint.AMQP.QueueName == "" {
			return endpoint, errors.New("missing AMQP queue name")
		}

		if len(endpoint.AMQP.RouteKey) == 0 {
			endpoint.AMQP.RouteKey = []string{""}
		}
		if endpoint.AMQP.Type == "" {
			endpoint.AMQP.Type = "direct"
		}
	}

	return endpoint, nil
}
