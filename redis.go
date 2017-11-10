package subscriber

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis"
)

// RedisSubscriber represents a subscriber, which consumes messages from redis
type RedisSubscriber struct {
	*Endpoint
	mu  sync.RWMutex
	wg  *sync.WaitGroup
	log logger

	name   string
	client *redis.Client
	exec   ActionFunc

	closed int32 // accessed atomically (non-zero means we're closed)
}

func newRedisSubscriber(name string, ep Endpoint, setup *Setup, logger logger) *RedisSubscriber {
	return &RedisSubscriber{
		name:     name,
		Endpoint: &ep,
		exec:     setup.ActionFunc,
		log:      logger,
		wg:       &sync.WaitGroup{},
	}
}

func (sub *RedisSubscriber) isClosed() bool {
	return atomic.LoadInt32(&sub.closed) != 0
}

// Close closes the subscriber gracefully, it blocks until all messages are finished
func (sub *RedisSubscriber) Close() {
	if atomic.AddInt32(&sub.closed, 1) == 1 {
		sub.mu.Lock()
		if sub.client != nil {
			sub.client.Close()
			sub.client = nil
		}
		sub.mu.Unlock()

		sub.wg.Wait()
		sub.log.Infof(" [-] Subscriber %s is now safe to shutdown", sub.name)
	}
}

// Run starts the subscriber and blocks until the subscriber is closed
func (sub *RedisSubscriber) Run() {
	if sub.isClosed() {
		return
	}

	sub.mu.Lock()
	if sub.client == nil {
		sub.client = redis.NewClient(&redis.Options{
			Addr:     sub.Redis.Addr,
			Password: sub.Redis.Password, // no password set
		})
	}
	sub.mu.Unlock()

	pubsub := sub.client.Subscribe(sub.Redis.Channels...)
	if _, err := pubsub.ReceiveTimeout(time.Second); err != nil {
		panic(err)
	}

	for {
		if sub.isClosed() {
			break
		}
		ch := pubsub.Channel()
		for msg := range ch {
			sub.wg.Add(1)
			go func() {
				sub.exec(msg)
				sub.wg.Done()
			}()
		}
	}
}
