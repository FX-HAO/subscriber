package subscriber

import (
	"sync"

	"github.com/go-redis/redis"
)

type RedisSubscriber struct {
	*Endpoint
	log logger
	mu   sync.RWMutex
	conn *redis.Conn
	exec ActionFunc

	drain bool
}

func newRedisSubscriber(ep Endpoint, setup *Setup, logger logger) *RedisSubscriber {
	return nil
}

func (rs *RedisSubscriber) Close() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if !rs.drain && rs.conn != nil {
		rs.conn.Close()
		rs.conn = nil
	}
}

func (rs *RedisSubscriber) Run() {

}
