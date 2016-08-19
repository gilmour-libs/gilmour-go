package backends

import (
	"log"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
)

var once sync.Once

func newPool(server, password string) *redis.Pool {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}

			if password != "" {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			}

			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

var pool *redis.Pool

func getPool(redis_host, password string) *redis.Pool {

	once.Do(func() {
		pool = newPool(redis_host, password)
	})

	return pool
}
