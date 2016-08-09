package backends

import (
	"log"
	"sync"
	"time"

	"gopkg.in/redis.v3"
)

var once sync.Once

func newClient(server, password string) *redis.Client {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	options := &redis.Options{
		Network:     "tcp",
		Addr:        server,
		Password:    password,
		DB:          0,
		PoolSize:    1000,
		IdleTimeout: 240 * time.Second,
	}

	return redis.NewClient(options)
}

func newFailoverClient(master, password string, sentinels []string) *redis.Client {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	options := &redis.FailoverOptions{
		MasterName:    master,
		SentinelAddrs: sentinels,
		Password:      password,
		DB:            0,
		PoolSize:      1000,
		IdleTimeout:   240 * time.Second,
	}

	return redis.NewFailoverClient(options)
}

var cachedClient, cachedFailoverClient *redis.Client

func getClient(redis_host, password string) *redis.Client {
	once.Do(func() {
		cachedClient = newClient(redis_host, password)
	})

	return cachedClient
}

func getFailoverClient(master, password string, sentinels []string) *redis.Client {
	once.Do(func() {
		cachedFailoverClient = newFailoverClient(master, password, sentinels)
	})

	return cachedFailoverClient
}
