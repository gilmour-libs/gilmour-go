package sentinel

import (
	"log"

	"github.com/keimoon/gore"
)

func getPool(server, password string) (*gore.Pool, error) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	pool := &gore.Pool{
		InitialConn: 1,
		MaximumConn: 10,
		Password:    password,
	}

	if err := pool.Dial(server); err != nil {
		return nil, err
	}

	return pool, nil

}

func getFailoverPool(master, password string, sentinels ...string) (*gore.Pool, error) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	s := gore.NewSentinel()
	s.AddServer(sentinels...)

	if err := s.Dial(); err != nil {
		return nil, err
	}

	return s.GetPoolWithPassword(master, password)
}
