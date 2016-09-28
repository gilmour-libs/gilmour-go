package backends

import "gopkg.in/gilmour-libs/gilmour-e-go.v5/backends/sentinel"

func MakeRedis(host, password string) *sentinel.Redis {
	return sentinel.MakeRedis(host, password)
}
