package backends

import (
	"errors"
	"log"
	"strings"
	"sync"

	"github.com/garyburd/redigo/redis"
	"gopkg.in/gilmour-libs/gilmour-e-go.v1/protocol"
)

const defaultErrorQueue = "gilmour.errorqueue"
const defaultErrorTopic = "gilmour.errors"
const defaultHealthTopic = "gilmour.health"
const defaultResponseTopic = "gilmour.response"
const defaultIdentKey = "gilmour.known_host.health"
const defaultErrorBuffer = 9999

func MakeRedis(host, password string) *Redis {
	redisPool := GetPool(host, password)
	return &Redis{
		redisPool:  redisPool,
		pubsubConn: redis.PubSubConn{Conn: redisPool.Get()},
	}
}

type Redis struct {
	redisPool  *redis.Pool
	pubsubConn redis.PubSubConn
	sync.Mutex
}

func (self *Redis) getErrorTopic() string {
	return defaultErrorTopic
}

func (self *Redis) getPubSubConn() redis.PubSubConn {
	return self.pubsubConn
}

func (self *Redis) GetConn() redis.Conn {
	return self.redisPool.Get()
}

func (self *Redis) HasActiveSubscribers(topic string) (bool, error) {
	conn := self.GetConn()
	defer conn.Close()

	data, err := redis.IntMap(conn.Do("PUBSUB", "NUMSUB", topic))
	if err == nil {
		count, has := data[topic]
		return has && count > 0, err
	} else {
		return false, err
	}
}

func (self *Redis) AcquireGroupLock(group, sender string) bool {
	conn := self.GetConn()
	defer conn.Close()

	key := sender + group

	val, err := conn.Do("SET", key, key, "NX", "EX", "600")
	if err != nil {
		return false
	}

	if val == nil {
		return false
	}

	return true
}

func (self *Redis) getErrorQueue() string {
	return defaultErrorQueue
}

func (self *Redis) ReportError(method string, message *protocol.Error) (err error) {
	conn := self.GetConn()
	defer conn.Close()

	switch method {
	case protocol.PUBLISH:
		err = self.Publish(self.getErrorTopic(), message)

	case protocol.QUEUE:
		msg, merr := message.Marshal()
		if merr != nil {
			err = merr
			return
		}

		queue := self.getErrorQueue()
		conn.Send("LPUSH", queue, string(msg))
		conn.Send("LTRIM", queue, 0, defaultErrorBuffer)

		_, err = conn.Receive()

	}

	return err
}

func (self *Redis) Unsubscribe(topic string) (err error) {
	self.Lock()
	defer self.Unlock()

	if strings.HasSuffix(topic, "*") {
		err = self.getPubSubConn().PUnsubscribe(topic)
	} else {
		err = self.getPubSubConn().Unsubscribe(topic)
	}

	return
}

func (self *Redis) Subscribe(topic, group string) (err error) {
	self.Lock()
	defer self.Unlock()

	if strings.HasSuffix(topic, "*") {
		err = self.getPubSubConn().PSubscribe(topic)
	} else {
		err = self.getPubSubConn().Subscribe(topic)
	}

	return
}

func (self *Redis) GetHealthIdent() string {
	return defaultIdentKey
}

func (self *Redis) HealthTopic(ident string) string {
	return defaultHealthTopic + "." + ident
}

func (self *Redis) ResponseTopic(sender string) string {
	return defaultResponseTopic + "." + sender
}

func (self *Redis) Publish(topic string, message interface{}) (err error) {
	var msg string
	switch t := message.(type) {
	case string:
		msg = t
	case protocol.Messenger:
		msg2, err2 := t.Marshal()
		if err != nil {
			err = err2
		} else {
			msg = string(msg2)
		}
	default:
		err = errors.New("Message can only be String or protocol.Messenger")
	}

	if err != nil {
		return
	}

	conn := self.GetConn()
	defer conn.Close()

	_, err = conn.Do("PUBLISH", topic, msg)
	return
}

func (self *Redis) RegisterIdent(uuid string) error {
	conn := self.GetConn()
	defer conn.Close()

	_, err := conn.Do("HSET", self.GetHealthIdent(), uuid, "true")
	return err
}

func (self *Redis) UnregisterIdent(uuid string) error {
	conn := self.GetConn()
	defer conn.Close()

	_, err := conn.Do("HDEL", self.GetHealthIdent(), uuid)
	return err
}

func (self *Redis) Start(sink chan *protocol.Message) {
	self.setupListeners(sink)
}

func (self *Redis) Stop() {
}

func (self *Redis) setupListeners(sink chan *protocol.Message) {
	go func() {
		for {
			switch v := self.getPubSubConn().Receive().(type) {
			case redis.PMessage:
				msg := &protocol.Message{"pmessage", v.Channel, v.Data, v.Pattern}
				sink <- msg
			case redis.Message:
				msg := &protocol.Message{"message", v.Channel, v.Data, v.Channel}
				sink <- msg
			case redis.Subscription:
				//log.Println("PubSub event", "Channel", v.Channel, "Kind", v.Kind, "Count", v.Count)
			case redis.Pong:
				//log.Println("Pong", "Data", v.Data)
			case error:
				log.Println("Error", "message", v.Error())
			}
		}
	}()
}
