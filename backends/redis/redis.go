package redis

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"

	"gopkg.in/gilmour-libs/gilmour-e-go.v5/proto"

	"github.com/garyburd/redigo/redis"
)

const (
	defaultErrorQueue  = "gilmour.errorqueue"
	defaultIdentKey    = "gilmour.known_host.health"
	defaultErrorBuffer = 9999
	errorPolicyQueue   = "queue"
	errorPolicyPublish = "publish"
	errorPolicyIgnore  = ""
	errorTopic         = "gilmour.errors"
)

func MakeRedis(host, password string) *Redis {
	redisPool := getPool(host, password)
	return &Redis{
		redisPool:  redisPool,
		pubsubConn: redis.PubSubConn{Conn: redisPool.Get()},
	}
}

type Redis struct {
	errorPolicy string
	redisPool   *redis.Pool
	pubsubConn  redis.PubSubConn
	sync.Mutex
}

func (r *Redis) SupportedErrorPolicies() []string {
	return []string{errorPolicyQueue, errorPolicyPublish, errorPolicyIgnore}
}

func (r *Redis) SetErrorPolicy(policy string) error {
	if policy != errorPolicyQueue &&
		policy != errorPolicyPublish &&
		policy != errorPolicyIgnore {
		return errors.New(fmt.Sprintf("Invalid error policy"))
	}

	r.errorPolicy = policy
	return nil
}

func (r *Redis) GetErrorPolicy() string {
	return r.errorPolicy
}

func (r *Redis) getPubSubConn() redis.PubSubConn {
	return r.pubsubConn
}

func (r *Redis) getConn() redis.Conn {
	return r.redisPool.Get()
}

func (r *Redis) IsTopicSubscribed(topic string) (bool, error) {
	conn := r.getConn()
	defer conn.Close()

	idents, err2 := redis.Strings(conn.Do("PUBSUB", "CHANNELS"))
	if err2 != nil {
		log.Println(err2.Error())
		return false, err2
	}

	for _, t := range idents {
		if t == topic {
			return true, nil
		}
	}

	return false, nil
}

func (r *Redis) HasActiveSubscribers(topic string) (bool, error) {
	conn := r.getConn()
	defer conn.Close()

	data, err := redis.IntMap(conn.Do("PUBSUB", "NUMSUB", topic))
	if err == nil {
		count, has := data[topic]
		return has && count > 0, err
	} else {
		return false, err
	}
}

func (r *Redis) AcquireGroupLock(group, sender string) bool {
	conn := r.getConn()
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

func (r *Redis) getErrorQueue() string {
	return defaultErrorQueue
}

func (r *Redis) ReportError(method string, message *proto.GilmourError) (err error) {
	conn := r.getConn()
	defer conn.Close()

	switch method {
	case errorPolicyPublish:
		err = r.Publish(errorTopic, *message)

	case errorPolicyQueue:
		msg, merr := (*message).Marshal()
		if merr != nil {
			err = merr
			return
		}

		queue := r.getErrorQueue()
		conn.Send("LPUSH", queue, string(msg))
		conn.Send("LTRIM", queue, 0, defaultErrorBuffer)

		_, err = conn.Receive()

	}

	return err
}

func (r *Redis) Unsubscribe(topic string) (err error) {
	r.Lock()
	defer r.Unlock()

	if strings.HasSuffix(topic, "*") {
		err = r.getPubSubConn().PUnsubscribe(topic)
	} else {
		err = r.getPubSubConn().Unsubscribe(topic)
	}

	return
}

func (r *Redis) Subscribe(topic, group string) (err error) {
	r.Lock()
	defer r.Unlock()

	if strings.HasSuffix(topic, "*") {
		err = r.getPubSubConn().PSubscribe(topic)
	} else {
		err = r.getPubSubConn().Subscribe(topic)
	}

	return
}

func (r *Redis) getHealthIdent() string {
	return defaultIdentKey
}

func (r *Redis) Publish(topic string, message interface{}) (err error) {
	var msg string
	switch t := message.(type) {
	case string:
		msg = t
	case proto.BackendWriter:
		msg2, err2 := t.Marshal()
		if err != nil {
			err = err2
		} else {
			msg = string(msg2)
		}
	default:
		err = errors.New("Message can only be String or WireWriter")
	}

	if err != nil {
		return
	}

	conn := r.getConn()
	defer conn.Close()

	_, err = conn.Do("PUBLISH", topic, msg)
	return
}

func (r *Redis) ActiveIdents() (map[string]string, error) {
	conn := r.getConn()
	defer conn.Close()

	return redis.StringMap(conn.Do("HGETALL", r.getHealthIdent()))
}

func (r *Redis) RegisterIdent(uuid string) error {
	conn := r.getConn()
	defer conn.Close()

	_, err := conn.Do("HSET", r.getHealthIdent(), uuid, "true")
	return err
}

func (r *Redis) UnregisterIdent(uuid string) error {
	conn := r.getConn()
	defer conn.Close()

	_, err := conn.Do("HDEL", r.getHealthIdent(), uuid)
	return err
}

func (r *Redis) Start(sink chan<- *proto.Packet) {
	r.setupListeners(sink)
}

func (r *Redis) Stop() {
}

func (r *Redis) setupListeners(sink chan<- *proto.Packet) {
	go func() {
		for {
			switch v := r.getPubSubConn().Receive().(type) {
			case redis.PMessage:
				msg := proto.NewPacket("pmessage", v.Channel, v.Pattern, v.Data)
				sink <- msg
			case redis.Message:
				msg := proto.NewPacket("message", v.Channel, v.Channel, v.Data)
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
