package backends

import (
	"errors"
	"log"
	"strings"
	"sync"
	"time"

	"gopkg.in/gilmour-libs/gilmour-e-go.v4/protocol"
	"gopkg.in/redis.v3"
)

const defaultErrorQueue = "gilmour.errorqueue"
const defaultIdentKey = "gilmour.known_host.health"
const defaultErrorBuffer = 9999

func MakeRedis(host, password string) *Redis {
	client := getClient(host, password)
	return &Redis{
		client: client,
		pubsub: client.PubSub(),
	}
}

func MakeRedisSentinel(master, password string, sentinels []string) *Redis {
	client := getFailoverClient(master, password, sentinels)
	return &Redis{
		client: client,
		pubsub: client.PubSub(),
	}
}

type Redis struct {
	client *redis.Client
	pubsub *redis.PubSub
	sync.Mutex
}

func (r *Redis) getClient() *redis.Client {
	return r.client
}

func (r *Redis) getPubSub() *redis.PubSub {
	return r.pubsub
}

func (r *Redis) IsTopicSubscribed(topic string) (bool, error) {
	client := r.getClient()

	response := client.PubSubChannels(topic)

	idents, err2 := response.Result()
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
	client := r.getClient()

	response := client.PubSubNumSub(topic)
	data, err := response.Result()
	if err == nil {
		count, has := data[topic]
		return has && count > 0, err
	} else {
		return false, err
	}
}

func (r *Redis) AcquireGroupLock(group, sender string) bool {
	client := r.getClient()

	key := sender + group

	val, err := client.SetNX(key, key, 600*time.Second).Result()
	if err != nil {
		return false
	}

	return val
}

func (r *Redis) getErrorQueue() string {
	return defaultErrorQueue
}

func (r *Redis) ReportError(method string, message protocol.Error) (err error) {
	pipeline := r.getClient().Pipeline()
	defer pipeline.Close()

	switch method {
	case protocol.ErrorPolicyPublish:
		err = r.Publish(protocol.ErrorTopic, message)

	case protocol.ErrorPolicyQueue:
		msg, merr := message.Marshal()
		if merr != nil {
			err = merr
			return
		}

		queue := r.getErrorQueue()
		pipeline.LPush(queue, string(msg))
		pipeline.LTrim(queue, 0, defaultErrorBuffer)

		_, err = pipeline.Exec()

	}

	return err
}

func (r *Redis) Unsubscribe(topic string) (err error) {
	r.Lock()
	defer r.Unlock()

	if strings.HasSuffix(topic, "*") {
		err = r.getPubSub().PUnsubscribe(topic)
	} else {
		err = r.getPubSub().Unsubscribe(topic)
	}

	return err
}

func (r *Redis) Subscribe(topic, group string) (err error) {
	r.Lock()
	defer r.Unlock()

	if strings.HasSuffix(topic, "*") {
		err = r.getPubSub().PSubscribe(topic)
	} else {
		err = r.getPubSub().Subscribe(topic)
	}

	return err
}

func (r *Redis) getHealthIdent() string {
	return defaultIdentKey
}

func (r *Redis) Publish(topic string, message interface{}) (err error) {
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

	client := r.getClient()

	_, err = client.Publish(topic, msg).Result()
	return
}

func (r *Redis) ActiveIdents() (map[string]string, error) {
	client := r.getClient()

	return client.HGetAllMap(r.getHealthIdent()).Result()
}

func (r *Redis) RegisterIdent(uuid string) error {
	client := r.getClient()

	_, err := client.HSet(r.getHealthIdent(), uuid, "true").Result()
	return err
}

func (r *Redis) UnregisterIdent(uuid string) error {
	client := r.getClient()

	_, err := client.HDel(r.getHealthIdent(), uuid).Result()
	return err
}

func (r *Redis) Start(sink chan<- *protocol.Message) {
	r.setupListeners(sink)
}

func (r *Redis) Stop() {
}

func (r *Redis) setupListeners(sink chan<- *protocol.Message) {
	go func() {
		for {
			resp, _ := r.getPubSub().Receive()
			switch v := resp.(type) {
			case *redis.PMessage:
				msg := &protocol.Message{"pmessage", v.Channel, v.Payload, v.Pattern}
				sink <- msg
			case *redis.Message:
				msg := &protocol.Message{"message", v.Channel, v.Payload, v.Channel}
				sink <- msg
			case *redis.Subscription:
				//log.Println("PubSub event", "Channel", v.Channel, "Kind", v.Kind, "Count", v.Count)
			case *redis.Pong:
				//log.Println("Pong", "Data", v.Data)
			case error:
				log.Println("Error", "message", v.Error())
			}
		}
	}()
}
