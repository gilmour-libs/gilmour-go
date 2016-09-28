package sentinel

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"

	"gopkg.in/gilmour-libs/gilmour-e-go.v5/proto"

	"github.com/keimoon/gore"
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
	pool, _ := getPool(host, password)
	pubsubConn, _ := pool.Acquire()
	pubsub := gore.NewSubscriptions(pubsubConn)
	return &Redis{
		pool:       pool,
		pubsub:     pubsub,
		pubsubConn: pubsubConn,
	}
}

func MakeRedisSentinel(master, password string, sentinels ...string) *Redis {
	pool, _ := getFailoverPool(master, password, sentinels...)
	pubsubConn, _ := pool.Acquire()
	pubsub := gore.NewSubscriptions(pubsubConn)
	return &Redis{
		pool:       pool,
		pubsub:     pubsub,
		pubsubConn: pubsubConn,
	}
}

type Redis struct {
	errorPolicy string
	pool        *gore.Pool
	pubsub      *gore.Subscriptions
	pubsubConn  *gore.Conn
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

func (r *Redis) getConn() (*gore.Conn, error) {
	return r.pool.Acquire()
}

func (r *Redis) Close(conn *gore.Conn) {
	r.pool.Release(conn)
}

func (r *Redis) IsTopicSubscribed(topic string) (bool, error) {
	conn, err := r.getConn()
	if err != nil {
		return false, err
	}
	defer r.Close(conn)

	if err != nil {
		return false, err
	}

	resp, err := gore.NewCommand("PUBSUB", "CHANNELS").Run(conn)

	var idents []string

	if err := resp.Slice(&idents); err != nil {
		log.Println(err.Error())
		return false, err
	}

	for _, t := range idents {
		if t == topic {
			return true, nil
		}
	}

	return false, nil
}

func (r *Redis) HasActiveSubscribers(topic string) (bool, error) {
	conn, err := r.getConn()
	if err != nil {
		return false, err
	}
	defer r.Close(conn)

	resp, err := gore.NewCommand("PUBSUB", "NUMSUB", topic).Run(conn)
	if err != nil {
		return false, err
	}

	data, err2 := resp.Array()

	if err2 == nil {
		count, _ := data[1].Int()
		return count > 0, err
	} else {
		return false, err
	}
}

func (r *Redis) AcquireGroupLock(group, sender string) bool {
	conn, err := r.getConn()
	if err != nil {
		return false
	}
	defer r.Close(conn)

	key := sender + group

	resp, err := gore.NewCommand("SET", key, key, "NX", "EX", "600").Run(conn)
	if err != nil {
		return false
	}

	if resp.IsOk() {
		return true
	}

	return false
}

func (r *Redis) getErrorQueue() string {
	return defaultErrorQueue
}

func (r *Redis) ReportError(method string, message *proto.GilmourError) (err error) {
	conn, err := r.getConn()
	if err != nil {
		return
	}
	defer r.Close(conn)

	pipeline := gore.NewPipeline()

	switch method {
	case errorPolicyPublish:
		err = gore.Publish(conn, errorTopic, *message)

	case errorPolicyQueue:
		msg, merr := (*message).Marshal()
		if merr != nil {
			err = merr
			return
		}

		queue := r.getErrorQueue()
		pipeline.Add(gore.NewCommand("LPUSH", queue, string(msg)))
		pipeline.Add(gore.NewCommand("LTRIM", queue, 0, defaultErrorBuffer))

		_, err = pipeline.Run(conn)

		if err != nil {
			return err
		}

		_, err = gore.Receive(conn)

	}

	return
}

func (r *Redis) Unsubscribe(topic string) (err error) {
	r.Lock()
	defer r.Unlock()

	if strings.HasSuffix(topic, "*") {
		err = r.pubsub.PUnsubscribe(topic)
	} else {
		err = r.pubsub.Unsubscribe(topic)
	}

	return err
}

func (r *Redis) Subscribe(topic, group string) error {
	r.Lock()
	defer r.Unlock()

	var err error

	if strings.HasSuffix(topic, "*") {
		err = r.pubsub.PSubscribe(topic)
	} else {
		err = r.pubsub.Subscribe(topic)
	}

	return err
}

func (r *Redis) getHealthIdent() string {
	return defaultIdentKey
}

func (r *Redis) Publish(topic string, message interface{}) (sent bool, err error) {
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
		err = errors.New("Message can only be String or protocol.Messenger")
	}

	if err != nil {
		return
	}

	conn, err := r.getConn()
	if err != nil {
		return
	}
	defer r.Close(conn)

	resp, err := gore.NewCommand("PUBLISH", topic, msg).Run(conn)
	num, numErr := resp.Int()
	if numErr != nil {
		log.Println("Response for Publish did not contain an Integer. Foud %v", numErr)
	}

	return num > 0, err
}

func (r *Redis) ActiveIdents() (map[string]string, error) {
	conn, err := r.getConn()
	if err != nil {
		return nil, err
	}
	defer r.Close(conn)

	resp, err := gore.NewCommand("HGETALL", r.getHealthIdent()).Run(conn)
	if err != nil {
		return nil, err
	}

	return resp.Map()
}

func (r *Redis) RegisterIdent(uuid string) error {
	conn, err := r.getConn()
	if err != nil {
		return err
	}
	defer r.Close(conn)

	_, err = gore.NewCommand("HSET", r.getHealthIdent(), uuid, "true").Run(conn)
	return err
}

func (r *Redis) UnregisterIdent(uuid string) error {
	conn, err := r.getConn()
	if err != nil {
		return err
	}
	defer r.Close(conn)

	_, err = gore.NewCommand("HDEL", r.getHealthIdent(), uuid).Run(conn)
	return err
}

func (r *Redis) Start(sink chan<- *proto.Packet) {
	r.setupListeners(sink)
}

func (r *Redis) Stop() {
	r.Close(r.pubsubConn)
	r.pool.Close()
}

func (r *Redis) setupListeners(sink chan<- *proto.Packet) {
	go func() {
		for message := range r.pubsub.Message() {
			if message == nil {
				break
			}

			msg := proto.NewPacket(message.Type, message.OriginalChannel, message.Channel, message.Message)
			sink <- msg
		}
	}()
}
