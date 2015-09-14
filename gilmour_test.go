package gilmour

import (
	"fmt"
	golog "log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	redigo "github.com/garyburd/redigo/redis"
	"gopkg.in/gilmour-libs/gilmour-e-go.v0/backends"
)

const (
	PingTopic    = "ping"
	PingResponse = "pong"
	SleepTopic   = "sleepy-ping"
)

var engine *Gilmour
var redis *backends.Redis

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	rand.Seed(time.Now().UTC().UnixNano())
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func isReplySubscribed(topic string) (bool, error) {
	return isTopicSubscribed(topic, false)
}

func isSlotSubscribed(topic string) (bool, error) {
	return isTopicSubscribed(topic, true)
}

func isTopicSubscribed(topic string, is_slot bool) (bool, error) {
	if is_slot {
		topic = engine.slotDestination(topic)
	} else {
		topic = engine.requestDestination(topic)
	}

	conn := redis.GetConn()
	defer conn.Close()

	idents, err2 := redigo.Strings(conn.Do("PUBSUB", "CHANNELS"))
	if err2 != nil {
		return false, err2
	}

	for _, t := range idents {
		if t == topic {
			return true, nil
		}
	}

	return false, nil
}

func TestHealthSubscribe(t *testing.T) {
	engine.SetHealthCheckEnabled()

	topic := redis.HealthTopic(engine.GetIdent())
	if has, _ := isReplySubscribed(topic); !has {
		t.Error(topic, "should have been subscribed")
	}
}

func TestSubscribePing(t *testing.T) {
	timeout := 3
	handler_opts := MakeHandlerOpts().SetTimeout(timeout)
	sub, err := engine.ReplyTo(PingTopic, func(req *Request, resp *Response) {
		var x string
		req.Data(&x)
		req.Logger.Debug(PingTopic, "Received", x)
		resp.Send(PingResponse)
	}, handler_opts)

	if err != nil {
		t.Error("Error Subscribing", PingTopic, err.Error())
		return
	}

	actualTimeout := sub.GetOpts().GetTimeout()

	if actualTimeout != timeout {
		t.Error("Handler should have timeout of", timeout, "seconds. Found", actualTimeout)
	}

	if has, _ := isReplySubscribed(PingTopic); !has {
		t.Error("Topic", PingTopic, "should have been subscribed")
	}
}

func TestWildcardSlot(t *testing.T) {
	opts := MakeHandlerOpts().SetGroup("wildcard_group")
	topic := fmt.Sprintf("%v*", PingTopic)
	_, err := engine.Slot(topic, func(req *Request, resp *Response) {}, opts)
	if err != nil {
		t.Error("Error Subscribing", PingTopic, err.Error())
	}
}

func TestWildcardReply(t *testing.T) {
	topic := fmt.Sprintf("%v*", PingTopic)
	_, err := engine.ReplyTo(topic, func(req *Request, resp *Response) {}, nil)
	if err == nil {
		t.Error("ReplyTo cannot have wildcard topics.")
	}
}

func TestPublisherSleep(t *testing.T) {
	_, err := engine.ReplyTo(
		SleepTopic,
		func(req *Request, resp *Response) {
			var delay int
			req.Data(&delay)
			time.Sleep(time.Duration(delay) * time.Second)
			resp.Send(PingResponse)
		}, nil,
	)

	if err != nil {
		t.Error("Error Subscribing", SleepTopic, err.Error())
		return
	}

	if has, _ := isReplySubscribed(SleepTopic); !has {
		t.Error("Topic", SleepTopic, "should have been subscribed")
	}
}

func TestHealthGetAll(t *testing.T) {
	conn := redis.GetConn()
	defer conn.Close()

	idents, err := redigo.StringMap(conn.Do("HGETALL", redis.GetHealthIdent()))
	if err != nil {
		t.Error(err)
	}

	val, ok := idents[engine.GetIdent()]
	if !ok || val != "true" {
		t.Error("Ident is missing in the Health ident")
	}
}

func TestUnsubscribe(t *testing.T) {
	topic := randSeq(10)
	sub, err := engine.Slot(topic, func(req *Request, resp *Response) {}, nil)
	if err != nil {
		t.Error("Error Subscribing", topic, err.Error())
		return
	}

	if has, _ := isSlotSubscribed(topic); !has {
		t.Error(topic, "should have been subscribed")
	}

	engine.UnsubscribeSlot(topic, sub)

	if has, _ := isSlotSubscribed(topic); has {
		t.Error("Topic", topic, "should have been unsubscribed")
	}
}

func TestRelayFromSubscriber(t *testing.T) {
}

func TestTwiceSlot(t *testing.T) {
	topic := randSeq(10)
	opts := MakeHandlerOpts()

	subs := []*Subscription{}
	defer func() {
		for _, s := range subs {
			engine.UnsubscribeSlot(topic, s)
		}
	}()

	for i := 0; i < 2; i++ {
		sub, err := engine.Slot(topic, func(_ *Request, _ *Response) {}, opts)
		if sub != nil {
			subs = append(subs, sub)
		} else if err != nil {
			t.Error("Cannot subscribe. Error", err.Error())
		}
	}
}

func TestTwiceSlotFail(t *testing.T) {
	topic := randSeq(10)
	count := 2
	opts := MakeHandlerOpts().SetGroup("unique")

	subs := []*Subscription{}
	defer func() {
		for _, s := range subs {
			engine.UnsubscribeSlot(topic, s)
		}
	}()

	for i := 0; i < count; i++ {
		sub, err := engine.Slot(topic, func(_ *Request, _ *Response) {}, opts)
		if sub != nil {
			subs = append(subs, sub)
		} else if i == 1 {
			if err == nil {
				t.Error("Should now allow second Subscription citing duplication.")
			}
		} else if err != nil {
			t.Error("Cannot subscribe. Error", err.Error())
		}
	}
}

func TestTwiceReplyToFail(t *testing.T) {
	topic := randSeq(10)
	count := 2
	opts := MakeHandlerOpts() //Note, we did not pass a group.

	subs := []*Subscription{}
	defer func() {
		for _, s := range subs {
			engine.UnsubscribeReply(topic, s)
		}
	}()

	for i := 0; i < count; i++ {
		sub, err := engine.ReplyTo(topic, func(_ *Request, _ *Response) {}, opts)
		if sub != nil {
			subs = append(subs, sub)
		} else if i == 1 {
			if err == nil {
				t.Error("Should now allow second Subscription citing duplication.")
			}
		} else if err != nil {
			t.Error("Cannot subscribe. Error", err.Error())
		}
	}
}

func TestSendOnceReceiveTwice(t *testing.T) {
	topic := randSeq(10)
	count := 2

	out := []string{}
	subs := []*Subscription{}
	// cleanup. Unsubscribe from all subscribed channels.
	defer func() {
		for _, sub := range subs {
			engine.UnsubscribeSlot(topic, sub)
		}

		// Confirm that the topic was Indeed unsubscribed.
		if has, _ := isSlotSubscribed(topic); has {
			t.Error("Topic", topic, "should have been unsubscribed")
		}
	}()

	out_chan := make(chan string, count)

	// Subscribe x no. of times
	for i := 0; i < count; i++ {
		data := fmt.Sprintf("hello %v", i)
		opts := MakeHandlerOpts().SetGroup(randSeq(10))

		sub, err := engine.Slot(topic, func(_ *Request, _ *Response) {
			out_chan <- data
		}, opts)

		if err != nil {
			t.Error("Error Subscribing", err.Error())
			return
		}

		subs = append(subs, sub)
	}

	//Publish a message to random topic
	engine.Signal(topic, NewResponse().SetData("ping?"))

	// Select Case, once and that should work.
	for i := 0; i < count; i++ {
		select {
		case result := <-out_chan:
			out = append(out, result)
		case <-time.After(time.Second * 2):
			t.Error("Response should be twice, timed out instead")
		}
	}

	// Results should be received twice.
	if len(out) != count {
		t.Error("Response should be returned ", count, "items. Found", out)
	}
}

func TestHealthResponse(t *testing.T) {
	out_chan := make(chan string, 1)

	data := NewResponse().SetData("is-healthy?")

	opts := NewRequestOpts().SetHandler(func(req *Request, resp *Response) {
		x := []string{}
		req.Data(&x)

		if len(x) > 0 {
			out_chan <- "healthy"
		} else {
			out_chan <- "false"
		}
	})

	_, err := engine.Request(redis.HealthTopic(engine.GetIdent()), data, opts)
	if err != nil {
		t.Error(err)
	}

	select {
	case result := <-out_chan:
		if result != "healthy" {
			t.Error("Response should be healthy. Found", result)
		}
	case <-time.After(time.Second * 5):
		t.Error("Response should be", PingResponse, "timed out instead")
	}
}

func TestReceiveOnWildcard(t *testing.T) {
	topic := fmt.Sprintf("%v*", PingTopic)
	out_chan := make(chan string, 1)

	//Subscribe to the wildcard topic.
	sub, _ := engine.Slot(
		topic,
		func(_ *Request, _ *Response) {
			out_chan <- PingResponse
		},
		nil,
	)

	//Publish a message to random topic
	engine.Signal("pingworld", NewResponse().SetData("ping?"))

	// Select Case, once and that should work.
	select {
	case <-out_chan:
		// True Case.
	case <-time.After(time.Second * 2):
		t.Error("Response should be received, timed out instead")
	}

	// cleanup. Unsubscribe from all subscribed channels.
	engine.UnsubscribeSlot(topic, sub)

	// Confirm that the topic was Indeed unsubscribed.
	if has, _ := isSlotSubscribed(topic); has {
		t.Error("Topic", topic, "should have been unsubscribed")
	}
}

func TestSendAndReceive(t *testing.T) {
	out_chan := make(chan string, 1)

	data := NewResponse().SetData("ping?")

	opts := NewRequestOpts().SetHandler(func(req *Request, resp *Response) {
		var x string
		req.Data(&x)
		out_chan <- x
	})

	_, err := engine.Request(PingTopic, data, opts)
	if err != nil {
		t.Error(err)
	}

	select {
	case result := <-out_chan:
		if result != PingResponse {
			t.Error("Response should be", PingResponse, "Found", result)
		}
	case <-time.After(time.Second * 5):
		t.Error("Response should be", PingResponse, "timed out instead")
	}
}

func TestPublisherTimeout(t *testing.T) {
	out_chan := make(chan string, 1)

	sleepFor := 5

	data := NewResponse().SetData(sleepFor)
	opts := NewRequestOpts().SetHandler(func(req *Request, resp *Response) {
		var x string
		req.Data(&x)
		out_chan <- x
	}).SetTimeout(2)

	_, err := engine.Request(SleepTopic, data, opts)
	if err != nil {
		t.Error(err)
	}

	select {
	case result := <-out_chan:
		if result != "Execution timed out" {
			t.Error("Response should be 'Execution timed out' Found", result)
		}
	case <-time.After(time.Second * time.Duration(sleepFor)):
		t.Error("Response should be", "Execution timed out", "timed out instead")
	}
}

func TestNoPublisher(t *testing.T) {
	_, err := engine.Signal(PingTopic, nil)
	if err == nil || !strings.Contains(err.Error(), "provide publisher") {
		t.Error("Must provide publisher to be published")
	}
}

func TestSubscriberTimeout(t *testing.T) {
	out_chan := make(chan string, 1)
	topic := "sleep_delayed"

	sub, err := engine.ReplyTo(
		topic,
		func(req *Request, resp *Response) {
			time.Sleep(time.Second * 4)
			resp.Send(PingResponse)
		},
		MakeHandlerOpts().SetTimeout(3),
	)

	if err == nil {
		t.Error("Error Subscribing", PingTopic, err.Error())
		return
	}

	data := NewResponse().SetData("send")

	opts := NewRequestOpts().SetHandler(func(req *Request, resp *Response) {
		var x string
		req.Data(&x)
		out_chan <- x
	})

	engine.Request(topic, data, opts)

	select {
	case result := <-out_chan:
		if result != "Execution timed out" {
			t.Error("Response should be", PingResponse, "Found", result)
		}
	case <-time.After(time.Second * 5):
		t.Error("Response should be", PingResponse, "timed out instead")
	}

	engine.UnsubscribeReply(topic, sub)
}

func TestHandlerException(t *testing.T) {
	out_chan := make(chan int, 1)
	topic := randSeq(10)

	sub, err := engine.ReplyTo(topic, func(req *Request, resp *Response) {
		// Just to induce errors, access a null pointers's method.
		var x *HandlerOpts
		golog.Println(x.GetGroup())
	}, nil)

	if err == nil {
		t.Error("Error Subscribing", PingTopic, err.Error())
		return
	}

	data := NewResponse().SetData("send")

	opts := NewRequestOpts().SetHandler(func(req *Request, resp *Response) {
		out_chan <- req.Code()
	})

	engine.Request(topic, data, opts)

	select {
	case result := <-out_chan:
		if result != 500 {
			t.Error("Response should raise exception")
		}
	case <-time.After(time.Second * 5):
		t.Error("Response should have raised Exception, timed out instead")
	}

	engine.UnsubscribeReply(topic, sub)
}

func TestSansListener(t *testing.T) {
	data := NewResponse().SetData("ping?")
	if _, err := engine.Signal("ping-sans-listener", data); err != nil {
		t.Error(err)
	}
}

func TestConfirmSansListener(t *testing.T) {
	data := NewResponse().SetData("ping?")
	opts := NewRequestOpts().SetHandler(func(req *Request, resp *Response) {})

	if _, err := engine.Request("ping-confirm-sans-listener", data, opts); err != nil {
		if !strings.Contains(err.Error(), "active subscribers") {
			t.Error(err.Error())
		}
	}
}

func waitBeforeExiting(interval int) {
	var wg sync.WaitGroup
	wg.Add(1)

	time.AfterFunc(time.Duration(interval)*time.Second, func() {
		wg.Done()
	})

	wg.Wait()
}

func TestMain(m *testing.M) {
	redis = backends.MakeRedis("127.0.0.1:6379")
	engine = Get(redis)

	engine.Start()

	status := m.Run()

	waitBeforeExiting(2) //Wait 5 seconds before exiting.
	engine.Stop()
	waitBeforeExiting(2) //Wait 2 more seconds before exiting.

	os.Exit(status)
}
