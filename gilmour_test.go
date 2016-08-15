package gilmour

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"./backends"
	"./ui"
)

const (
	PingTopic    = "ping"
	PingResponse = "pong"
	SleepTopic   = "sleepy-ping"
)

var engine *Gilmour

var redis = backends.MakeRedis("127.0.0.1:6379", "")

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

	return redis.IsTopicSubscribed(topic)
}

func TestHealthSubscribe(t *testing.T) {
	engine.SetHealthCheckEnabled()

	topic := healthTopic(engine.getIdent())
	if has, _ := isReplySubscribed(topic); !has {
		t.Error(topic, "should have been subscribed")
	}
}

func TestSubscribePing(t *testing.T) {
	timeout := 3
	handler_opts := NewHandlerOpts().SetTimeout(timeout)
	sub, err := engine.ReplyTo(PingTopic, func(req *Request, resp *Message) {
		var x string
		req.Data(&x)
		ui.Message("Topic %v Received %v", PingTopic, x)
		ui.Message("Topic %v Sending %v", PingTopic, PingResponse)
		resp.SetData(PingResponse)
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

func TestSubscribePingResponse(t *testing.T) {
	req := engine.NewRequest(PingTopic)
	resp, err := req.Execute(NewMessage().SetData("ping?"))
	if err != nil {
		t.Error("Error in response", err.Error())
		return
	}

	x := resp.Next()
	var recv string
	x.GetData(&recv)

	if recv != PingResponse {
		t.Error("Expecting", PingResponse, "Found", recv)
	}
}

func TestSubscribePingSync(t *testing.T) {
	req := engine.NewRequest(PingTopic)
	response, err := req.Execute(NewMessage().SetData("ping?"))
	if err != nil {
		t.Error("Error in sync response", err.Error())
		return
	}

	msg := response.Next()
	var recv string
	msg.GetData(&recv)

	if recv != PingResponse {
		t.Error("Expecting", PingResponse, "Found", recv)
	}
}

func TestWildcardSlot(t *testing.T) {
	opts := NewHandlerOpts().SetGroup("wildcard_group")
	topic := fmt.Sprintf("%v*", PingTopic)
	_, err := engine.Slot(topic, func(*Request) {}, opts)
	if err != nil {
		t.Error("Error Subscribing", PingTopic, err.Error())
	}
}

func TestWildcardReply(t *testing.T) {
	topic := fmt.Sprintf("%v*", PingTopic)
	_, err := engine.ReplyTo(topic, func(req *Request, resp *Message) {}, nil)
	if err == nil {
		t.Error("ReplyTo cannot have wildcard topics.")
	}
}

func TestPublisherSleep(t *testing.T) {
	_, err := engine.ReplyTo(
		SleepTopic,
		func(req *Request, resp *Message) {
			var delay int
			req.Data(&delay)
			time.Sleep(time.Duration(delay) * time.Second)
			resp.SetData(PingResponse)
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
	idents, err := redis.ActiveIdents()
	if err != nil {
		t.Error(err)
	}

	val, ok := idents[engine.getIdent()]
	if !ok || val != "true" {
		t.Error("Ident is missing in the Health ident")
	}
}

func TestUnsubscribe(t *testing.T) {
	topic := randSeq(10)
	sub, err := engine.Slot(topic, func(req *Request) {}, nil)
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
	opts := NewHandlerOpts()

	subs := []*Subscription{}
	defer func() {
		for _, s := range subs {
			engine.UnsubscribeSlot(topic, s)
		}
	}()

	for i := 0; i < 2; i++ {
		sub, err := engine.Slot(topic, func(_ *Request) {}, opts)
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
	opts := NewHandlerOpts().SetGroup("unique")

	subs := []*Subscription{}
	defer func() {
		for _, s := range subs {
			engine.UnsubscribeSlot(topic, s)
		}
	}()

	for i := 0; i < count; i++ {
		sub, err := engine.Slot(topic, func(_ *Request) {}, opts)
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
	opts := NewHandlerOpts() //Note, we did not pass a group.

	subs := []*Subscription{}
	defer func() {
		for _, s := range subs {
			engine.UnsubscribeReply(topic, s)
		}
	}()

	for i := 0; i < count; i++ {
		sub, err := engine.ReplyTo(topic, func(_ *Request, _ *Message) {}, opts)
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

type slotStruct struct {
	Args []string `json:"args"`
}

type failedSlotStruct struct {
	args []string `json:"args"`
}

func TestSignalSlotStruct(t *testing.T) {
	topic := randSeq(10)
	out_chan := make(chan *slotStruct, 1)
	opts := NewHandlerOpts()

	sub, err := engine.Slot(topic, func(req *Request) {
		var x slotStruct
		req.Data(&x)
		out_chan <- &x
	}, opts)

	defer engine.UnsubscribeSlot(topic, sub)

	if err != nil {
		t.Error(err.Error())
		return
	}

	data := slotStruct{[]string{"a", "b", "c"}}

	//Publish a message to random topic
	engine.Signal(topic, NewMessage().SetData(data))

	// Select Case, once and that should work.
	select {
	case result := <-out_chan:
		if len(result.Args) != 3 {
			t.Error("Should have received 3 strings in data")
		}
	case <-time.After(time.Second * 2):
		t.Error("Message should be twice, timed out instead")
	}
}

// Should fail because of unexported field in struct
func TestSignalSlotStructFailed(t *testing.T) {
	topic := randSeq(10)
	out_chan := make(chan *failedSlotStruct, 1)
	opts := NewHandlerOpts()

	sub, err := engine.Slot(topic, func(req *Request) {
		var x failedSlotStruct
		req.Data(&x)
		out_chan <- &x
	}, opts)

	defer engine.UnsubscribeSlot(topic, sub)

	if err != nil {
		t.Error(err.Error())
		return
	}

	data := failedSlotStruct{[]string{"a", "b", "c"}}

	//Publish a message to random topic
	engine.Signal(topic, NewMessage().SetData(data))

	// Select Case, once and that should work.
	select {
	case result := <-out_chan:
		if len(result.args) != 0 {
			t.Error("Should not have received any string in data")
		}
	case <-time.After(time.Second * 2):
		t.Error("Message should be twice, timed out instead")
	}

	engine.UnsubscribeSlot(topic, sub)
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
		opts := NewHandlerOpts().SetGroup(randSeq(10))

		sub, err := engine.Slot(topic, func(_ *Request) {
			out_chan <- data
		}, opts)

		if err != nil {
			t.Error("Error Subscribing", err.Error())
			return
		}

		subs = append(subs, sub)
	}

	//Publish a message to random topic
	engine.Signal(topic, NewMessage().SetData("ping?"))

	// Select Case, once and that should work.
	for i := 0; i < count; i++ {
		select {
		case result := <-out_chan:
			out = append(out, result)
		case <-time.After(time.Second * 2):
			t.Error("Message should be twice, timed out instead")
		}
	}

	// Results should be received twice.
	if len(out) != count {
		t.Error("Message should be returned ", count, "items. Found", out)
	}
}

func TestHealthResponse(t *testing.T) {
	out_chan := make(chan string, 1)

	data := NewMessage().SetData("is-healthy?")

	req := engine.NewRequest(healthTopic(engine.getIdent()))
	resp, err := req.Execute(data)
	if err != nil {
		t.Error(err)
	}

	msg := resp.Next()
	x := []string{}
	msg.GetData(&x)

	if len(x) > 0 {
		out_chan <- "healthy"
	} else {
		out_chan <- "false"
	}

	select {
	case result := <-out_chan:
		if result != "healthy" {
			t.Error("Message should be healthy. Found", result)
		}
	case <-time.After(time.Second * 5):
		t.Error("Message should be", PingResponse, "timed out instead")
	}
}

func TestReceiveOnWildcard(t *testing.T) {
	topic := fmt.Sprintf("%v*", PingTopic)
	out_chan := make(chan string, 1)

	//Subscribe to the wildcard topic.
	sub, _ := engine.Slot(
		topic,
		func(_ *Request) {
			out_chan <- PingResponse
		},
		nil,
	)

	//Publish a message to random topic
	engine.Signal("pingworld", NewMessage().SetData("ping?"))

	// Select Case, once and that should work.
	select {
	case <-out_chan:
		// True Case.
	case <-time.After(time.Second * 2):
		t.Error("Message should be received, timed out instead")
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

	data := NewMessage().SetData("ping?")

	req := engine.NewRequest(PingTopic)
	resp, err := req.Execute(data)
	if err != nil {
		t.Error(err)
	}

	msg := resp.Next()
	var x string
	msg.GetData(&x)
	out_chan <- x

	select {
	case result := <-out_chan:
		if result != PingResponse {
			t.Error("Message should be", PingResponse, "Found", result)
		}
	case <-time.After(time.Second * 5):
		t.Error("Message should be", PingResponse, "timed out instead")
	}
}

func TestPublisherTimeout(t *testing.T) {
	out_chan := make(chan string, 1)

	sleepFor := 5

	data := NewMessage().SetData(sleepFor)
	opts := NewRequestOpts().SetTimeout(2)

	req := engine.NewRequestWithOpts(SleepTopic, opts)
	resp, err := req.Execute(data)
	if err != nil {
		t.Error(err)
	}

	msg := resp.Next()
	var x string
	msg.GetData(&x)
	out_chan <- x

	select {
	case result := <-out_chan:
		if result != "Execution timed out" {
			t.Error("Message should be 'Execution timed out' Found", result)
		}
	case <-time.After(time.Second * time.Duration(sleepFor)):
		t.Error("Message should be", "Execution timed out", "timed out instead")
	}
}

func TestSansListenerSlot(t *testing.T) {
	_, err := engine.Signal("humpty-dumpty", nil)
	if err != nil {
		t.Error("Slots do not need recivers")
	}
}

func TestSansListenerRequest(t *testing.T) {
	req := engine.NewRequest("humpty-dumpty")
	_, err := req.Execute(nil)
	if err == nil || !strings.Contains(err.Error(), "listeners") {
		t.Error(err.Error())
	}
}

func TestSubscriberTimeout(t *testing.T) {
	out_chan := make(chan string, 1)
	topic := "sleep_delayed"

	sub, err := engine.ReplyTo(
		topic,
		func(req *Request, resp *Message) {
			time.Sleep(time.Second * 4)
			resp.SetData(PingResponse)
		},
		NewHandlerOpts().SetTimeout(2),
	)

	if err != nil {
		t.Error("Error Subscribing", PingTopic, err.Error())
		return
	}

	data := NewMessage().SetData("send")
	req := engine.NewRequest(topic)
	resp, err := req.Execute(data)
	if err != nil {
		t.Error(err)
	}

	msg := resp.Next()
	var x string
	msg.GetData(&x)
	out_chan <- x

	select {
	case result := <-out_chan:
		if result != "Execution timed out" {
			t.Error("Message should be Execution timed out. Found", result)
		}
	case <-time.After(time.Second * 5):
		t.Error("Message should be", PingResponse, "timed out instead")
	}

	engine.UnsubscribeReply(topic, sub)
}

func TestHandlerException(t *testing.T) {
	out_chan := make(chan int, 1)
	topic := randSeq(10)

	sub, err := engine.ReplyTo(topic, func(req *Request, resp *Message) {
		// Just to induce errors, access a null pointers's method.
		var x *HandlerOpts
		log.Println(x.GetGroup())
	}, nil)

	if err != nil {
		t.Error("Error Subscribing", PingTopic, err.Error())
		return
	}

	data := NewMessage().SetData("send")
	req := engine.NewRequest(topic)
	resp, err := req.Execute(data)
	if err != nil {
		t.Error(err)
	}

	msg := resp.Next()
	out_chan <- msg.GetCode()

	select {
	case result := <-out_chan:
		if result != 500 {
			t.Error("Message should raise exception")
		}
	case <-time.After(time.Second * 5):
		t.Error("Message should have raised Exception, timed out instead")
	}

	engine.UnsubscribeReply(topic, sub)
}

func TestSansListener(t *testing.T) {
	data := NewMessage().SetData("ping?")
	if _, err := engine.Signal("ping-sans-listener", data); err != nil {
		t.Error(err)
	}
}

func TestConfirmSansListener(t *testing.T) {
	data := NewMessage().SetData("ping?")
	req := engine.NewRequest("ping-confirm-sans-listener")

	if _, err := req.Execute(data); err != nil {
		if !strings.Contains(err.Error(), "active listeners") {
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

var initializers []func(*Gilmour)

func attachSetup(f func(*Gilmour)) {
	initializers = append(initializers, f)
}

func TestMain(m *testing.M) {
	ui.SetLevel(ui.Levels.Message)

	engine = Get(redis)

	for _, f := range initializers {
		f(engine)
	}

	engine.Start()

	status := m.Run()

	waitBeforeExiting(2) //Wait 5 seconds before exiting.
	engine.Stop()
	waitBeforeExiting(2) //Wait 2 more seconds before exiting.

	os.Exit(status)
}
