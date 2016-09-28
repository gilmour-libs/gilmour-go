package gilmour

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"gopkg.in/gilmour-libs/gilmour-e-go.v5/proto"
	"gopkg.in/gilmour-libs/gilmour-e-go.v5/ui"
)

func responseTopic(sender string) string {
	return fmt.Sprintf("gilmour.response.%s", sender)
}

//Get a working Gilmour Engine powered by the backend provided.
//Currently, only Redis is supported as a backend.
func Get(backend proto.Backend) *Gilmour {
	x := Gilmour{}
	x.subscriber = newSubscriptionManager()
	x.addBackend(backend)
	return &x
}

type Gilmour struct {
	enableHealthCheck bool
	identMutex        sync.RWMutex
	subscriberMutex   sync.RWMutex
	backend           proto.Backend
	ident             string
	errorPolicy       string
	subscriber        subscriber
}

// Start the Gilmour engine. Creates a bi-directional channel; sent to both
// backend and startReciver
func (g *Gilmour) Start() {
	sink := make(chan *proto.Packet)
	g.backend.Start(sink)
	go g.startReciver(sink)
}

//Exit routine. UnSubscribes Slots, removes registered health ident and
//triggers backend Stop.
func (g *Gilmour) Stop() {
	defer g.backend.Stop()

	for topic, handlers := range g.getAllSubscribers() {
		for _, h := range handlers {
			g.UnsubscribeSlot(topic, h)
			g.UnsubscribeReply(topic, h)
		}
	}

	g.unregisterIdent()
}

//Keep listening to messages on sink, spinning a new goroutine for every
//message recieved.
func (g *Gilmour) startReciver(sink <-chan *proto.Packet) {
	for {
		go g.processMessage(<-sink)
	}
}

/*
Parse a gilmour *Message and for subscribers of this topic do the following:
	* If subscriber is one shot, unsubscribe the subscriber to prevent subscribers from re-execution.
	* If subscriber belongs to a group, try acquiring a lock via backend to ensure group exclusivity.
	* If all conditions suffice spin up a new goroutine for each subscription.
*/
func (g *Gilmour) processMessage(msg *proto.Packet) {
	subs, ok := g.getSubscribers(msg.GetPattern())
	if !ok || len(subs) == 0 {
		ui.Warn("*Message cannot be processed. No subs found for key %v", msg.GetPattern())
		return
	}

	m, err := parseMessage(msg.GetData())
	if err != nil {
		ui.Alert(err.Error())
		return
	}

	for _, s := range subs {

		opts := s.GetOpts()
		if opts != nil && opts.isOneShot() {
			ui.Message("Unsubscribing one shot response topic %v", msg.GetTopic())
			go g.UnsubscribeReply(msg.GetPattern(), s)
		}

		if opts.GetGroup() != "" && opts.shouldSendResponse() {
			if !g.backend.AcquireGroupLock(opts.GetGroup(), m.GetSender()) {
				ui.Warn(
					"Unable to acquire Lock. Topic %v Group %v Sender %v",
					msg.GetTopic(), opts.GetGroup(), m.GetSender(),
				)
				continue
			}
		}

		go g.handleRequest(s, msg.GetTopic(), m)
	}
}

func (g *Gilmour) handleRequest(s *Subscription, topic string, m *Message) {
	senderId := m.GetSender()

	req := &Request{topic, m}
	res := NewMessage()
	res.setSender(responseTopic(senderId))

	done := make(chan bool, 1)

	//Executing Request
	go func(done chan<- bool) {

		// Schedule a function to recover in case handler runs into an error.
		// Read more: https://gist.github.com/meson10/d56eface6f87c664d07d

		defer func() {
			err := recover()
			if err == nil {
				return
			}

			const size = 4096
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)]
			buffer := string(buf)
			res.SetData(buffer).SetCode(500)

			done <- true

		}()

		s.GetHandler()(req, res)
		done <- true
	}(done)

	// Start a timeout handler, which writes on the Done channel, ahead of the
	// handler. This might result in a RACE condition, as there is no way to
	// kill a goroutine, since they are not preemptive.

	timeout := s.GetOpts().GetTimeout()
	time.AfterFunc(time.Duration(timeout)*time.Second, func() {
		done <- false
	})

	status := <-done

	if s.GetOpts().shouldSendResponse() {
		if status == false {
			g.sendTimeout(senderId, res.GetSender())
		} else {
			if res.GetCode() == 0 {
				res.SetCode(200)
			}

			if err := g.publish(res.GetSender(), res); err != nil {
				ui.Alert(err.Error())
			}
		}

	} else if status == false {
		// Inform the error catcher, If there is no handler for this Request
		// but the request had failed. This is automatically handled in case
		// of a response being written via Publisher.
		request := string(req.bytes())
		errMsg := proto.MakeError(499, topic, request, "", req.Sender(), "")
		g.reportError(errMsg)
	}
}

func (g *Gilmour) timeoutMessage(senderId string) *Message {
	msg := NewMessage()
	msg.setSender(senderId).SetCode(499).SetData("Execution timed out")
	return msg
}

func (g *Gilmour) sendTimeout(senderId, channel string) {
	msg := g.timeoutMessage(senderId)
	if err := g.publish(channel, msg); err != nil {
		ui.Alert(err.Error())
	}
}

func (g *Gilmour) addBackend(backend proto.Backend) {
	g.backend = backend
}

func (g *Gilmour) getIdent() string {
	g.identMutex.Lock()
	defer g.identMutex.Unlock()

	if g.ident == "" {
		g.ident = proto.Ident()
	}

	return g.ident
}

func (g *Gilmour) registerIdent() {
	ident := g.getIdent()
	err := g.backend.RegisterIdent(ident)
	if err != nil {
		panic(err)
	}
}

func (g *Gilmour) unregisterIdent() {
	if !g.IsHealthCheckEnabled() {
		return
	}

	ident := g.getIdent()
	err := g.backend.UnregisterIdent(ident)
	if err != nil {
		panic(err)
	}
}

func (g *Gilmour) IsHealthCheckEnabled() bool {
	return g.enableHealthCheck
}

func (g *Gilmour) SetHealthCheckEnabled() *Gilmour {
	g.enableHealthCheck = true
	subscribeHealth(g)
	g.registerIdent()
	return g
}

func (g *Gilmour) getAllSubscribers() map[string][]*Subscription {
	return g.subscriber.getAll()
}

func (g *Gilmour) getSubscribers(topic string) ([]*Subscription, bool) {
	return g.subscriber.get(topic)
}

func (g *Gilmour) removeSubscribers(topic string) {
	g.subscriber.deleteAll(topic)
}

func (g *Gilmour) removeSubscriber(topic string, s *Subscription) {
	g.subscriber.delete(topic, s)
}

func (g *Gilmour) addSubscriber(t string, h handler, o *HandlerOpts) *Subscription {
	return g.subscriber.add(t, h, o)
}

func (g *Gilmour) isExclusiveDuplicate(topic, group string) bool {
	subs, ok := g.getSubscribers(topic)
	if ok {
		for _, s := range subs {
			if s.GetOpts().GetGroup() == group {
				return true
			}
		}
	}

	return false
}

//Underlying subscribe method.
func (g *Gilmour) subscribe(topic string, h handler, opts *HandlerOpts) (*Subscription, error) {
	group := opts.GetGroup()

	if group != "" && g.isExclusiveDuplicate(topic, group) {
		return nil, errors.New(fmt.Sprintf("Duplicate reply handler for %v:%v", topic, group))
	}

	if err := g.backend.Subscribe(topic, opts.GetGroup()); err != nil {
		return nil, err
	} else {
		return g.addSubscriber(topic, h, opts), nil
	}
}

//Underlying unsubscribe.
func (g *Gilmour) unsubscribe(topic string, s *Subscription) {
	g.removeSubscriber(topic, s)

	if _, ok := g.getSubscribers(topic); !ok {
		err := g.backend.Unsubscribe(topic)
		if err != nil {
			panic(err)
		}
	}
}

func (g *Gilmour) SupportedErrorPolicies() []string {
	return g.backend.SupportedErrorPolicies()
}

func (g *Gilmour) SetErrorPolicy(policy string) {
	err := g.backend.SetErrorPolicy(policy)
	if err != nil {
		panic(err)
	}
}

//Error policy for this Gilmour engine.
func (g *Gilmour) GetErrorPolicy() string {
	return g.backend.GetErrorPolicy()
}

func (g *Gilmour) reportError(e *proto.GilmourError) {
	ui.Warn(
		"Reporting Error. Code %v Sender %v Topic %v",
		e.GetCode(), e.GetSender(), e.GetTopic(),
	)

	err := g.backend.ReportError(g.GetErrorPolicy(), e)
	if err != nil {
		panic(err)
	}
}

func (g *Gilmour) requestDestination(topic string) string {
	if strings.HasPrefix(topic, "gilmour.") {
		return topic
	} else {
		return fmt.Sprintf("gilmour.request.%v", topic)
	}
}

func (g *Gilmour) slotDestination(topic string) string {
	if strings.HasPrefix(topic, "gilmour.") {
		return topic
	} else {
		return fmt.Sprintf("gilmour.slot.%v", topic)
	}
}

// Reply part of Request-Reply design pattern.
func (g *Gilmour) ReplyTo(topic string, h RequestHandler, opts *HandlerOpts) (*Subscription, error) {
	if strings.Contains(topic, "*") {
		return nil, errors.New("ReplyTo cannot have wildcard topics")
	}

	if opts == nil {
		opts = &HandlerOpts{}
	}

	if opts.GetGroup() == "" {
		opts.SetGroup("_default")
	}

	handler := func(req *Request, resp *Message) {
		h(req, resp)
	}

	return g.subscribe(g.requestDestination(topic), handler, opts)
}

//Unsubscribe Previously registered Reply to.
func (g *Gilmour) UnsubscribeReply(topic string, s *Subscription) {
	g.unsubscribe(g.requestDestination(topic), s)
}

// Request part of Request-Reply design pattern.
func (g *Gilmour) request(topic string, msg *Message, opts *RequestOpts) (*Response, error) {
	if has, err := g.backend.HasActiveSubscribers(g.requestDestination(topic)); err != nil {
		return nil, err
	} else if !has {
		return nil, errors.New("No active listeners for: " + topic)
	}

	if msg == nil {
		msg = NewMessage()
	}

	sender := proto.SenderId()
	msg.setSender(sender)
	respChannel := responseTopic(sender)

	if opts == nil {
		opts = NewRequestOpts()
	}

	//Wait for a responseHandler
	f := make(chan *Message, 1)

	rOpts := NewHandlerOpts().setOneShot().sendResponse(false)
	g.ReplyTo(respChannel, func(req *Request, _ *Message) {
		f <- req.gData
	}, rOpts)

	// Send Timeout message to channel, Need to send timeout over the wire,
	// to ensure gilmour cleans up the oneShot response handlers as well.
	timeout := opts.GetTimeout()
	if timeout > 0 {
		time.AfterFunc(time.Duration(timeout)*time.Second, func() {
			g.sendTimeout(sender, respChannel)
		})
	}

	g.publish(g.requestDestination(topic), msg)

	response := newResponse(1)
	response.write(<-f)
	return response, nil
}

//Slot counterpart of Signal Slot architecture.
func (g *Gilmour) Slot(topic string, h SlotHandler, opts *HandlerOpts) (*Subscription, error) {
	if opts == nil {
		opts = &HandlerOpts{}
	}

	opts.setSlot()
	handler := func(req *Request, _ *Message) {
		h(req)
	}

	return g.subscribe(g.slotDestination(topic), handler, opts)
}

func (g *Gilmour) UnsubscribeSlot(topic string, s *Subscription) {
	g.unsubscribe(g.slotDestination(topic), s)
}

// Signal counterpart of Signl-Slot design pattern.
func (g *Gilmour) Signal(topic string, msg *Message) (sender string, err error) {
	if msg == nil {
		msg = NewMessage()
	}

	sender = proto.SenderId()
	msg.setSender(sender)
	return sender, g.publish(g.slotDestination(topic), msg)
}

// Internal method to publish a message.
func (g *Gilmour) publish(topic string, msg *Message) error {
	if msg.GetCode() == 0 {
		msg.SetCode(200)
	}

	if msg.GetCode() >= 300 {
		go func() {
			request, err := msg.Marshal()
			if err != nil {
				request = []byte{}
			}

			g.reportError(
				proto.MakeError(
					msg.GetCode(), topic, string(request), "", msg.GetSender(), "",
				),
			)

		}()
	}

	sent, err := g.backend.Publish(topic, msg)
	if err != nil {
		return err
	} else if !sent {
		return errors.New(fmt.Sprintf("Mesage for %v was not received by anyone.", topic))
	} else {
		return err
	}
}
