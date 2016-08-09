package gilmour

type Request struct {
	topic string
	gData *Message
}

func (r *Request) Sender() string {
	return r.gData.GetSender()
}

func (r *Request) RawData() interface{} {
	return r.gData.rawData()
}

func (r *Request) Data(t interface{}) error {
	return r.gData.GetData(t)
}

func (r *Request) Topic() string {
	return r.topic
}

func (r *Request) Code() int {
	return r.gData.GetCode()
}

func (r *Request) bytes() []byte {
	byt, _ := r.gData.bytes()
	return byt
}

//New Request composition
func (g *Gilmour) NewRequest(topic string) *RequestComposer {
	rc := new(RequestComposer)
	rc.setEngine(g)
	rc.topic = topic
	rc.opts = nil
	return rc
}

func (g *Gilmour) NewRequestWithOpts(topic string, opts *RequestOpts) *RequestComposer {
	rc := new(RequestComposer)
	rc.setEngine(g)
	rc.topic = topic
	rc.opts = opts
	return rc
}

// Command represent the each command inside a Pipeline.
// Requires a topic to send the message to, and an optional transformer.
type RequestComposer struct {
	topic   string
	opts    *RequestOpts
	engine  *Gilmour
	message interface{}
}

//Set the Gilmour Engine required for execution
func (rc *RequestComposer) setEngine(g *Gilmour) {
	rc.engine = g
}

func (rc *RequestComposer) With(t interface{}) *RequestComposer {
	if rc.message != nil {
		panic("Cannot change the message after its been set")
	}

	rc.message = t
	return rc
}

func (rc *RequestComposer) Execute(m *Message) (resp *Response, err error) {
	if rc.message != nil {
		if err := compositionMerge(m.rawData(), &rc.message); err != nil {
			return nil, err
		}
	}

	err = try(rc.engine, func() (err error) {
		resp, err = rc.engine.request(rc.topic, m, rc.opts)
		return
	})

	return
}
