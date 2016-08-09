package gilmour

import "sync"

/*
Microservices increase the granularity of our services oriented architectures.
Borrowing from unix philosohy, they should do one thing and do it well.
However, for this to be really useful, there should be a facility such as is
provided by unix shells. Unix shells allow the composition of small commands
using the following methods

	- Composition: cmd1 | cmd2 | cmd2
	- AndAnd: cmd1 && cmd2 && cmd3
	- Batch: cmd1; cmd2; cmd3 > out or (cmd1; cmd2; cmd3) > out
	- Parallel: This runs stages in parallel call the callback when all are done.
	The data passed to the callback is an array of :code hashes in no particular
	order. The code is the highest error code from the array.

Also, you should be able to use these methods of composition in any combination
and also in a nexted manner - (cmd1 | cmd2) && cmd3. Gilmour enables you to do
just that.
*/
type Composition interface {
	Execute(*Message) (*Response, error)
}

type Executable interface {
	Execute(*Message) (*Response, error)
}

func inflateResponse(r *Response) *Response {
	if r.Cap() <= 1 {
		return r
	}

	output := make([]interface{}, r.Cap())

	i := 0
	for x := r.Next(); x != nil; x = r.Next() {
		output[i] = x.rawData()
		i++
	}

	resp := newResponse(1)
	resp.write(NewMessage().SetData(output).SetCode(r.Code()))
	resp.code = r.Code()
	return resp
}

func performJob(cmd Executable, m *Message, g *Gilmour) (resp *Response, err error) {
	err = try(g, func() (err error) {
		resp, err = cmd.Execute(copyMessage(m))
		return
	})
	return
}

func copyMessage(m *Message) *Message {
	byts, err := m.Marshal()
	if err != nil {
		panic(err)
	}

	msg, err := parseMessage(byts)
	if err != nil {
		panic(err)
	}

	return msg
}

// composition refers to a group of commands and defines the manner in which
// they are to be executed. Common executables are:
// AndAnd, Batch, Pipe, Parallel. etc. Read documentation for more details.
type composition struct {
	sync.RWMutex
	engine       *Gilmour
	_executables []Executable
	output       []*Message
}

//Set the Gilmour Engine required for execution
func (c *composition) setEngine(g *Gilmour) {
	c.engine = g
}

//Get the output if they were previously recorded, or of a Parallel composition
func (c *composition) getOutput() []*Message {
	c.RLock()
	defer c.RUnlock()

	return c.output
}

func (c *composition) makeResponse() *Response {
	return newResponse(1)
}

//Get the list of executables. Has inbuilt locking.
func (c *composition) executables() []Executable {
	c.RLock()
	defer c.RUnlock()

	return c._executables
}

// Add a new composer to the List.
func (c *composition) add(cmds ...Executable) {
	c.Lock()
	defer c.Unlock()

	for _, cp := range cmds {
		c._executables = append(c._executables, cp)
	}
}

//Pop the first composer. Useful for Tail recursion.
func (c *composition) lpop() Executable {
	cmps := c.executables()

	c.Lock()
	defer c.Unlock()

	if len(cmps) == 0 {
		return nil
	}

	cmd := cmps[0]

	if len(cmps) > 1 {
		c._executables = cmps[1:]
	} else {
		c._executables = []Executable{}
	}

	return cmd
}

//Save the output message to array of outputs
func (c *composition) saveOutput(m *Message) {
	c.Lock()
	defer c.Unlock()

	c.output = append(c.output, m)
}

type recordableComposition struct {
	composition
	record bool
}

func (c *recordableComposition) isRecorded() bool {
	c.RLock()
	defer c.RUnlock()

	return c.record
}

//Should the output be recorded? Mostly used in AndAnd and Batch.
func (c *recordableComposition) RecordOutput() {
	c.Lock()
	defer c.Unlock()

	c.record = true
}

func (c *recordableComposition) makeResponse() *Response {
	c.RLock()
	defer c.RUnlock()

	LEN := 1
	if c.record || len(c.executables()) == 0 {
		LEN = len(c.executables())
	}

	return newResponse(LEN)
}

func (c *recordableComposition) makeMessage(data interface{}) *Message {
	m := NewMessage()
	m.SetData(data)
	m.SetCode(200)
	return m
}

type recfunc func(recfunc, *Message)
