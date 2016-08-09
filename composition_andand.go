package gilmour

//New AndAnd composition.
func (g *Gilmour) NewAndAnd(cmds ...Executable) *AndAndComposition {
	c := new(AndAndComposition)
	c.setEngine(g)
	c.add(cmds...)
	return c
}

type AndAndComposition struct {
	composition
}

func (c *AndAndComposition) Execute(m *Message) (resp *Response, err error) {
	do := func(do recfunc, m *Message) {
		cmd := c.lpop()

		resp, err = performJob(cmd, m, c.engine)

		// Keep going if nothing has failed so far.
		if len(c.executables()) > 0 && err == nil && resp.Code() == 200 {
			do(do, m)
			return
		}
	}

	do(do, m)
	return
}
