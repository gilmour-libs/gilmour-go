package gilmour

func (g *Gilmour) NewOrOr(cmds ...Executable) *OrOrComposition {
	c := new(OrOrComposition)
	c.setEngine(g)
	c.add(cmds...)
	return c
}

type OrOrComposition struct {
	composition
}

func (c *OrOrComposition) Execute(m *Message) (resp *Response, err error) {
	do := func(do recfunc, m *Message) {
		cmd := c.lpop()

		resp, err = performJob(cmd, m, c.engine)

		// Keep going, If the Pipeline has failed so far and there are still
		// some executables left.
		if len(c.executables()) > 0 && (resp.Code() > 200 || err != nil) {
			do(do, m)
			return
		}
	}

	do(do, m)
	return
}
