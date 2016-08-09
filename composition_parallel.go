package gilmour

import (
	"log"
	"sync"
)

//New Parallel composition
func (g *Gilmour) NewParallel(cmds ...Executable) *ParallelComposition {
	c := new(ParallelComposition)
	c.setEngine(g)
	c.add(cmds...)
	c.RecordOutput()
	return c
}

type ParallelComposition struct {
	recordableComposition
}

type parallelfunc func(parallelfunc, *Message, *Response)

func (c *ParallelComposition) Execute(m *Message) (*Response, error) {
	resp := c.makeResponse()
	var wg sync.WaitGroup

	do := func(do parallelfunc, m *Message, f *Response) {
		wg.Add(1)
		cmd := c.lpop()

		go func(cmd Executable, w *sync.WaitGroup) {
			defer w.Done()

			response, err := performJob(cmd, m, c.engine)

			if err != nil {
				log.Println(err)
			}

			response = inflateResponse(response)
			f.write(response.Next())
		}(cmd, &wg)

		if len(c.executables()) != 0 {
			do(do, m, f)
		}
	}

	do(do, m, resp)
	wg.Wait()
	return resp, nil
}
