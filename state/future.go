package state

type result struct {
	response any
	err      error
}

type future struct {
	ch chan result
}

func newFuture() *future {
	return &future{
		ch: make(chan result, 1),
	}
}

func (r *future) complete(response any, err error) {
	if err != nil {
		r.ch <- result{err: err}
		return
	}
	r.ch <- result{response: response}
}

func (r *future) wait() (any, error) {
	result := <-r.ch
	return result.response, result.err
}
