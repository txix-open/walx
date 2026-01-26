package state

type result struct {
	response any
	err      error
}

type future struct {
	event any
	ch    chan result
}

func newFuture(event any) *future {
	return &future{
		event: event,
		ch:    make(chan result, 1),
	}
}

func (r *future) complete(response any, err error) {
	r.ch <- result{response: response, err: err}
}

func (r *future) wait() (any, error) {
	result := <-r.ch
	return result.response, result.err
}
