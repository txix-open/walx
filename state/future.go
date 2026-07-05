package state

import "sync"

type result struct {
	response any
	err      error
}

type future struct {
	event any
	ch    chan result
}

func (r *future) complete(response any, err error) {
	r.ch <- result{response: response, err: err}
}

func (r *future) wait() (any, error) {
	res := <-r.ch
	return res.response, res.err
}

var futurePool = sync.Pool{
	New: func() any { return &future{ch: make(chan result, 1)} },
}

func acquireFuture(event any) *future {
	f := futurePool.Get().(*future)
	f.event = event
	return f
}

func releaseFuture(f *future) {
	f.event = nil
	futurePool.Put(f)
}
