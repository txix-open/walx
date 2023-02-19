package walx2

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/tidwall/wal"
)

type ReadOnlyLog interface {
	Read(index uint64) (data []byte, err error)
}

type Reader struct {
	close  func()
	index  uint64
	log    ReadOnlyLog
	closed *atomic.Bool
	ch     chan struct{}
}

func NewReader(close func(), index uint64, log ReadOnlyLog) *Reader {
	return &Reader{
		close:  close,
		index:  index,
		log:    log,
		closed: &atomic.Bool{},
		ch:     make(chan struct{}),
	}
}

func (r *Reader) Read() (*Entry, error) {
	if r.closed.Load() {
		return nil, wal.ErrClosed
	}

	for {
		data, err := r.log.Read(r.index)
		if errors.Is(err, wal.ErrNotFound) {
			<-r.ch
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("wal read: %w", err)
		}
		r.index++
		return &Entry{Data: data, Index: r.index - 1}, nil
	}
}

func (r *Reader) Close() {
	if r.closed.Load() {
		return
	}

	r.close()
	close(r.ch)
	r.closed.Store(true)
}

func (r *Reader) notify() {
	select {
	case r.ch <- struct{}{}:
	default:
	}
}
