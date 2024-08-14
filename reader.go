package walx

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/txix-open/wal"
)

const (
	waitEntryTimeout = 500 * time.Millisecond
)

type ReadOnlyLog interface {
	Read(index uint64) (data []byte, err error)
}

type Reader struct {
	unsub  func()
	index  *atomic.Uint64
	log    ReadOnlyLog
	closed *atomic.Bool
	ch     chan struct{}
}

func NewReader(unsub func(), index uint64, log ReadOnlyLog) *Reader {
	i := &atomic.Uint64{}
	i.Store(index)
	return &Reader{
		unsub:  unsub,
		index:  i,
		log:    log,
		closed: &atomic.Bool{},
		ch:     make(chan struct{}),
	}
}

func (r *Reader) Read(ctx context.Context) (Entry, error) {
	for {
		if r.closed.Load() {
			return Entry{}, ErrClosed
		}

		index := r.index.Load()
		data, err := r.log.Read(index)
		if errors.Is(err, wal.ErrNotFound) {
			select {
			case <-r.ch:
			case <-time.After(waitEntryTimeout):
			case <-ctx.Done():
				return Entry{}, ctx.Err()
			}
			continue
		}
		if err != nil {
			return Entry{}, fmt.Errorf("wal read: %w", err)
		}
		r.index.Add(1)
		return Entry{
			Data:  data,
			Index: index,
		}, nil
	}
}

func (r *Reader) Close() {
	if r.closed.Load() {
		return
	}

	r.unsub()
	r.close()
}

func (r *Reader) close() {
	r.closed.Store(true)
	close(r.ch)
}

func (r *Reader) LastIndex() uint64 {
	return r.index.Load() - 1
}

func (r *Reader) notify() {
	select {
	case r.ch <- struct{}{}:
	default:
	}
}
