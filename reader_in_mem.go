package walx

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/txix-open/wal"
)

type ReadOnlyLog interface {
	Read(index uint64) (data []byte, err error)
}

type InMemReader struct {
	unsub    func()
	index    *atomic.Uint64
	log      ReadOnlyLog
	closed   *atomic.Bool
	readTime *time.Timer
	ch       chan struct{}
}

func NewInMemReader(unsub func(), index uint64, log *wal.Log) Reader {
	i := &atomic.Uint64{}
	i.Store(index)
	return &InMemReader{
		unsub:    unsub,
		index:    i,
		log:      log,
		closed:   &atomic.Bool{},
		readTime: time.NewTimer(waitEntryTimeout),
		ch:       make(chan struct{}),
	}
}

func (r *InMemReader) Read(ctx context.Context) (Entry, error) {
	return r.read(ctx, true)
}

func (r *InMemReader) ReadAtMost(ctx context.Context, limit int) (Entries, error) {
	result := make([]Entry, 0, limit/2)
	for len(result) < limit {
		shouldWait := len(result) == 0
		entry, err := r.read(ctx, shouldWait)
		if errors.Is(err, wal.ErrNotFound) {
			return result, nil
		}
		if err != nil {
			return nil, err
		}
		result = append(result, entry)
	}
	return result, nil
}

func (r *InMemReader) read(ctx context.Context, wait bool) (Entry, error) {
	for {
		if r.closed.Load() {
			return Entry{}, ErrClosed
		}

		index := r.index.Load()
		data, err := r.log.Read(index)
		if errors.Is(err, wal.ErrNotFound) && wait {
			r.readTime.Reset(waitEntryTimeout)
			select {
			case <-r.ch:
			case <-r.readTime.C:
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

func (r *InMemReader) Close() {
	if r.closed.Load() {
		return
	}

	r.unsub()
	r.close()
}

func (r *InMemReader) close() {
	r.closed.Store(true)
	close(r.ch)
	r.readTime.Stop()
}

func (r *InMemReader) LastIndex() uint64 {
	return r.index.Load() - 1
}

func (r *InMemReader) notify() {
	select {
	case r.ch <- signal:
	default:
	}
}
