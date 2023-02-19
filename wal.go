package walx2

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/tidwall/wal"
)

type Log struct {
	index *atomic.Uint64
	lock  sync.Locker
	subId int
	subs  map[int]*Reader
	log   *wal.Log
}

func Open(dir string) (*Log, error) {
	log, err := wal.Open(dir, &wal.Options{
		SegmentCacheSize: 4,
		SegmentSize:      1 * 1024 * 1024 * 1024 * 1024,
		NoSync:           true,
	})
	if err != nil {
		return nil, fmt.Errorf("wal open: %w", err)
	}

	index, err := log.LastIndex()
	if err != nil {
		return nil, fmt.Errorf("wal get last index: %w", err)
	}
	atomicIndex := &atomic.Uint64{}
	atomicIndex.Store(index)

	return &Log{
		index: atomicIndex,
		lock:  &sync.Mutex{},
		log:   log,
		subs:  make(map[int]*Reader),
	}, nil
}

func (l *Log) Write(data []byte, nextIndex func(index uint64)) (uint64, error) {
	l.lock.Lock()
	defer l.lock.Unlock()

	next := l.index.Load() + 1
	nextIndex(next)
	err := l.log.Write(next, data)
	if err != nil {
		return 0, fmt.Errorf("wal write: %w", err)
	}

	l.index.Add(1)

	for _, c := range l.subs {
		c.notify()
	}

	return next, nil
}

func (l *Log) WriteEntry(entry Entry) error {
	l.lock.Lock()
	defer l.lock.Unlock()

	err := l.log.Write(entry.Index, entry.Data)
	if err != nil {
		return fmt.Errorf("wal write: %w", err)
	}

	l.index.Store(entry.Index)

	for _, c := range l.subs {
		c.notify()
	}

	return nil
}

func (l *Log) OpenReader(lastIndex uint64) *Reader {
	l.lock.Lock()
	defer l.lock.Unlock()

	l.subId++

	subId := l.subId
	close := func() {
		l.lock.Lock()
		defer l.lock.Unlock()

		delete(l.subs, subId)
	}
	reader := NewReader(close, lastIndex+1, l.log)
	l.subs[subId] = reader

	return reader
}

func (l *Log) LastIndex() uint64 {
	return l.index.Load()
}

func (l *Log) Close() error {
	err := l.log.Close()
	if err != nil {
		return fmt.Errorf("wal close: %w", err)
	}

	for _, c := range l.subs {
		c.Close()
	}

	return nil
}
