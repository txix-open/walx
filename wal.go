package walx

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/txix-open/wal"
)

var (
	ErrClosed = wal.ErrClosed
)

type Log struct {
	index          *atomic.Uint64
	lock           sync.Locker
	subId          *atomic.Int32
	subs           map[int32]*Reader
	log            *wal.Log
	hook           Hook
	fsyncThreshold int
	writtenBytes   int
}

func Open(dir string, opts ...Option) (*Log, error) {
	options := newOptions()
	for _, opt := range opts {
		opt(options)
	}

	log, err := wal.Open(dir, &wal.Options{
		SegmentCacheSize: options.segmentCacheSize,
		SegmentSize:      options.segmentSize,
		NoSync:           true,
		NoCopy:           true,
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
		index:          atomicIndex,
		lock:           &sync.Mutex{},
		log:            log,
		subId:          &atomic.Int32{},
		subs:           map[int32]*Reader{},
		fsyncThreshold: options.fsyncThreshold,
		hook:           options.hook,
	}, nil
}

func (l *Log) Write(data []byte, nextIndex func(index uint64)) (uint64, error) {
	l.lock.Lock()
	defer l.lock.Unlock()

	next := l.index.Load() + 1
	nextIndex(next)
	startWrite := time.Now()
	err := l.log.Write(next, data)
	if err != nil {
		return 0, fmt.Errorf("wal write: %w", err)
	}
	writeTime := time.Since(startWrite)

	startFsync := time.Now()
	var fsyncTime time.Duration
	fsyncCalled, err := l.trySync(data)
	if err != nil {
		return 0, err
	}
	if fsyncCalled {
		fsyncTime = time.Since(startFsync)
	}

	l.hook(HookData{
		Index:       next,
		Data:        data,
		WriteTime:   writeTime,
		FSyncCalled: fsyncCalled,
		FSyncTime:   fsyncTime,
	})

	l.index.Add(1)

	for _, reader := range l.subs {
		reader.notify()
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

	_, err = l.trySync(entry.Data)
	if err != nil {
		return err
	}

	l.index.Store(entry.Index)

	for _, reader := range l.subs {
		reader.notify()
	}

	return nil
}

func (l *Log) OpenReader(lastIndex uint64) *Reader {
	l.lock.Lock()
	defer l.lock.Unlock()

	subId := l.subId.Add(1)
	unsub := func() {
		l.lock.Lock()
		defer l.lock.Unlock()

		delete(l.subs, subId)
	}
	reader := NewReader(unsub, lastIndex+1, l.log)
	l.subs[subId] = reader

	return reader
}

func (l *Log) LastIndex() uint64 {
	return l.index.Load()
}

func (l *Log) IsInMemory(index uint64) bool {
	return l.log.IsInMemory(index)
}

func (l *Log) Close() error {
	l.lock.Lock()
	defer l.lock.Unlock()

	err := l.log.Close()
	if err != nil {
		return fmt.Errorf("wal close: %w", err)
	}

	for _, reader := range l.subs {
		reader.close()
	}
	l.subs = map[int32]*Reader{}

	return nil
}

func (l *Log) TruncateFront(newFirstIndex uint64) error {
	l.lock.Lock()
	defer l.lock.Unlock()

	err := l.log.TruncateFront(newFirstIndex)
	if err != nil {
		return fmt.Errorf("wal front truncate: %w", err)
	}
	index, err := l.log.LastIndex()
	if err != nil {
		return fmt.Errorf("wal get last index: %w", err)
	}
	l.index.Store(index)
	return nil
}

func (l *Log) trySync(data []byte) (bool, error) {
	l.writtenBytes += len(data)
	if l.writtenBytes < l.fsyncThreshold {
		return false, nil
	}

	err := l.log.Sync()
	if err != nil {
		return false, fmt.Errorf("wal sync: %w", err)
	}

	l.writtenBytes = 0

	return true, nil
}
