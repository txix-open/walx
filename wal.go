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
	index        *atomic.Uint64
	log          *wal.Log
	readers      *Readers
	batch        *wal.Batch
	writtenBytes int
	options      *options
	lock         sync.Locker
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
		index:        atomicIndex,
		log:          log,
		readers:      NewReaders(),
		batch:        &wal.Batch{},
		writtenBytes: 0,
		options:      options,
		lock:         &sync.Mutex{},
	}, nil
}

func (l *Log) Write(data []byte, nextIndex func(index uint64)) (uint64, error) {
	var hookData HookData
	l.lock.Lock()
	defer func() {
		l.lock.Unlock()

		l.options.hook(hookData)
		l.readers.Notify()
	}()

	next := l.index.Load() + 1
	nextIndex(next)

	startWrite := time.Now()
	err := l.log.Write(next, data)
	if err != nil {
		return 0, fmt.Errorf("wal write: %w", err)
	}
	writeTime := time.Since(startWrite)

	hookData, err = l.postWrite(len(data), writeTime, next)
	if err != nil {
		return 0, err
	}

	return next, nil
}

func (l *Log) WriteEntries(entries Entries) error {
	if len(entries) == 0 {
		return nil
	}

	var hookData HookData
	l.lock.Lock()
	defer func() {
		l.batch.Clear()
		l.lock.Unlock()

		l.options.hook(hookData)
		l.readers.Notify()
	}()

	lastIndex := entries.LastIndex()

	bytesWritten := 0
	for _, entry := range entries {
		l.batch.Write(entry.Index, entry.Data)
		bytesWritten += len(entry.Data)
	}

	startWrite := time.Now()
	err := l.log.WriteBatch(l.batch)
	if err != nil {
		return fmt.Errorf("write batch: %w", err)
	}
	writeTime := time.Since(startWrite)

	hookData, err = l.postWrite(bytesWritten, writeTime, lastIndex)
	if err != nil {
		return err
	}

	return nil
}
func (l *Log) postWrite(bytesWritten int, writeTime time.Duration, index uint64) (HookData, error) {
	startFsync := time.Now()
	var fsyncTime time.Duration
	fsyncCalled, err := l.trySync(bytesWritten)
	if err != nil {
		return HookData{}, err
	}

	l.index.Store(index)

	if fsyncCalled {
		fsyncTime = time.Since(startFsync)
	}

	return HookData{
		BytesWritten: bytesWritten,
		WriteTime:    writeTime,
		FSyncCalled:  fsyncCalled,
		FSyncTime:    fsyncTime,
	}, nil
}

func (l *Log) OpenReader(lastIndex uint64) Reader {
	return l.openReader(lastIndex, false)
}

func (l *Log) OpenInMemReader(lastIndex uint64) Reader {
	return l.openReader(lastIndex, true)
}

func (l *Log) openReader(lastIndex uint64, inMem bool) Reader {
	return l.readers.Open(lastIndex, inMem, l.log)
}

func (l *Log) FirstIndex() (uint64, error) {
	ret, err := l.log.FirstIndex()
	if err != nil {
		return 0, fmt.Errorf("wal first index: %w", err)
	}
	return ret, nil
}

func (l *Log) LastIndex() uint64 {
	return l.index.Load()
}

func (l *Log) Close() error {
	l.lock.Lock()
	defer l.lock.Unlock()

	err := l.log.Close()
	if err != nil {
		return fmt.Errorf("wal close: %w", err)
	}

	l.readers.Close()

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

func (l *Log) trySync(bytesWritten int) (bool, error) {
	l.writtenBytes += bytesWritten
	if l.writtenBytes < l.options.fsyncThreshold {
		return false, nil
	}

	err := l.log.Sync()
	if err != nil {
		return false, fmt.Errorf("wal sync: %w", err)
	}

	l.writtenBytes = 0

	return true, nil
}
