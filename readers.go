package walx

import (
	"sync"
	"sync/atomic"

	"github.com/txix-open/wal"
)

type Readers struct {
	readLock  sync.Locker
	writeLock sync.Locker

	readers map[int32]Reader
	subId   *atomic.Int32
}

func NewReaders() *Readers {
	lock := &sync.RWMutex{}
	return &Readers{
		readLock:  lock.RLocker(),
		writeLock: lock,
		readers:   map[int32]Reader{},
		subId:     &atomic.Int32{},
	}
}

func (r *Readers) Open(lastIndex uint64, inMem bool, log *wal.Log) Reader {
	subId := r.subId.Add(1)
	unsub := func() {
		r.writeLock.Lock()
		delete(r.readers, subId)
		r.writeLock.Unlock()
	}
	newReader := NewReaderV2
	if inMem {
		newReader = NewInMemReader
	}
	reader := newReader(unsub, lastIndex+1, log)

	r.writeLock.Lock()
	r.readers[subId] = reader
	r.writeLock.Unlock()

	return reader
}

func (r *Readers) Notify() {
	r.readLock.Lock()
	for _, reader := range r.readers {
		reader.notify()
	}
	r.readLock.Unlock()
}

func (r *Readers) Close() {
	r.writeLock.Lock()
	for _, reader := range r.readers {
		reader.close()
	}
	r.readers = map[int32]Reader{}
	r.writeLock.Unlock()
}
