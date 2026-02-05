package walx

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/txix-open/wal"
)

type Entries []Entry

func (entries Entries) LastIndex() uint64 {
	if len(entries) == 0 {
		return 0
	}
	return entries[len(entries)-1].Index
}

type ReaderV2 struct {
	currentScanner *bufio.Scanner
	currentFile    *atomic.Pointer[os.File]
	isInMemory     bool

	closed   *atomic.Bool
	readTime *time.Timer
	ch       chan struct{}
	unsub    func()
	index    *atomic.Uint64
	log      *wal.Log
}

func NewReaderV2(unsub func(), index uint64, log *wal.Log) Reader {
	i := &atomic.Uint64{}
	i.Store(index)
	return &ReaderV2{
		unsub:          unsub,
		currentFile:    &atomic.Pointer[os.File]{},
		currentScanner: nil,
		index:          i,
		log:            log,
		closed:         &atomic.Bool{},
		readTime:       time.NewTimer(waitEntryTimeout),
		ch:             make(chan struct{}),
	}
}

func (r *ReaderV2) Read(ctx context.Context) (Entry, error) {
	return r.read(ctx, true)
}

func (r *ReaderV2) ReadAtMost(ctx context.Context, limit int) (Entries, error) {
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

func (r *ReaderV2) read(ctx context.Context, wait bool) (Entry, error) {
	if r.closed.Load() {
		return Entry{}, ErrClosed
	}

	index := r.index.Load()

	if r.isInMemory {
		for {
			if r.closed.Load() {
				return Entry{}, ErrClosed
			}

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

	if r.currentScanner != nil {
		ok := r.currentScanner.Scan()
		if ok {
			r.index.Add(1)
			return Entry{
				Data:  r.currentScanner.Bytes(),
				Index: index,
			}, nil
		}
		err := r.currentScanner.Err()
		if err != nil {
			return Entry{}, errors.WithMessage(err, "scanner error")
		}

		_ = r.currentFile.Load().Close()
		r.currentFile.Store(nil)
		r.currentScanner = nil
	}

	r.isInMemory = r.log.IsInMemory(index)

	if !r.isInMemory {
		ss := r.log.FindSegment(index)

		currentFile, err := os.Open(ss.Path())
		if err != nil {
			return Entry{}, errors.WithMessagef(err, "open segment file: %s", ss.Path())
		}
		r.currentFile.Store(currentFile)

		currentScanner := bufio.NewScanner(currentFile)
		const buffSize = 64 * 1024 * 1024
		currentScanner.Buffer(make([]byte, 0, buffSize), buffSize)
		currentScanner.Split(LengthPrefixedSplitFunc)
		r.currentScanner = currentScanner

		currentIndex := ss.FirstIndex()
		for currentIndex < index {
			_ = currentScanner.Scan()
			err = currentScanner.Err()
			if err != nil {
				return Entry{}, errors.WithMessage(err, "scanner error")
			}
		}
	}

	return r.read(ctx, wait)
}

func (r *ReaderV2) Close() {
	if r.closed.Load() {
		return
	}

	r.unsub()
	r.close()
}

func (r *ReaderV2) close() {
	r.closed.Store(true)
	close(r.ch)
	r.readTime.Stop()
	if r.currentFile.Load() != nil {
		_ = r.currentFile.Load().Close()
	}
}

func (r *ReaderV2) LastIndex() uint64 {
	return r.index.Load() - 1
}

func (r *ReaderV2) notify() {
	select {
	case r.ch <- signal:
	default:
	}
}

func LengthPrefixedSplitFunc(data []byte, atEOF bool) (advance int, token []byte, err error) {
	payloadLen, n := binary.Uvarint(data)
	if n == 0 {
		return 0, nil, nil
	}
	if n < 0 {
		return 0, nil, errors.New("invalid data size")
	}

	totalLen := n + int(payloadLen)

	if len(data) < totalLen {
		return 0, nil, nil
	}

	return totalLen, data[n:totalLen], nil
}
