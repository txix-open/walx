package walx

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
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
	currentReader *PrefixSizeDataReader
	currentFile   *atomic.Pointer[os.File]
	isInMemory    bool

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
		unsub:         unsub,
		currentFile:   &atomic.Pointer[os.File]{},
		currentReader: nil,
		index:         i,
		log:           log,
		closed:        &atomic.Bool{},
		readTime:      time.NewTimer(waitEntryTimeout),
		ch:            make(chan struct{}),
	}
}

func (r *ReaderV2) Read(ctx context.Context) (Entry, error) {
	return r.read(ctx, true)
}

func (r *ReaderV2) ReadAtMost(ctx context.Context, limit int) (Entries, error) {
	return readAtMost(ctx, r, limit)
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

	if r.currentReader != nil {
		data, err := r.currentReader.ReadNext(false)
		if errors.Is(err, io.EOF) {
			_ = r.currentFile.Load().Close()
			r.currentFile.Store(nil)
			r.currentReader = nil
			return r.read(ctx, wait)
		}
		if err != nil {
			return Entry{}, errors.WithMessage(err, "scanner error")
		}

		r.index.Add(1)
		return Entry{
			Data:  data,
			Index: index,
		}, nil
	}

	r.isInMemory = r.log.IsInMemory(index)

	if !r.isInMemory {
		ss := r.log.FindSegment(index)

		currentFile, err := os.Open(ss.Path())
		if err != nil {
			return Entry{}, errors.WithMessagef(err, "open segment file: %s", ss.Path())
		}
		r.currentFile.Store(currentFile)

		const buffSize = 64 * 1024 * 1024
		buff := bufio.NewReaderSize(currentFile, buffSize)
		currentReader := NewPrefixSizeDataReader(buff)
		r.currentReader = currentReader

		currentIndex := ss.FirstIndex()
		for currentIndex < index {
			_, err := currentReader.ReadNext(true)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return Entry{}, errors.WithMessagef(err, "read segment file: %s", ss.Path())
			}
			currentIndex++
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

type PrefixSizeDataReader struct {
	r *bufio.Reader
}

func NewPrefixSizeDataReader(r *bufio.Reader) *PrefixSizeDataReader {
	return &PrefixSizeDataReader{r: r}
}

func (r *PrefixSizeDataReader) ReadNext(skip bool) ([]byte, error) {
	payloadLen, err := binary.ReadUvarint(r.r)
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, io.EOF
	}
	if err != nil {
		return nil, err
	}

	if skip {
		_, err := r.r.Discard(int(payloadLen))
		if err != nil {
			return nil, err
		}
		return nil, nil
	}

	buf := make([]byte, payloadLen)
	_, err = io.ReadFull(r.r, buf)
	if err != nil {
		return nil, err
	}

	return buf, nil
}
