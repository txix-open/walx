package walx

import (
	"context"
	"errors"
	"time"

	"github.com/txix-open/wal"
)

var (
	signal = struct{}{}
)

const (
	waitEntryTimeout = 100 * time.Millisecond
)

type Reader interface {
	Read(ctx context.Context) (Entry, error)
	ReadAtMost(ctx context.Context, limit int) (Entries, error)
	Close()

	notify()
	close()
	read(ctx context.Context, wait bool) (Entry, error)
}

func readAtMost(ctx context.Context, reader Reader, limit int) (Entries, error) {
	result := make([]Entry, 0)
	for len(result) < limit {
		shouldWait := len(result) == 0
		entry, err := reader.read(ctx, shouldWait)
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
