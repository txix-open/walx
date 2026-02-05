package walx

import (
	"context"
	"time"
)

var (
	signal = struct{}{}
)

const (
	waitEntryTimeout = 500 * time.Millisecond
)

type Reader interface {
	Read(ctx context.Context) (Entry, error)
	ReadAtMost(ctx context.Context, limit int) (Entries, error)
	Close()
	notify()
	close()
}
