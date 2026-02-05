package stream

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/txix-open/isp-kit/log"
	"github.com/txix-open/wal"
	"github.com/txix-open/walx/v2"
)

type Handler interface {
	Handle(ctx context.Context, entry walx.Entry) error
}

type Source interface {
	OpenReader(startFrom uint64) walx.Reader
}

type closer interface {
	Close()
}

type Queue struct {
	worker      string
	handler     Handler
	reader      walx.Reader
	matcher     Matcher
	batchSize   int
	logger      log.Logger
	closed      chan struct{}
	shouldClose atomic.Bool
}

func New(
	worker string,
	startFrom uint64,
	source Source,
	handler Handler,
	filteredStreams []string,
	batchSize int,
	logger log.Logger,
) *Queue {
	reader := source.OpenReader(startFrom)
	return &Queue{
		worker:      worker,
		handler:     handler,
		reader:      reader,
		matcher:     NewMatcher(filteredStreams),
		batchSize:   batchSize,
		logger:      logger,
		closed:      make(chan struct{}),
		shouldClose: atomic.Bool{},
	}
}

func (r *Queue) Run() {
	go r.run()
}

func (r *Queue) run() {
	ctx := log.ToContext(context.Background(), log.String("worker", r.worker))
	defer func() {
		r.logger.Debug(ctx, "stop reading")
		closer, ok := r.handler.(closer)
		if ok {
			closer.Close()
		}
		close(r.closed)
	}()

	for {
		entry, err := r.reader.Read(ctx)
		if errors.Is(err, wal.ErrClosed) {
			return
		}
		if err != nil {
			r.logger.Error(ctx, errors.WithMessage(err, "read wal entry"))
			return
		}
		if len(entry.Data) == 0 {
			continue
		}
		if !r.matcher.Match(entry) {
			continue
		}

		for {
			if r.shouldClose.Load() {
				return
			}

			err = r.handler.Handle(ctx, entry)
			if err != nil {
				r.logger.Error(ctx, err)
				time.Sleep(1 * time.Second)
				continue
			}
			break
		}
	}
}

func (r *Queue) Close() {
	r.shouldClose.Store(true)
	r.reader.Close()
	<-r.closed
}
