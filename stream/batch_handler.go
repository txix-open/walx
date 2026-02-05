package stream

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/txix-open/isp-kit/log"
	"github.com/txix-open/walx/v2"
)

type BatchHandlerAdapter interface {
	Handle(entries walx.Entries) error
}

type BatchHandler struct {
	adapter       BatchHandlerAdapter
	purgeInterval time.Duration
	maxSize       int
	batch         []walx.Entry
	c             chan walx.Entry
	shouldClose   *atomic.Bool
	closed        chan struct{}
	logCtx        context.Context
	logger        log.Logger
}

func NewBatchHandler(
	adapter BatchHandlerAdapter,
	purgeInterval time.Duration,
	maxSize int,
	logger log.Logger,
) *BatchHandler {
	handler := &BatchHandler{
		adapter:       adapter,
		purgeInterval: purgeInterval,
		maxSize:       maxSize,
		c:             make(chan walx.Entry),
		shouldClose:   &atomic.Bool{},
		closed:        make(chan struct{}),
		logCtx:        context.Background(),
		logger:        logger,
	}
	go handler.run()
	return handler
}

func (r *BatchHandler) Handle(ctx context.Context, entry walx.Entry) error {
	if r.shouldClose.Load() {
		return nil
	}

	r.logCtx = ctx
	r.c <- entry
	return nil
}

func (r *BatchHandler) run() {
	var timer *time.Timer
	defer func() {
		if len(r.batch) > 0 && !r.shouldClose.Load() {
			_ = r.adapter.Handle(r.batch)
		}
		if timer != nil {
			timer.Stop()
		}
		close(r.closed)
	}()
	for {
		if timer == nil {
			timer = time.NewTimer(r.purgeInterval)
		} else {
			timer.Reset(r.purgeInterval)
		}

		select {
		case item, ok := <-r.c:
			if !ok {
				return
			}
			r.batch = append(r.batch, item)
			if len(r.batch) < r.maxSize {
				continue
			}
		case <-timer.C:
			if len(r.batch) == 0 {
				continue
			}
		}

		for {
			if r.shouldClose.Load() {
				return
			}

			err := r.adapter.Handle(r.batch)
			if err != nil {
				r.logger.Error(r.logCtx, errors.WithMessage(err, "handle batch"))
				time.Sleep(1 * time.Second)
				continue
			}
			break
		}

		r.batch = nil
	}
}

func (r *BatchHandler) Close() {
	r.shouldClose.Store(true)
	close(r.c)
	<-r.closed
}
