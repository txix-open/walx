package replication_test

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/txix-open/walx/v2/state/codec/json"
	"github.com/txix-open/walx/v2/stream"

	"github.com/stretchr/testify/require"
	"github.com/txix-open/isp-kit/log"
	"github.com/txix-open/walx/v2"
	"github.com/txix-open/walx/v2/replication"
	"github.com/txix-open/walx/v2/state"
)

type fsm struct {
}

func (f fsm) Apply(log state.Log) (any, error) {
	return nil, nil
}

type logger struct {
	log.Logger
	errCounter *atomic.Int64
}

func newLogger(log log.Logger) *logger {
	return &logger{
		Logger:     log,
		errCounter: &atomic.Int64{},
	}
}

func (l *logger) Error(ctx context.Context, message interface{}, fields ...log.Field) {
	l.Logger.Error(ctx, message, fields...)
	l.errCounter.Add(1)
}

func TestOutOfCache(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	log, err := log.New()
	require.NoError(err)
	logger := newLogger(log)

	mDir := dir()
	t.Cleanup(func() {
		_ = os.RemoveAll(mDir)
	})
	masterWal, err := walx.Open(mDir, walx.SegmentsCachePolicy(2, 10))
	require.NoError(err)
	t.Cleanup(func() {
		_ = masterWal.Close()
	})
	s := state.New(masterWal, fsm{}, json.NewCodec(), "test")
	ctx := context.Background()
	err = s.Recovery(ctx)
	require.NoError(err)
	go func() {
		err := s.Run(ctx)
		require.NoError(err)
	}()
	time.Sleep(300 * time.Millisecond)
	server := replication.NewServer(masterWal, logger)
	lis, addr := listener(require)
	go func() {
		err := server.Serve(lis)
		require.NoError(err)
	}()

	for i := 0; i < 512; i++ {
		data := make([]byte, 12)
		_, err := s.Write(data, func(index uint64) {

		})
		require.NoError(err)
	}

	for i := 0; i < 3; i++ {
		sDir := dir()
		t.Cleanup(func() {
			_ = os.RemoveAll(sDir)
		})
		slaveWal, err := walx.Open(sDir, walx.SegmentsCachePolicy(2, 10))
		require.NoError(err)
		cli := replication.NewClient(slaveWal, "test", addr, []string{stream.AllStreams}, logger)
		go func() {
			err := cli.Run(ctx)
			require.NoError(err)
		}()
	}

	go func() {
		for i := 0; i < 512; i++ {
			data := make([]byte, 12)
			_, err := s.Write(data, func(index uint64) {

			})
			require.NoError(err)
		}
	}()

	time.Sleep(25 * time.Second)
	require.EqualValues(0, logger.errCounter.Load())
}
