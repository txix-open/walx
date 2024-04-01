package replication_test

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/txix-open/isp-kit/log"
	"github.com/txix-open/walx"
	"github.com/txix-open/walx/replication"
	"github.com/txix-open/walx/state"
)

type fsm struct {
}

func (f fsm) Apply(log []byte) (any, error) {
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
	s := state.New(masterWal, fsm{}, "test")
	err = s.Recovery()
	require.NoError(err)
	go func() {
		err := s.Run(context.Background())
		require.NoError(err)
	}()
	time.Sleep(300 * time.Millisecond)
	server := replication.NewServer(masterWal, func() (*walx.Log, error) {
		return walx.Open(mDir, walx.SegmentsCachePolicy(2, 10))
	}, logger)
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
	go func() {
		for i := 0; i < 512; i++ {
			data := make([]byte, 12)
			_, err := s.Write(data, func(index uint64) {

			})
			require.NoError(err)
		}
	}()

	for i := 0; i < 3; i++ {
		sDir := dir()
		t.Cleanup(func() {
			_ = os.RemoveAll(sDir)
		})
		slaveWal, err := walx.Open(sDir, walx.SegmentsCachePolicy(2, 10))
		require.NoError(err)
		cli := replication.NewClient(slaveWal, "test", addr, []string{replication.AllStreams}, logger)
		go func() {
			err := cli.Run(context.Background())
			require.NoError(err)
		}()
	}

	time.Sleep(15 * time.Second)
	require.EqualValues(0, logger.errCounter.Load())
}
