package replication_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net"
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

func TestReplicationChain(t *testing.T) {
	t.Parallel()

	require := require.New(t)
	logger, err := log.New()
	require.NoError(err)

	masterWal := createWal(t, require)
	srv := replication.NewServer(masterWal, logger)
	lis, addr := listener(require)
	go func() {
		err := srv.Serve(lis)
		require.NoError(err)
	}()

	slaveWal := createWal(t, require)
	cli := replication.NewClient(slaveWal, "test", addr, []string{"test"}, logger)
	go func() {
		err := cli.Run(context.Background())
		require.NoError(err)
	}()
	slaveReader := slaveWal.OpenReader(0)
	slaveCounter := &atomic.Int64{}
	go func() {
		for {
			entry, err := slaveReader.Read(context.Background())
			stream, data := state.UnpackEvent(entry.Data)
			require.EqualValues("test", string(stream))
			require.NoError(err)
			slaveCounter.Add(1)
			require.EqualValues([]byte("hello"), data)
		}
	}()

	for i := 0; i < 10000; i++ {
		_, err := masterWal.Write(append([]byte{4}, []byte("testhello")...), func(index uint64) {

		})
		require.NoError(err)
	}

	time.Sleep(2 * time.Second)
	require.NoError(masterWal.Close())

	require.EqualValues(10000, slaveCounter.Load())
}

func listener(require *require.Assertions) (net.Listener, string) {
	lis, err := net.Listen("tcp", "127.0.0.1:")
	require.NoError(err)
	return lis, lis.Addr().String()
}

func dir() string {
	d := make([]byte, 8)
	_, _ = rand.Read(d)
	return hex.EncodeToString(d)
}

func createWal(t *testing.T, require *require.Assertions) *walx.Log {
	dir := dir()
	t.Cleanup(func() {
		_ = os.RemoveAll(dir)
	})
	wal, err := walx.Open(dir)
	require.NoError(err)
	return wal
}
