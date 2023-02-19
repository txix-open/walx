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
	"walx"
	"walx/replication"
)

func TestReplicationChain(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	masterWal := createWal(t, require)
	srv := replication.NewServer(masterWal)
	lis, addr := listener(require)
	go func() {
		err := srv.Serve(lis)
		require.NoError(err)
	}()

	slaveWal := createWal(t, require)
	cli, err := replication.NewClient(slaveWal, addr)
	require.NoError(err)
	go func() {
		err := cli.RunReplication(context.Background())
		require.NoError(err)
	}()
	slaveReader := slaveWal.OpenReader(0)
	slaveCounter := &atomic.Int64{}
	go func() {
		for {
			entry, err := slaveReader.Read()
			require.NoError(err)
			slaveCounter.Add(1)
			require.EqualValues([]byte("hello"), entry.Data)
		}
	}()

	for i := 0; i < 10000; i++ {
		_, err := masterWal.Write([]byte("hello"), func(index uint64) {

		})
		require.NoError(err)
	}

	time.Sleep(1 * time.Second)
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

func createWal(t *testing.T, require *require.Assertions) *walx2.Log {
	dir := dir()
	t.Cleanup(func() {
		_ = os.RemoveAll(dir)
	})
	wal, err := walx2.Open(dir)
	require.NoError(err)
	return wal
}
