package walx_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"walx"
)

func TestConcurrent(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	writers := 16
	readers := 10

	writeGroup, ctx := errgroup.WithContext(context.Background())
	writeGroup.SetLimit(writers)

	readersGroup, _ := errgroup.WithContext(ctx)

	dir := dir()
	t.Cleanup(func() {
		_ = os.RemoveAll(dir)
	})
	wal, err := walx.Open(dir)
	require.NoError(err)

	for i := 0; i < readers; i++ {
		reader := wal.OpenReader(0)
		readersGroup.Go(func() error {
			for i := 0; i < 10000; i++ {
				data, err := reader.Read()
				if err != nil {
					return err
				}
				require.EqualValues([]byte("hello"), data.Data)
			}
			return nil
		})
	}

	for i := 0; i < 10000; i++ {
		writeGroup.Go(func() error {
			_, err := wal.Write([]byte("hello"), func(index uint64) {

			})
			return err
		})
	}

	go func() {
		<-time.After(1 * time.Second)
		require.Fail("max time for waiting readers exceeded")
	}()

	err = readersGroup.Wait()
	require.NoError(err)

	err = writeGroup.Wait()
	require.NoError(err)

	err = wal.Close()
	require.NoError(err)
}

func dir() string {
	d := make([]byte, 8)
	_, _ = rand.Read(d)
	return hex.EncodeToString(d)
}
