package stream_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/txix-open/walx/v2"
	"github.com/txix-open/walx/v2/state/codec/json"
	"github.com/txix-open/walx/v2/stream"
)

type testEvent struct {
	Value int    `json:"value"`
	Msg   string `json:"msg"`
}

func TestWriterWriteEvents(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	dir := dir()
	t.Cleanup(func() {
		_ = os.RemoveAll(dir)
	})

	wal, err := walx.Open(dir)
	require.NoError(err)
	t.Cleanup(func() {
		_ = wal.Close()
	})

	require.Equal(uint64(0), wal.LastIndex())

	codec := json.NewCodec()
	writer := stream.NewWriter(wal, codec, "test-stream")

	err = writer.WriteEvents([]byte("-suffix"),
		testEvent{Value: 1, Msg: "first"},
		testEvent{Value: 2, Msg: "second"},
		testEvent{Value: 3, Msg: "third"},
	)
	require.NoError(err)

	require.Equal(uint64(3), wal.LastIndex())

	reader := wal.OpenReader(0)
	d1, err := reader.Read(context.Background())
	require.NoError(err)
	require.Contains(string(d1.Data), "test-stream")
	require.Contains(string(d1.Data), "-suffix")
	require.Contains(string(d1.Data), `"value":1`)

	d2, err := wal.OpenReader(1).Read(context.Background())
	require.NoError(err)
	require.Contains(string(d2.Data), "test-stream")
	require.Contains(string(d2.Data), `-suffix`)
	require.Contains(string(d2.Data), `"value":2`)

	d3, err := wal.OpenReader(2).Read(context.Background())
	require.NoError(err)
	require.Contains(string(d3.Data), "test-stream")
	require.Contains(string(d3.Data), `-suffix`)
	require.Contains(string(d3.Data), `"value":3`)
}

func TestWriterWriteEventsSingle(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	dir := dir()
	t.Cleanup(func() {
		_ = os.RemoveAll(dir)
	})

	wal, err := walx.Open(dir)
	require.NoError(err)
	t.Cleanup(func() {
		_ = wal.Close()
	})

	require.Equal(uint64(0), wal.LastIndex())

	codec := json.NewCodec()
	writer := stream.NewWriter(wal, codec, "single")

	err = writer.WriteEvents(nil, testEvent{Value: 42, Msg: "only"})
	require.NoError(err)

	require.Equal(uint64(1), wal.LastIndex())

	data, err := wal.OpenReader(0).Read(context.Background())
	require.NoError(err)
	require.Contains(string(data.Data), `"value":42`)
	require.Contains(string(data.Data), `"msg":"only"`)
}

func TestWriterWriteEventsMultipleBatches(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	dir := dir()
	t.Cleanup(func() {
		_ = os.RemoveAll(dir)
	})

	wal, err := walx.Open(dir)
	require.NoError(err)
	t.Cleanup(func() {
		_ = wal.Close()
	})

	codec := json.NewCodec()
	writer := stream.NewWriter(wal, codec, "multi")

	err = writer.WriteEvents(nil,
		testEvent{Value: 1, Msg: "a"},
		testEvent{Value: 2, Msg: "b"},
	)
	require.NoError(err)
	require.Equal(uint64(2), wal.LastIndex())

	err = writer.WriteEvents(nil,
		testEvent{Value: 3, Msg: "c"},
		testEvent{Value: 4, Msg: "d"},
		testEvent{Value: 5, Msg: "e"},
	)
	require.NoError(err)
	require.Equal(uint64(5), wal.LastIndex())

	err = writer.WriteEvents(nil,
		testEvent{Value: 6, Msg: "f"},
	)
	require.NoError(err)
	require.Equal(uint64(6), wal.LastIndex())

	for i := uint64(0); i < 6; i++ {
		data, err := wal.OpenReader(i).Read(context.Background())
		require.NoError(err)

		expectedValue := i + 1
		require.Contains(string(data.Data), fmt.Sprintf(`"value":%d`, expectedValue))
	}
}

func TestWriterWriteEvent(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	dir := dir()
	t.Cleanup(func() {
		_ = os.RemoveAll(dir)
	})

	wal, err := walx.Open(dir)
	require.NoError(err)
	t.Cleanup(func() {
		_ = wal.Close()
	})

	require.Equal(uint64(0), wal.LastIndex())

	codec := json.NewCodec()
	writer := stream.NewWriter(wal, codec, "simple")

	err = writer.WriteEvent(testEvent{Value: 99, Msg: "no-suffix"})
	require.NoError(err)

	require.Equal(uint64(1), wal.LastIndex())

	data, err := wal.OpenReader(0).Read(context.Background())
	require.NoError(err)
	require.Contains(string(data.Data), `"value":99`)
}

func dir() string {
	d := make([]byte, 8)
	_, _ = rand.Read(d)
	return hex.EncodeToString(d)
}
