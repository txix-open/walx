package state_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/txix-open/walx"
	"github.com/txix-open/walx/state"
)

type v struct {
	Value int
}

type events struct {
	Add *v
	Sub *v
}

type businessState struct {
	value int
}

func (s *businessState) Apply(log []byte) (any, error) {
	e := events{}
	err := state.UnmarshalEvent(log, &e)
	if err != nil {
		return nil, err
	}
	switch {
	case e.Add != nil:
		s.value += e.Add.Value
		return s.value, nil
	case e.Sub != nil:
		s.value -= e.Sub.Value
		return s.value, nil
	}
	return nil, errors.New("unexpected event")
}

func TestState(t *testing.T) {
	t.Parallel()

	dir := dir()
	t.Cleanup(func() {
		_ = os.RemoveAll(dir)
	})
	require := require.New(t)

	wal := createWal(dir, require)
	s := businessState{}
	ss := state.New(wal, &s, "test")
	go func() {
		err := ss.Run(context.Background())
		require.NoError(err)
	}()
	time.Sleep(1 * time.Second) // we must run wait before first run

	newValue, err := ss.Apply(events{Add: &v{13}}, nil)
	require.NoError(err)
	require.EqualValues(13, newValue)

	newValue, err = ss.Apply(events{Sub: &v{8}}, nil)
	require.NoError(err)
	require.EqualValues(5, newValue)

	err = ss.Close()
	require.NoError(err)

	wal = createWal(dir, require)
	s = businessState{}
	ss = state.New(wal, &s, "test")
	ctx := context.Background()
	err = ss.Recovery(ctx)
	require.NoError(err)

	require.EqualValues(5, s.value)

	go func() {
		err := ss.Run(ctx)
		require.NoError(err)
	}()
	time.Sleep(1 * time.Second) // we must run wait before first run

	newValue, err = ss.Apply(events{Add: &v{5}}, nil)
	require.NoError(err)
	require.EqualValues(10, newValue)
	require.EqualValues(10, s.value)
}

func dir() string {
	d := make([]byte, 8)
	_, _ = rand.Read(d)
	return hex.EncodeToString(d)
}

func createWal(dir string, require *require.Assertions) *walx.Log {
	wal, err := walx.Open(dir)
	require.NoError(err)
	return wal
}
