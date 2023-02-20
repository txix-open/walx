package state_test

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"walx"
	"walx/state"
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
	ss := state.New(wal, &s)
	go func() {
		err := ss.Run()
		require.NoError(err)
	}()

	newValue, err := ss.Apply(events{Add: &v{13}})
	require.NoError(err)
	require.EqualValues(13, newValue)

	newValue, err = ss.Apply(events{Sub: &v{8}})
	require.NoError(err)
	require.EqualValues(5, newValue)

	err = ss.Close()
	require.NoError(err)

	wal = createWal(dir, require)
	s = businessState{}
	ss = state.New(wal, &s)
	err = ss.Recovery()
	require.NoError(err)

	require.EqualValues(5, s.value)

	go func() {
		err := ss.Run()
		require.NoError(err)
	}()

	newValue, err = ss.Apply(events{Add: &v{5}})
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
