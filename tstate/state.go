package tstate

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/txix-open/walx/v2"
	"github.com/txix-open/walx/v2/state"
	"github.com/txix-open/walx/v2/state/codec/json"
)

func ServeState[T state.BusinessState](t *testing.T, s T) T {
	businessState, _ := ServeStateWithState(t, s, t.Name())
	return businessState
}

func ServeStateWithState[T state.BusinessState](t *testing.T, s T, stateName string) (T, *state.State) {
	t.Helper()

	require := require.New(t)
	dir := dir()
	log, err := walx.Open(dir)
	require.NoError(err)
	state := state.New(log, s, json.NewCodec(), stateName)
	s.SetMutator(state)
	go func() {
		err := state.Run(context.Background())
		require.NoError(err)
	}()
	time.Sleep(100 * time.Millisecond)
	t.Cleanup(func() {
		err := state.Close()
		require.NoError(err)
		err = os.RemoveAll(dir)
		require.NoError(err)
	})
	return s, state
}

func dir() string {
	d := make([]byte, 8)
	_, _ = rand.Read(d)
	return hex.EncodeToString(d)
}
