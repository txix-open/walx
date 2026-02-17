package sub_test

import (
	"sync"
	"testing"

	require2 "github.com/stretchr/testify/require"
	"github.com/txix-open/walx/v2/state/sub"
	"github.com/txix-open/walx/v2/tstate"
)

type IncRequest struct {
	Key string
}

type StateExample struct {
	sub.Router

	data map[string]int
	lock sync.Locker
}

func NewStateExample() *StateExample {
	state := &StateExample{
		data: make(map[string]int),
		lock: &sync.Mutex{},
	}
	sub.On(state, "inc", state.inc)
	return state
}

func (s *StateExample) inc(request IncRequest) (any, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.data[request.Key]++
	return s.data[request.Key], nil
}

func (s *StateExample) Inc(key string) (*int, error) {
	return sub.Emit[int](s, "inc", IncRequest{Key: key})
}

func (s *StateExample) Get(key string) int {
	s.lock.Lock()
	defer s.lock.Unlock()

	return s.data[key]
}

func TestSubState(t *testing.T) {
	require := require2.New(t)

	example := NewStateExample()
	tstate.ServeState(t, example)

	_, err := example.Inc("value1")
	require.NoError(err)
	_, err = example.Inc("value1")
	require.NoError(err)

	_, err = example.Inc("value2")
	require.NoError(err)

	value1 := example.Get("value1")
	require.EqualValues(2, value1)
	value2 := example.Get("value2")
	require.EqualValues(1, value2)
}
