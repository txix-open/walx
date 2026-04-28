package state_test

import (
	"fmt"
	"testing"

	require2 "github.com/stretchr/testify/require"
	"github.com/txix-open/isp-kit/test/fake"
	"github.com/txix-open/walx/v2/crud"
	"github.com/txix-open/walx/v2/state"
	"github.com/txix-open/walx/v2/tstate"
)

type Item1 struct {
	Id string
	X  string
}

func (i Item1) GetId() string {
	return i.Id
}

func BenchmarkCompose(b *testing.B) {
	require := require2.New(b)
	states := make([]state.NamedState, 0)
	for i := range 5 {
		s := crud.New[Item1](fmt.Sprintf("state_%d", i))
		states = append(states, s)
	}
	tstate.ServeState(b, state.Compose(states...))

	b.ReportAllocs()
	b.ResetTimer()
	s := states[0].(*crud.State[Item1])
	value := fake.It[Item1]()
	for i := 0; i < b.N; i++ {
		err := s.Upsert(value)
		require.NoError(err)
	}
}
