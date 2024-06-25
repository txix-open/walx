package index_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/txix-open/isp-kit/test/fake"
	"github.com/txix-open/walx/crud"
	"github.com/txix-open/walx/crud/index"
	"github.com/txix-open/walx/tstate"
)

type Item struct {
	Id       string
	IndexKey string
}

func (i Item) GetId() string {
	return i.Id
}

func TestHashIndex(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	s, index := state(t)
	id := fake.It[string]()
	indexKey := fake.It[string]()
	err := s.Insert(Item{Id: id, IndexKey: indexKey})
	require.NoError(err)
	elems := index.Get(indexKey)
	require.Len(elems, 1)
	require.Equal(*s.Get(id), *elems[0])
	require.True(s.Get(id) == elems[0])

	err = s.Insert(Item{Id: id, IndexKey: indexKey})
	require.Error(err)
	elems = index.Get(indexKey)
	require.Len(elems, 1)

	err = s.Upsert(Item{Id: id, IndexKey: indexKey})
	require.NoError(err)
	elems = index.Get(indexKey)
	require.Len(elems, 1)

	_, err = s.Delete(id)
	require.NoError(err)
	elems = index.Get(indexKey)
	require.Empty(elems)

	n := 10
	for range n {
		err := s.Upsert(Item{
			Id:       fake.It[string](),
			IndexKey: indexKey,
		})
		require.NoError(err)
	}
	err = s.Upsert(Item{
		Id:       fake.It[string](),
		IndexKey: indexKey + "extra",
	})
	require.NoError(err)
	err = s.Insert(Item{Id: id, IndexKey: indexKey})
	require.NoError(err)
	elems = index.Get(indexKey)
	require.Len(elems, n+1)
	_, err = s.Delete(id)
	require.NoError(err)
	elems = index.Get(indexKey)
	require.Len(elems, n)

	err = s.DeleteAll()
	require.NoError(err)
	elems = index.Get(indexKey)
	require.Empty(elems)

	err = s.Upsert(Item{Id: id, IndexKey: ""})
	require.NoError(err)
	elems = index.Get("")
	require.Empty(elems)
}

func state(t *testing.T) (*crud.State[Item], *index.Hash[Item]) {
	t.Helper()

	state := crud.New[Item]("state")
	index := index.NewHash[Item](func(item *Item) string {
		return item.IndexKey
	})
	state.SetHooks(index.Hooks())
	tstate.ServeState(t, state)
	return state, index
}
