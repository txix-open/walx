package crud_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/txix-open/walx/crud"
	"github.com/txix-open/walx/state"
	"github.com/txix-open/walx/tstate"
)

type Item1 struct {
	Id string
	X  string
}

func (i Item1) GetId() string {
	return i.Id
}

type Item2 struct {
	Id string
	Y  string
}

func (i Item2) GetId() string {
	return i.Id
}

func TestState(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	crud1 := crud.New[Item1]("crud1")
	crud2 := crud.New[Item2]("crud2")

	tstate.ServeState(t, state.Compose(crud1, crud2))

	err := crud1.Upsert(Item1{
		Id: "1",
		X:  "test",
	})
	require.NoError(err)

	firstItem := crud2.Get("1")
	require.Nil(firstItem)

	item := crud1.Get("1")
	require.EqualValues(Item1{
		Id: "1",
		X:  "test",
	}, *item)

	item, err = crud1.Delete("2")
	require.Nil(item)
	require.EqualValues(err, crud.ErrNotFound)

	item, err = crud1.Delete("1")
	require.NoError(err)
	require.EqualValues(Item1{
		Id: "1",
		X:  "test",
	}, *item)

	item = crud1.Get("1")
	require.Nil(item)
}
