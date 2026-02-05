package crud_test

import (
	"github.com/txix-open/isp-kit/test/fake"
	"testing"

	"github.com/stretchr/testify/require"
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

func TestState_BulkUpsert(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	item1 := fake.It[Item1]()
	item2 := fake.It[Item1]()
	crud := crud.New[Item1]("crud")
	tstate.ServeState(t, crud)
	err := crud.BulkUpsert([]Item1{
		item1,
		item2,
	})
	require.NoError(err)

	receivedItem1 := crud.Get(item1.Id)
	require.EqualValues(item1, *receivedItem1)

	receivedItem2 := crud.Get(item2.Id)
	require.EqualValues(item2, *receivedItem2)
}
