package crud

import (
	"errors"
	"sync"

	"github.com/txix-open/walx/state"
)

var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")
)

type UpdateHook[T WithId] func(t *T, updated bool)
type InsertHook[T WithId] func(t *T, inserted bool)
type DeleteHook[T WithId] func(t *T, deleted bool)
type UpsertHook[T WithId] func(t *T, updated bool)
type Hooks[T WithId] struct {
	UpdateHooks []UpdateHook[T]
	InsertHooks []InsertHook[T]
	DeleteHooks []DeleteHook[T]
	UpsertHooks []UpsertHook[T]
}

type nothing struct{}

type request[T WithId] struct {
	State             string `json:"__state__"`
	UpsertRequest     *T     `json:",omitempty"`
	UpdateRequest     *T     `json:",omitempty"`
	InsertRequest     *T     `json:",omitempty"`
	DeleteRequest     string `json:",omitempty"`
	DeleteAllRequest  bool   `json:",omitempty"`
	BulkUpsertRequest []T    `json:",omitempty"`
}

type WithId interface {
	GetId() string
}

type State[T WithId] struct {
	mutator      state.Mutator
	items        map[string]*T
	name         string
	streamSuffix []byte
	hooks        Hooks[T]
	readLock     sync.Locker
	writeLock    sync.Locker
}

func New[T WithId](name string) *State[T] {
	mu := &sync.RWMutex{}
	return &State[T]{
		name:         name,
		streamSuffix: []byte(name),
		items:        map[string]*T{},
		readLock:     mu.RLocker(),
		writeLock:    mu,
	}
}

func (s *State[T]) SetHooks(hooks Hooks[T]) {
	s.hooks = hooks
}

func (s *State[T]) SetMutator(mutator state.Mutator) {
	s.mutator = mutator
}

func (s *State[T]) All() []T {
	return s.Find(func(elem *T) bool {
		return true
	})
}

func (s *State[T]) Find(filter func(elem *T) bool) []T {
	s.readLock.Lock()
	defer s.readLock.Unlock()

	arr := make([]T, 0)
	for _, item := range s.items {
		if filter(item) {
			arr = append(arr, *item)
		}
	}
	return arr
}

func (s *State[T]) ForEach(f func(elem *T)) {
	s.readLock.Lock()
	defer s.readLock.Unlock()

	for _, item := range s.items {
		f(item)
	}
}

func (s *State[T]) Get(id string) *T {
	s.readLock.Lock()
	defer s.readLock.Unlock()

	item := s.items[id]
	return item
}

func (s *State[T]) Upsert(item T) error {
	_, err := state.ApplyWithStreamSuffix[nothing](s.mutator, request[T]{
		State:         s.name,
		UpsertRequest: &item,
	}, s.streamSuffix)
	return err
}

func (s *State[T]) upsert(item T) (any, error) {
	_, updated := s.items[item.GetId()]
	s.items[item.GetId()] = &item
	for _, hook := range s.hooks.UpsertHooks {
		hook(&item, updated)
	}
	return nothing{}, nil
}

func (s *State[T]) Delete(id string) (*T, error) {
	val, err := state.ApplyWithStreamSuffix[T](s.mutator, request[T]{
		State:         s.name,
		DeleteRequest: id,
	}, s.streamSuffix)
	return val, err
}

func (s *State[T]) delete(id string) (any, error) {
	item, ok := s.items[id]
	defer func() {
		for _, hook := range s.hooks.DeleteHooks {
			hook(item, ok)
		}
	}()
	if ok {
		delete(s.items, id)
		return *item, nil
	}
	return nil, ErrNotFound
}

func (s *State[T]) DeleteAll() error {
	_, err := state.ApplyWithStreamSuffix[nothing](s.mutator, request[T]{
		State:            s.name,
		DeleteAllRequest: true,
	}, s.streamSuffix)
	if err != nil {
		return err
	}
	return nil
}

func (s *State[T]) deleteAll() (any, error) {
	old := s.items
	s.items = map[string]*T{}
	for _, item := range old {
		for _, hook := range s.hooks.DeleteHooks {
			hook(item, true)
		}
	}
	return nothing{}, nil
}

func (s *State[T]) Update(item T) error {
	_, err := state.ApplyWithStreamSuffix[nothing](s.mutator, request[T]{
		State:         s.name,
		UpdateRequest: &item,
	}, s.streamSuffix)
	return err
}

func (s *State[T]) update(item T) (any, error) {
	id := item.GetId()
	_, ok := s.items[id]
	defer func() {
		for _, hook := range s.hooks.UpdateHooks {
			hook(&item, ok)
		}
	}()
	if !ok {
		return nil, ErrNotFound
	}
	s.items[id] = &item
	return nothing{}, nil
}

func (s *State[T]) Insert(item T) error {
	_, err := state.ApplyWithStreamSuffix[nothing](s.mutator, request[T]{
		State:         s.name,
		InsertRequest: &item,
	}, s.streamSuffix)
	return err
}

func (s *State[T]) insert(item T) (any, error) {
	id := item.GetId()
	_, ok := s.items[id]
	defer func() {
		for _, hook := range s.hooks.InsertHooks {
			hook(&item, !ok)
		}
	}()
	if ok {
		return nil, ErrAlreadyExists
	}
	s.items[id] = &item
	return nothing{}, nil
}

func (s *State[T]) BulkUpsert(items []T) error {
	_, err := state.ApplyWithStreamSuffix[nothing](s.mutator, request[T]{
		State:             s.name,
		BulkUpsertRequest: items,
	}, s.streamSuffix)
	return err
}

func (s *State[T]) bulkUpsert(items []T) (any, error) {
	for _, item := range items {
		_, updated := s.items[item.GetId()]
		s.items[item.GetId()] = &item
		for _, hook := range s.hooks.UpsertHooks {
			hook(&item, updated)
		}
	}
	return nothing{}, nil
}

func (s *State[T]) StateName() string {
	return s.name
}

func (s *State[T]) Apply(log state.Log) (any, error) {
	req, err := state.UnmarshalEvent[request[T]](log)
	if err != nil {
		return nil, err
	}
	if req.State != s.name {
		return nil, state.ErrSkipApply
	}

	s.writeLock.Lock()
	defer s.writeLock.Unlock()

	switch {
	case req.UpsertRequest != nil:
		return s.upsert(*req.UpsertRequest)
	case req.InsertRequest != nil:
		return s.insert(*req.InsertRequest)
	case req.UpdateRequest != nil:
		return s.update(*req.UpdateRequest)
	case req.DeleteRequest != "":
		return s.delete(req.DeleteRequest)
	case req.DeleteAllRequest:
		return s.deleteAll()
	case req.BulkUpsertRequest != nil:
		return s.bulkUpsert(req.BulkUpsertRequest)
	default:
		return nil, errors.New("handler not found")
	}
}
