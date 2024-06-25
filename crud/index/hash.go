package index

import (
	"sync"

	"github.com/txix-open/walx/crud"
)

type Hash[T crud.WithId] struct {
	data       map[string][]*T
	keySuppler func(t *T) string
	readLock   sync.Locker
	writeLock  sync.Locker
}

func NewHash[T crud.WithId](keySuppler func(item *T) string) *Hash[T] {
	mu := &sync.RWMutex{}
	return &Hash[T]{
		data:       make(map[string][]*T),
		keySuppler: keySuppler,
		readLock:   mu.RLocker(),
		writeLock:  mu,
	}
}

func (h *Hash[T]) Get(key string) []*T {
	h.readLock.Lock()
	defer h.readLock.Unlock()

	return h.data[key]
}

func (h *Hash[T]) Hooks() crud.Hooks[T] {
	return crud.Hooks[T]{
		UpdateHooks: []crud.UpdateHook[T]{h.updateHook},
		InsertHooks: []crud.InsertHook[T]{h.insertHook},
		DeleteHooks: []crud.DeleteHook[T]{h.deleteHook},
		UpsertHooks: []crud.UpsertHook[T]{h.upsertHook},
	}
}

func (h *Hash[T]) updateHook(item *T, updated bool) {
	if !updated {
		return
	}

	h.index(item)
}

func (h *Hash[T]) insertHook(item *T, inserted bool) {
	if !inserted {
		return
	}

	h.index(item)
}

func (h *Hash[T]) deleteHook(item *T, deleted bool) {
	if !deleted {
		return
	}

	key := h.keySuppler(item)
	if key == "" {
		return
	}

	h.writeLock.Lock()
	defer h.writeLock.Unlock()

	arr := h.data[key]
	if len(arr) == 0 {
		return
	}

	newArr := make([]*T, 0)
	for _, elem := range arr {
		if (*elem).GetId() == (*item).GetId() {
			continue
		}
		newArr = append(newArr, elem)
	}
	if len(newArr) == 0 {
		delete(h.data, key)
		return
	}

	h.data[key] = newArr
}

func (h *Hash[T]) upsertHook(item *T, updated bool) {
	h.index(item)
}

func (h *Hash[T]) index(item *T) {
	key := h.keySuppler(item)
	if key == "" {
		return
	}

	h.writeLock.Lock()
	defer h.writeLock.Unlock()

	arr := h.data[key]
	slicesIsChanged := false
	for i, elem := range arr {
		if (*elem).GetId() == (*item).GetId() {
			arr[i] = item
			slicesIsChanged = true
			break
		}
	}
	if !slicesIsChanged {
		arr = append(arr, item)
	}

	h.data[key] = arr
}
