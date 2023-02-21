package state

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/tidwall/wal"
	"gitlab.elocont.ru/integration-system/walx"
)

type FSM interface {
	Apply(log []byte) (any, error)
}

type State struct {
	*walx.Log
	fsm     FSM
	futures *sync.Map
}

func New(log *walx.Log, fsm FSM) *State {
	return &State{
		Log:     log,
		fsm:     fsm,
		futures: &sync.Map{},
	}
}

func (s *State) Recovery() error {
	reader := s.Log.OpenReader(0)
	defer reader.Close()

	lastIndex := s.Log.LastIndex()
	for i := uint64(0); i < lastIndex; i++ {
		entry, err := reader.Read()
		if err != nil {
			return err
		}
		_, _ = s.fsm.Apply(entry.Data)
	}

	return nil
}

func (s *State) Apply(event any) (any, error) {
	data, err := MarshalEvent(event)
	if err != nil {
		return nil, fmt.Errorf("marshal event: %w", err)
	}

	future := newFuture()
	_, err = s.Log.Write(data, func(index uint64) {
		s.futures.Store(index, future)
	})
	if err != nil {
		return nil, fmt.Errorf("write: %w", err)
	}

	return future.wait()
}

func (s *State) Run(ctx context.Context) error {
	lastIndex := s.Log.LastIndex()
	reader := s.Log.OpenReader(lastIndex)
	defer reader.Close()

	for {
		entry, err := reader.Read()
		if errors.Is(err, wal.ErrClosed) {
			return nil
		}
		if err != nil {
			return err
		}

		response, err := s.fsm.Apply(entry.Data)

		value, _ := s.futures.LoadAndDelete(entry.Index)
		feature, ok := value.(*future)
		if ok {
			feature.complete(response, err)
		}
	}
}

func (s *State) Close() error {
	err := s.Log.Close()
	if err != nil {
		return err
	}

	s.futures.Range(func(key, value any) bool {
		completer, ok := value.(*future)
		if ok {
			completer.complete(nil, errors.New("state close"))
		}
		return true
	})

	return nil
}
