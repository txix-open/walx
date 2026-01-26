package state

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/txix-open/walx"
	"github.com/txix-open/walx/pool"
)

type Log struct {
	serializedEvent []byte
	event           any
}

func NewLog(serializedEvent []byte) Log {
	return Log{
		serializedEvent: serializedEvent,
	}
}

type FSM interface {
	Apply(log Log) (any, error)
}

type Mutator interface {
	Apply(event any, streamSuffix []byte) (any, error)
}

type BusinessState interface {
	FSM
	SetMutator(state Mutator)
}

type State struct {
	*walx.Log
	fsm           FSM
	futures       *sync.Map
	primaryStream []byte
}

func New(log *walx.Log, fsm FSM, primaryStream string) *State {
	return &State{
		Log:           log,
		fsm:           fsm,
		futures:       &sync.Map{},
		primaryStream: []byte(primaryStream),
	}
}

func (s *State) Recovery(ctx context.Context) error {
	firstIdx, err := s.FirstIndex()
	if err != nil {
		return err
	}
	if firstIdx > 0 {
		firstIdx--
	}

	reader := s.Log.OpenReader(firstIdx)
	defer reader.Close()

	lastIndex := s.Log.LastIndex()
	for i := firstIdx; i < lastIndex; i++ {
		entry, err := reader.Read(ctx)
		if err != nil {
			return err
		}

		streamName, data := UnpackEvent(entry.Data)
		log := Log{serializedEvent: data}
		if MatchStream(streamName, s.primaryStream) {
			_, _ = s.fsm.Apply(log)
		}
	}

	return nil
}

func (s *State) Apply(event any, streamSuffix []byte) (any, error) {
	buff := pool.AcquireBuffer()
	err := PackEvent(s.primaryStream, streamSuffix, event, buff)
	if err != nil {
		return nil, fmt.Errorf("pack event: %w", err)
	}

	future := newFuture(event)
	_, err = s.Log.Write(buff.Bytes(), func(index uint64) {
		s.futures.Store(index, future)
	})
	if err != nil {
		return nil, fmt.Errorf("write: %w", err)
	}
	pool.ReleaseBuffer(buff)

	return future.wait()
}

func (s *State) Run(ctx context.Context) error {
	lastIndex := s.Log.LastIndex()
	reader := s.Log.OpenReader(lastIndex)
	defer reader.Close()

	for {
		entry, err := reader.Read(ctx)
		if errors.Is(err, walx.ErrClosed) {
			return nil
		}
		if err != nil {
			return err
		}

		streamName, data := UnpackEvent(entry.Data)
		if !MatchStream(streamName, s.primaryStream) {
			continue
		}

		featureValue, _ := s.futures.LoadAndDelete(entry.Index)
		future, ok := featureValue.(*future)

		log := Log{serializedEvent: data}
		if ok && future.event != nil {
			log.event = future.event
		}

		response, err := s.fsm.Apply(log)

		if ok {
			future.complete(response, err)
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

func Apply[T any](mutator Mutator, event any) (res *T, err error) {
	return ApplyWithStreamSuffix[T](mutator, event, nil)
}

func ApplyWithStreamSuffix[T any](mutator Mutator, event any, streamSuffix []byte) (res *T, err error) {
	if mutator == nil {
		return nil, errors.New("state is not initialized properly, mutator is nil")
	}
	result, err := mutator.Apply(event, streamSuffix)
	if err != nil {
		return nil, err
	}
	t, ok := result.(T)
	if !ok {
		return nil, fmt.Errorf("unexpected result type. expected %T, got %T", res, result)
	}
	return &t, nil
}
