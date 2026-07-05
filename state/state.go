package state

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/txix-open/walx/v2"
	"github.com/txix-open/walx/v2/pool"
)

type FSM interface {
	Apply(log Log) (any, error)
}

type Mutator interface {
	Apply(event any, streamSuffix []byte) (any, error)
}

type BusinessState interface {
	FSM
	SetMutator(mutator Mutator)
}

type primaryStreamSetter interface {
	SetPrimaryStream([]byte)
}

type State struct {
	*walx.Log
	codec         Codec
	fsm           FSM
	futures       map[uint64]*future
	futuresLock   sync.Locker
	primaryStream []byte
}

func New(log *walx.Log, fsm FSM, codec Codec, primaryStream string) *State {
	s := &State{
		Log:           log,
		codec:         codec,
		fsm:           fsm,
		futures:       make(map[uint64]*future, 10),
		futuresLock:   &sync.Mutex{},
		primaryStream: []byte(primaryStream),
	}
	if pss, ok := fsm.(primaryStreamSetter); ok {
		pss.SetPrimaryStream(s.primaryStream)
	}
	return s
}

func (s *State) Recovery(ctx context.Context) error {
	firstIdx, err := s.FirstIndex()
	if err != nil {
		return err
	}
	if firstIdx > 0 {
		firstIdx--
	}

	reader := s.Log.OpenInMemReader(firstIdx)
	defer reader.Close()

	lastIndex := s.Log.LastIndex()
	log := Log{}
	log.isInRecovery = true
	log.codec = s.codec
	for i := firstIdx; i < lastIndex; i++ {
		entry, err := reader.Read(ctx)
		if err != nil {
			return err
		}

		streamName, data := UnpackEvent(entry.Data)
		if !MatchStream(streamName, s.primaryStream) {
			continue
		}

		log.streamName = streamName
		log.serializedEvent = data
		_, _ = s.fsm.Apply(log)
		log.streamName = nil
		log.serializedEvent = nil
	}

	return nil
}

func (s *State) Apply(event any, streamSuffix []byte) (any, error) {
	buff := pool.AcquireBuffer()
	err := PackEvent(s.primaryStream, streamSuffix, event, s.codec, buff)
	if err != nil {
		return nil, fmt.Errorf("pack event: %w", err)
	}

	future := acquireFuture(event)
	_, err = s.Log.Write(buff.Bytes(), func(index uint64) {
		s.futuresLock.Lock()
		s.futures[index] = future
		s.futuresLock.Unlock()
	})
	if err != nil {
		return nil, fmt.Errorf("write: %w", err)
	}
	pool.ReleaseBuffer(buff)

	res, err := future.wait()
	releaseFuture(future)
	return res, err
}

func (s *State) Run(ctx context.Context) error {
	lastIndex := s.Log.LastIndex()
	reader := s.Log.OpenInMemReader(lastIndex)
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

		s.futuresLock.Lock()
		future, ok := s.futures[entry.Index]
		delete(s.futures, entry.Index)
		s.futuresLock.Unlock()

		log := NewLog(streamName, data, s.codec)
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

	s.futuresLock.Lock()
	for _, future := range s.futures {
		future.complete(nil, errors.New("state close"))
	}
	s.futuresLock.Unlock()

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
