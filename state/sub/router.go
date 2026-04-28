package sub

import (
	"bytes"

	"github.com/pkg/errors"
	"github.com/txix-open/walx/v2/state"
	unsafe2 "github.com/txix-open/walx/v2/unsafe"
)

type State interface {
	on(eventName string, handler handler)
	getMutator() state.Mutator
}

type handler struct {
	handler func(log state.Log) (any, error)
}

type Router struct {
	handlers map[string]handler
	mutator  state.Mutator
}

func (s *Router) SetMutator(mutator state.Mutator) {
	s.mutator = mutator
}

func (s *Router) Apply(log state.Log) (any, error) {
	_, streamSuffix, found := bytes.Cut(log.StreamName(), state.Separator)
	if !found {
		return nil, errors.New("invalid stream format. expected: streamName/streamSuffix")
	}

	eventName := unsafe2.BytesToString(streamSuffix)
	handler, ok := s.handlers[eventName]
	if !ok {
		return nil, errors.Errorf("unknown event: %s", eventName)
	}

	return handler.handler(log)
}

func (s *Router) on(eventName string, h handler) {
	if s.handlers == nil {
		s.handlers = map[string]handler{}
	}
	s.handlers[eventName] = h
}

func (s *Router) getMutator() state.Mutator {
	return s.mutator
}

func On[T any](s State, eventName string, h func(payload T) (any, error)) {
	ff := func(log state.Log) (any, error) {
		payload, err := state.UnmarshalEvent[T](log)
		if err != nil {
			return nil, errors.WithMessage(err, "unmarshal event")
		}
		return h(payload)
	}
	handler := handler{handler: ff}
	s.on(eventName, handler)
}

func Emit[T any](s State, eventName string, payload any) (*T, error) {
	return state.ApplyWithStreamSuffix[T](s.getMutator(), payload, unsafe2.StringToBytes(eventName))
}
