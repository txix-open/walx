package state

import (
	"bytes"

	"github.com/pkg/errors"
	"github.com/txix-open/walx/v2/unsafe"
)

type NamedState interface {
	BusinessState
	StateName() string
}

type mutatorWithPrefix struct {
	delegate Mutator
	prefix   []byte
}

func newMutatorWithPrefix(prefix []byte, delegate Mutator) mutatorWithPrefix {
	return mutatorWithPrefix{
		prefix:   prefix,
		delegate: delegate,
	}
}

func (m mutatorWithPrefix) Apply(event any, streamSuffix []byte) (any, error) {
	streamSuffix = bytes.Join([][]byte{m.prefix, streamSuffix}, Separator)
	return m.delegate.Apply(event, streamSuffix)
}

type composedFSM struct {
	stateByName map[string]NamedState
}

func (c composedFSM) SetMutator(state Mutator) {
	for name, fsm := range c.stateByName {
		mutator := newMutatorWithPrefix([]byte(name), state)
		fsm.SetMutator(mutator)
	}
}

func (c composedFSM) Apply(log Log) (any, error) {
	parts := bytes.SplitN(log.StreamName(), Separator, 3)
	if len(parts) < 3 {
		return nil, errors.New("invalid stream format. expected: streamPrefix/streamName")
	}

	state, ok := c.stateByName[unsafe.BytesToString(parts[1])]
	if !ok {
		return nil, errors.Errorf("fsm by prefix: '%s' not found", parts[1])
	}

	log.streamName = bytes.Join([][]byte{parts[0], parts[2]}, Separator)
	return state.Apply(log)
}

func ComposeV2(states ...NamedState) BusinessState {
	stateByName := make(map[string]NamedState)
	for _, state := range states {
		stateByName[state.StateName()] = state
	}
	return composedFSM{
		stateByName: stateByName,
	}
}
