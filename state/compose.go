package state

import (
	"bytes"

	"github.com/pkg/errors"
	"github.com/txix-open/walx/v2/pool"
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
	buff := pool.AcquireBuffer()

	buff.Write(m.prefix)
	buff.WriteByte(Separator)
	buff.Write(streamSuffix)
	suffix := buff.Bytes()

	result, err := m.delegate.Apply(event, suffix)

	pool.ReleaseBuffer(buff)

	return result, err
}

type composedFSM struct {
	stateByName map[string]NamedState
	primaryLen  int
}

func (c *composedFSM) SetMutator(state Mutator) {
	for name, fsm := range c.stateByName {
		mutator := newMutatorWithPrefix([]byte(name), state)
		fsm.SetMutator(mutator)
	}
}

func (c *composedFSM) SetPrimaryStream(stream []byte) {
	c.primaryLen = len(stream)
}

func (c *composedFSM) Apply(log Log) (any, error) {
	streamName := log.StreamName()
	if len(streamName) < c.primaryLen+2 {
		return nil, errors.New("invalid stream format. expected: streamPrefix/streamName")
	}
	firstSep := c.primaryLen

	secondSep := bytes.IndexByte(streamName[firstSep+1:], Separator)
	if secondSep < 0 {
		return nil, errors.New("invalid stream format. expected: streamPrefix/streamName")
	}
	secondSep += firstSep + 1

	stateName := streamName[firstSep+1 : secondSep]
	state, ok := c.stateByName[unsafe.BytesToString(stateName)]
	if !ok {
		return nil, errors.Errorf("fsm by prefix: '%s' not found", stateName)
	}

	// Reconstruct: primaryStream + "/" + parts[2]
	// parts[2] may be empty (when streamSuffix was nil)
	buff := pool.AcquireBuffer()

	buff.Write(streamName[:c.primaryLen])
	buff.WriteByte(Separator)
	buff.Write(streamName[secondSep+1:])
	log.streamName = buff.Bytes()

	result, err := state.Apply(log)

	pool.ReleaseBuffer(buff)

	return result, err
}

func ComposeV2(states ...NamedState) BusinessState {
	stateByName := make(map[string]NamedState)
	for _, state := range states {
		stateByName[state.StateName()] = state
	}
	return &composedFSM{
		stateByName: stateByName,
	}
}
