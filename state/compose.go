package state

import (
	"github.com/pkg/errors"
	"github.com/tidwall/gjson"
)

const (
	NameField = "__state__"
)

var (
	ErrSkipApply = errors.New("skip apply")
)

type NamedState interface {
	BusinessState
	StateName() string
}

type composedFSM struct {
	states      []NamedState
	stateByName map[string]NamedState
}

func (c composedFSM) SetMutator(state Mutator) {
	for _, fsm := range c.states {
		fsm.SetMutator(state)
	}
}

func (c composedFSM) Apply(log Log) (any, error) {
	stateName := gjson.GetBytes(log.serializedEvent, NameField)
	if stateName.Exists() {
		state, ok := c.stateByName[stateName.Str]
		if ok {
			return state.Apply(log)
		}
	}

	//slow branch
	for _, fsm := range c.states {
		res, err := fsm.Apply(log)
		if errors.Is(err, ErrSkipApply) {
			continue
		}
		if err != nil {
			return nil, err
		}
		return res, nil
	}

	return nil, errors.New("fsm not found")
}

func Compose(states ...NamedState) BusinessState {
	stateByName := make(map[string]NamedState)
	for _, state := range states {
		stateByName[state.StateName()] = state
	}
	return composedFSM{
		states:      states,
		stateByName: stateByName,
	}
}
