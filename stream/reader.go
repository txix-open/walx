package stream

import (
	"github.com/pkg/errors"
	"github.com/txix-open/walx/state"
)

func ReadMessage[T any](data []byte) (*T, error) {
	_, eventData := state.UnpackEvent(data)

	result := new(T)
	err := state.UnmarshalEvent(eventData, result)
	if err != nil {
		return nil, errors.WithMessage(err, "unmarshal event")
	}

	return result, nil
}
