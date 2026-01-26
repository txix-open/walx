package stream

import (
	"github.com/pkg/errors"
	"github.com/txix-open/walx/state"
)

func ReadMessage[T any](data []byte) (T, error) {
	_, eventData := state.UnpackEvent(data)

	result, err := state.UnmarshalEvent[T](state.NewLog(eventData))
	if err != nil {
		return result, errors.WithMessage(err, "unmarshal event")
	}

	return result, nil
}
