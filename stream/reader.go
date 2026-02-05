package stream

import (
	"github.com/pkg/errors"
	"github.com/txix-open/walx/v2/state"
)

func ReadMessage[T any](data []byte, codec state.Codec) (T, error) {
	_, eventData := state.UnpackEvent(data)

	result, err := state.UnmarshalEvent[T](state.NewLog(eventData, codec))
	if err != nil {
		return result, errors.WithMessage(err, "unmarshal event")
	}

	return result, nil
}
