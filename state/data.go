package state

import (
	"bytes"
	"fmt"
	"io"

	"github.com/pkg/errors"
	"github.com/txix-open/walx/pool"
	"github.com/txix-open/walx/state/enc"
)

const (
	maxStreamNameSize = 255
)

func MarshalEvent(event any) ([]byte, error) {
	buff := pool.AcquireBuffer()
	err := EncodeEvent(buff, event)
	if err != nil {
		return nil, err
	}
	return buff.Bytes(), nil
}

func EncodeEvent(w io.Writer, event any) error {
	err := enc.EncodeInto(w, event)
	if err != nil {
		return fmt.Errorf("json marshal: %w", err)
	}
	return nil
}

func UnmarshalEvent(data []byte, event any) error {
	err := enc.Unmarshal(data, &event)
	if err != nil {
		return fmt.Errorf("json unmarshal: %w", err)
	}
	return nil
}

func PackEvent(primaryStream []byte, streamSuffix []byte, event any, w io.Writer) error {
	err := EncodeStreamData(primaryStream, streamSuffix, w)
	if err != nil {
		return err
	}

	err = EncodeEvent(w, event)
	if err != nil {
		return err
	}

	return nil
}

func EncodeStreamData(primaryStream []byte, streamSuffix []byte, w io.Writer) error {
	if len(primaryStream) == 0 {
		return errors.New("primaryStream is required")
	}

	streamNameSize := len(primaryStream) + len(streamSuffix)
	if len(streamSuffix) > 0 {
		streamNameSize++ //we add '/'
	}
	if streamNameSize > maxStreamNameSize {
		return errors.Errorf("full stream name is too long, max streamNameSize = %d", maxStreamNameSize)
	}

	_, err := w.Write([]byte{byte(streamNameSize)})
	if err != nil {
		return err
	}
	_, err = w.Write(primaryStream)
	if err != nil {
		return err
	}
	if len(streamSuffix) > 0 {
		_, err = w.Write([]byte{'/'})
		if err != nil {
			return err
		}
		_, err = w.Write(streamSuffix)
		if err != nil {
			return err
		}
	}

	return nil
}

func UnpackEvent(data []byte) (streamName []byte, eventData []byte) {
	if len(data) == 0 {
		return nil, nil
	}

	streamNameSize := data[0]

	streamName = data[1 : streamNameSize+1]
	eventData = data[streamNameSize+1:]

	return streamName, eventData
}

func MatchStream(fullStreamName []byte, primaryStream []byte) bool {
	if len(fullStreamName) == 0 {
		return false
	}
	if len(primaryStream) == 0 {
		return true
	}
	return bytes.HasPrefix(fullStreamName, primaryStream)
}
