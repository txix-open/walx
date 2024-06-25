package stream

import (
	"github.com/pkg/errors"
	"github.com/txix-open/walx/pool"
	"github.com/txix-open/walx/state"
)

type Log interface {
	Write(data []byte, nextIndex func(index uint64)) (uint64, error)
}

type Writer struct {
	log    Log
	stream []byte
}

func NewWriter(log Log, stream string) *Writer {
	return &Writer{
		log:    log,
		stream: []byte(stream),
	}
}

func (w *Writer) WriteMessage(msg any) error {
	buff := pool.AcquireBuffer()
	defer pool.ReleaseBuffer(buff)

	err := state.PackEvent(w.stream, nil, msg, buff)
	if err != nil {
		return errors.WithMessage(err, "pack event")
	}

	_, err = w.log.Write(buff.Bytes(), func(index uint64) {})
	if err != nil {
		return errors.WithMessage(err, "log write")
	}

	return nil
}

func (w *Writer) WriteData(data []byte) error {
	buff := pool.AcquireBuffer()
	defer pool.ReleaseBuffer(buff)

	err := state.EncodeStreamData(w.stream, nil, buff)
	if err != nil {
		return errors.WithMessage(err, "pack event")
	}

	_, err = buff.Write(data)
	if err != nil {
		return errors.WithMessage(err, "write data")
	}

	_, err = w.log.Write(buff.Bytes(), func(index uint64) {})
	if err != nil {
		return errors.WithMessage(err, "log write")
	}

	return nil
}
