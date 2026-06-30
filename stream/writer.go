package stream

import (
	"github.com/pkg/errors"
	"github.com/txix-open/walx/v2/pool"
	"github.com/txix-open/walx/v2/state"
)

type Log interface {
	Write(data []byte, nextIndex func(index uint64)) (uint64, error)
}

type Writer struct {
	log    Log
	stream []byte
	codec  state.Codec
}

func NewWriter(log Log, codec state.Codec, stream string) *Writer {
	return &Writer{
		log:    log,
		stream: []byte(stream),
		codec:  codec,
	}
}

func (w *Writer) WriteEventWithSuffix(event any, suffix []byte) error {
	buff := pool.AcquireBuffer()
	defer pool.ReleaseBuffer(buff)

	err := state.PackEvent(w.stream, suffix, event, w.codec, buff)
	if err != nil {
		return errors.WithMessage(err, "pack event")
	}

	_, err = w.log.Write(buff.Bytes(), func(index uint64) {})
	if err != nil {
		return errors.WithMessage(err, "log write")
	}

	return nil
}

func (w *Writer) WriteEvent(event any, suffix []byte) error {
	return w.WriteEventWithSuffix(event, nil)
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
