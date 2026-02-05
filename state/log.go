package state

type Log struct {
	serializedEvent []byte
	codec           Codec
	event           any
}

func NewLog(serializedEvent []byte, codec Codec) Log {
	return Log{
		serializedEvent: serializedEvent,
		codec:           codec,
	}
}

func (l Log) Unmarshal(eventPrt any) error {
	return l.codec.Decode(l.serializedEvent, eventPrt)
}
