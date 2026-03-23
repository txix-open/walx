package state

type Log struct {
	streamName      []byte
	serializedEvent []byte
	isInRecovery    bool
	codec           Codec
	event           any
}

func NewLog(
	streamName []byte,
	serializedEvent []byte,
	codec Codec,
) Log {
	return Log{
		streamName:      streamName,
		serializedEvent: serializedEvent,
		codec:           codec,
	}
}

func (l Log) StreamName() []byte {
	return l.streamName
}

func (l Log) IsInRecovery() bool {
	return l.isInRecovery
}

func (l Log) Unmarshal(eventPtr any) error {
	return l.codec.Decode(l.serializedEvent, eventPtr)
}

func (l Log) SerializedEvent() []byte {
	return l.serializedEvent
}
