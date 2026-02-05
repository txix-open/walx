package json

import (
	"io"

	"github.com/json-iterator/go"
	"github.com/modern-go/reflect2"
)

type Codec struct {
	api jsoniter.API
}

func NewCodec() Codec {
	api := jsoniter.Config{
		EscapeHTML:                    false,
		MarshalFloatWith6Digits:       true, // will lose precession
		ObjectFieldMustBeSimpleString: true, // do not unescape object field
	}.Froze()
	timeType := reflect2.TypeByName("time.Time")
	tc := NewTimeCodec(FullDateFormat)
	encExt := jsoniter.EncoderExtension{timeType: tc}
	decExt := jsoniter.DecoderExtension{timeType: tc}
	api.RegisterExtension(encExt)
	api.RegisterExtension(decExt)

	naming := &namingStrategyExtension{jsoniter.DummyExtension{}, lowerCaseFirstChar}
	api.RegisterExtension(naming)
	return Codec{
		api: api,
	}
}

func (j Codec) Encode(w io.Writer, event any) error {
	stream := j.api.BorrowStream(w)
	defer j.api.ReturnStream(stream)
	stream.WriteVal(event)
	stream.WriteRaw("\n")
	stream.Flush()
	return stream.Error
}

func (j Codec) Decode(data []byte, eventPtr any) error {
	return j.api.Unmarshal(data, eventPtr)
}
