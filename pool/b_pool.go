package pool

import (
	"bytes"
	"sync"
)

var (
	bpool = sync.Pool{New: func() any {
		return bytes.NewBuffer(make([]byte, 1024))
	}}
)

func AcquireBuffer() *bytes.Buffer {
	buf := bpool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

func ReleaseBuffer(buf *bytes.Buffer) {
	bpool.Put(buf)
}
