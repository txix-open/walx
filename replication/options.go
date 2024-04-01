package replication

import (
	"time"
)

type ClientOption func(o *options)

type options struct {
	reconnectTimeout  time.Duration
	logIntervalInTime time.Duration
	logIntervalIndex  uint64
}

func newOptions() *options {
	return &options{
		reconnectTimeout:  1 * time.Second,
		logIntervalInTime: 5 * time.Second,
		logIntervalIndex:  500,
	}
}

func ReconnectTimeout(timeout time.Duration) ClientOption {
	return func(o *options) {
		o.reconnectTimeout = timeout
	}
}

func LogIntervalInTime(t time.Duration) ClientOption {
	return func(o *options) {
		o.logIntervalInTime = t
	}
}

func LogIntervalIndex(logEveryEntries uint64) ClientOption {
	return func(o *options) {
		o.logIntervalIndex = logEveryEntries
	}
}
