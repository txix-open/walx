package replication

import (
	"crypto/tls"
	"time"
)

type ClientOption func(o *clientOptions)

type clientOptions struct {
	reconnectTimeout  time.Duration
	logIntervalInTime time.Duration
	logIntervalIndex  uint64
	batchSize         int32
	tls               *tls.Config
}

func newClientOptions() *clientOptions {
	return &clientOptions{
		reconnectTimeout:  1 * time.Second,
		logIntervalInTime: 5 * time.Second,
		logIntervalIndex:  500,
		batchSize:         100,
	}
}

func ReconnectTimeout(timeout time.Duration) ClientOption {
	return func(o *clientOptions) {
		o.reconnectTimeout = timeout
	}
}

func LogIntervalInTime(t time.Duration) ClientOption {
	return func(o *clientOptions) {
		o.logIntervalInTime = t
	}
}

func LogIntervalIndex(logEveryEntries uint64) ClientOption {
	return func(o *clientOptions) {
		o.logIntervalIndex = logEveryEntries
	}
}

func BatchSize(size int32) ClientOption {
	return func(o *clientOptions) {
		o.batchSize = size
	}
}

func ClientTls(cfg *tls.Config) ClientOption {
	return func(o *clientOptions) {
		o.tls = cfg
	}
}
