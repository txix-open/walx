package walx

import (
	"time"
)

const (
	DefaultFsyncThresholdInBytes = 64 * 1024
	DefaultSegmentCacheSize      = 4
	DefaultSegmentSize           = 1 * 1024 * 1024 * 1024
)

type HookData struct {
	Index       uint64
	Data        []byte
	WriteTime   time.Duration
	FSyncCalled bool
	FSyncTime   time.Duration
}

type Hook func(data HookData)

type options struct {
	fsyncThreshold   int
	segmentCacheSize int
	segmentSize      int
	hook             Hook
}

func newOptions() *options {
	return &options{
		fsyncThreshold:   DefaultFsyncThresholdInBytes,
		segmentCacheSize: DefaultSegmentCacheSize,
		segmentSize:      DefaultSegmentSize,
		hook: func(data HookData) {

		},
	}
}

type Option func(o *options)

func SegmentsCachePolicy(inMemSegmentsCount int, segmentSizeInBytes int) Option {
	return func(o *options) {
		o.segmentCacheSize = inMemSegmentsCount
		o.segmentSize = segmentSizeInBytes
	}
}

func FsyncThreshold(thresholdInBytes int) Option {
	return func(o *options) {
		o.fsyncThreshold = thresholdInBytes
	}
}

func WithHook(hook Hook) Option {
	return func(o *options) {
		o.hook = hook
	}
}
