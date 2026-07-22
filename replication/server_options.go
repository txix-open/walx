package replication

import (
	"crypto/tls"

	"google.golang.org/grpc"
)

const (
	mb                         = 1024 * 1024
	DefaultMaxMessageSizeBytes = 64 * mb
)

type ServerOption func(opts *serverOptions)

type serverOptions struct {
	maxMessageSizeBytes int
	tls                 *tls.Config
	minIndexLagToLog    int64
	grpcServerOptions   []grpc.ServerOption
}

func newServerOptions() *serverOptions {
	return &serverOptions{
		maxMessageSizeBytes: DefaultMaxMessageSizeBytes,
		minIndexLagToLog:    100_000,
	}
}

func ServerMaxMessageSize(maxBytesSize int) ServerOption {
	return func(opts *serverOptions) {
		opts.maxMessageSizeBytes = maxBytesSize
	}
}

func ServerTls(cfg *tls.Config) ServerOption {
	return func(o *serverOptions) {
		o.tls = cfg
	}
}

func ServerMinIndexLagToLog(minIndexLagToLog int64) ServerOption {
	return func(o *serverOptions) {
		if minIndexLagToLog > 0 {
			o.minIndexLagToLog = minIndexLagToLog
		}
	}
}

func GrpcServerOptions(opts ...grpc.ServerOption) ServerOption {
	return func(o *serverOptions) {
		o.grpcServerOptions = append(o.grpcServerOptions, opts...)
	}
}
