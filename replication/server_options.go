package replication

import (
	"crypto/tls"

	"google.golang.org/grpc"
)

type ServerOption func(opts *serverOptions)

type serverOptions struct {
	tls               *tls.Config
	minIndexLagToLog  int64
	grpcServerOptions []grpc.ServerOption
}

func newServerOptions() *serverOptions {
	return &serverOptions{minIndexLagToLog: 100_000}
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
