package replication

import (
	"crypto/tls"
)

type ServerOption func(opts *serverOptions)

type serverOptions struct {
	tls              *tls.Config
	minIndexLagToLog int64
}

func newServerOptions() *serverOptions {
	return &serverOptions{minIndexLagToLog: 100_000}
}

func ServerTls(cfg *tls.Config) ServerOption {
	return func(opts *serverOptions) {
		opts.tls = cfg
	}
}

func ServerMinIndexLagToLog(minIndexLagToLog int64) ServerOption {
	return func(opts *serverOptions) {
		if minIndexLagToLog > 0 {
			opts.minIndexLagToLog = minIndexLagToLog
		}
	}
}
