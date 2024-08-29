package replication

import (
	"crypto/tls"

	"github.com/txix-open/walx"
)

type ServerOption func(opts *serverOptions)

type WalOpener func() (*walx.Log, error)

type serverOptions struct {
	tls              *tls.Config
	oldSegmentOpener WalOpener
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

func ServerOldSegmentOpener(oldSegmentOpener WalOpener) ServerOption {
	return func(opts *serverOptions) {
		opts.oldSegmentOpener = oldSegmentOpener
	}
}

func ServerMinIndexLagToLog(minIndexLagToLog int64) ServerOption {
	return func(opts *serverOptions) {
		if minIndexLagToLog > 0 {
			opts.minIndexLagToLog = minIndexLagToLog
		}
	}
}
