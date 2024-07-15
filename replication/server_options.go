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
}

func newServerOptions() *serverOptions {
	return &serverOptions{}
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
