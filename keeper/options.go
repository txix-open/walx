package keeper

import (
	"github.com/txix-open/walx"
	"github.com/txix-open/walx/replication"
)

type options struct {
	serverPort               int
	filteredStreams          []string
	walOptions               []walx.Option
	replicationClientOptions []replication.ClientOption
}

func newOptions() *options {
	return &options{
		serverPort:      0,
		filteredStreams: []string{replication.AllStreams},
		walOptions: []walx.Option{
			walx.WithHook(metricsHook()),
		},
	}
}

type Option func(o *options)

func ServeWalOnPort(port int) Option {
	return func(o *options) {
		o.serverPort = port
	}
}

func FilterStreams(streams ...string) Option {
	return func(o *options) {
		o.filteredStreams = streams
	}
}

func WithWalOptions(opts ...walx.Option) Option {
	return func(o *options) {
		o.walOptions = append(o.walOptions, opts...)
	}
}

func WithReplicationClientOptions(opts ...replication.ClientOption) Option {
	return func(o *options) {
		o.replicationClientOptions = append(o.replicationClientOptions, opts...)
	}
}
