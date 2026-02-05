package keeper

import (
	"github.com/txix-open/walx/v2"
	"github.com/txix-open/walx/v2/replication"
	"github.com/txix-open/walx/v2/state"
	"github.com/txix-open/walx/v2/state/codec/json"
	"github.com/txix-open/walx/v2/stream"
)

type options struct {
	serverPort               int
	filteredStreams          []string
	walOptions               []walx.Option
	replicationClientOptions []replication.ClientOption
	replicationServerOptions []replication.ServerOption
	codec                    state.Codec
}

func newOptions() *options {
	return &options{
		serverPort:      0,
		filteredStreams: []string{stream.AllStreams},
		walOptions: []walx.Option{
			walx.WithHook(metricsHook()),
		},
		codec: json.NewCodec(),
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

func WithReplicationServerOptions(opts ...replication.ServerOption) Option {
	return func(o *options) {
		o.replicationServerOptions = append(o.replicationServerOptions, opts...)
	}
}

func WithCodec(codec state.Codec) Option {
	return func(o *options) {
		o.codec = codec
	}
}
