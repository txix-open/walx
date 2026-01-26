package keeper

import (
	"context"
	"fmt"
	"slices"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/txix-open/isp-kit/app"
	"github.com/txix-open/isp-kit/log"
	"github.com/txix-open/isp-kit/metrics"
	"github.com/txix-open/walx"
	"github.com/txix-open/walx/metric"
	"github.com/txix-open/walx/replication"
	"github.com/txix-open/walx/state"
	"github.com/txix-open/walx/stream"
	"golang.org/x/sync/errgroup"
)

type Keeper struct {
	name              string
	state             *state.State
	businessState     state.BusinessState
	replicationServer *replication.Server
	replicationClient *replication.Client
	options           options
	logger            log.Logger
}

func New(
	dir string,
	name string,
	businessState state.BusinessState,
	logger log.Logger,
	opts ...Option,
) (*Keeper, error) {
	options := newOptions()
	for _, opt := range opts {
		opt(options)
	}

	ctx := context.Background()
	ctx = log.ToContext(ctx, log.String("state", name))

	logger.Info(ctx, "start reading wal")
	wal, err := walx.Open(dir, options.walOptions...)
	if err != nil {
		return nil, errors.WithMessagef(err, "open wal for state %s", name)
	}
	logger.Info(ctx, "end reading wal")

	ss := state.New(wal, businessState, name)
	businessState.SetMutator(ss)

	logger.Info(ctx, "start state recovering")
	err = ss.Recovery(ctx)
	if err != nil {
		return nil, errors.WithMessagef(err, "recovery state %s", name)
	}
	logger.Info(ctx, "end state recovering")

	serverOptions := slices.Clone(options.replicationServerOptions)
	return &Keeper{
		name:              name,
		state:             ss,
		businessState:     businessState,
		logger:            logger,
		replicationServer: replication.NewServer(wal, logger, serverOptions...),
		options:           *options,
	}, nil
}

func (k *Keeper) State() *state.State {
	return k.state
}

func (k *Keeper) StreamWriter(streamName string) *stream.Writer {
	return stream.NewWriter(k.state, streamName)
}

func (k *Keeper) StopReplication() {
	if k.replicationClient != nil {
		_ = k.replicationClient.Close()
	}
	k.replicationClient = nil
}

func (k *Keeper) BeginReplication(address string) {
	if k.replicationClient != nil {
		return
	}

	k.replicationClient = replication.NewClient(
		k.state,
		k.name,
		address,
		k.options.filteredStreams,
		k.logger,
		k.options.replicationClientOptions...,
	)
	ctx := context.Background()
	go func() {
		err := k.replicationClient.Run(ctx)
		if err != nil {
			k.logger.Error(ctx, "run replication client", log.Any("error", err))
		}
	}()
}

func (k *Keeper) WalIndexMetric() metric.Metric {
	return metric.Metric{
		Name:        "wal_index",
		Description: "Log index from local Write Ahead Log for specific state",
		Labels:      []string{"state"},
		Collect: func() []metric.Value {
			return []metric.Value{
				metric.ValueOf(int(k.state.LastIndex()), k.name),
			}
		},
	}
}

func (k *Keeper) Run(ctx context.Context) error {
	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		err := k.state.Run(ctx)
		if err != nil {
			return errors.WithMessage(err, "serve state")
		}
		return nil
	})
	group.Go(func() error {
		if k.options.serverPort <= 0 {
			return nil
		}

		addr := fmt.Sprintf("0.0.0.0:%d", k.options.serverPort)
		k.logger.Info(ctx, "run wal replication server", log.String("address", addr), log.String("state", k.name))
		err := k.replicationServer.ListenAndServe(addr)
		if err != nil {
			return errors.WithMessage(err, "listen and serve wal replication")
		}
		return nil
	})
	return group.Wait()
}

func (k *Keeper) Close() error {
	closers := []app.Closer{
		k.replicationServer,
		app.CloserFunc(func() error {
			if k.replicationClient != nil {
				_ = k.replicationClient.Close()
			}
			return nil
		}),
		k.state,
	}
	for _, closer := range closers {
		err := closer.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func metricsHook() walx.Hook {
	sizeMetric := metrics.GetOrRegister[prometheus.Summary](
		metrics.DefaultRegistry,
		prometheus.NewSummary(prometheus.SummaryOpts{
			Name:       "wal_event_size",
			Help:       "Size of event in bytes is written to WAL",
			Objectives: metrics.DefaultObjectives,
		}),
	)
	writeTimeMetric := metrics.GetOrRegister[prometheus.Summary](
		metrics.DefaultRegistry,
		prometheus.NewSummary(prometheus.SummaryOpts{
			Name:       "wal_write_duration_ms",
			Help:       "Time to write event in milliseconds to WAL",
			Objectives: metrics.DefaultObjectives,
		}),
	)
	fsyncTimeMetric := metrics.GetOrRegister[prometheus.Summary](
		metrics.DefaultRegistry,
		prometheus.NewSummary(prometheus.SummaryOpts{
			Name:       "wal_fsync_duration_ms",
			Help:       "Time to call fsync in nanoseconds during WAL writing",
			Objectives: metrics.DefaultObjectives,
		}),
	)
	return func(data walx.HookData) {
		sizeMetric.Observe(float64(data.BytesWritten))
		writeTimeMetric.Observe(float64(data.WriteTime))
		if data.FSyncCalled {
			fsyncTimeMetric.Observe(float64(data.FSyncTime))
		}
	}
}
