package replication

import (
	"context"
	"google.golang.org/grpc/credentials"
	"io"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/txix-open/isp-kit/log"
	"github.com/txix-open/walx"
	"github.com/txix-open/walx/replication/replicator"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type Wal interface {
	WriteEntry(entry walx.Entry) error
	LastIndex() uint64
}

type Client struct {
	wal             Wal
	state           string
	remoteAddr      string
	close           chan struct{}
	closed          *atomic.Bool
	filteredStreams []string
	logger          log.Logger
	options         *clientOptions

	grpcCli *grpc.ClientConn
}

func NewClient(
	wal Wal,
	state string,
	remoteAddr string,
	filteredStreams []string,
	logger log.Logger,
	opts ...ClientOption,
) *Client {
	options := newClientOptions()
	for _, opt := range opts {
		opt(options)
	}
	return &Client{
		remoteAddr:      remoteAddr,
		state:           state,
		wal:             wal,
		close:           make(chan struct{}),
		closed:          &atomic.Bool{},
		filteredStreams: filteredStreams,
		options:         options,
		logger:          logger,
	}
}

func (c *Client) Run(ctx context.Context) error {
	defer close(c.close)

	ctx = log.ToContext(ctx, log.String("state", c.state))
	go c.logReplicationIndex(ctx)
	for {
		if c.closed.Load() {
			return nil
		}

		reader, err := c.begin(ctx)
		if err != nil {
			c.logger.Error(ctx, "unexpected error during replication, begin replication", log.Any("error", err), log.Any("lastIndex", c.wal.LastIndex()))
			<-time.After(c.options.reconnectTimeout)
			continue
		}

		for {
			entry, err := reader.Recv()
			if status.Code(err) == codes.Canceled || c.closed.Load() {
				c.logger.Info(ctx, "stop replication, close signal received", log.Any("lastIndex", c.wal.LastIndex()))
				return nil
			}
			if err != nil {
				if errors.Is(err, io.EOF) {
					c.logger.Info(ctx, "pause replication, remote server was closed", log.Any("lastIndex", c.wal.LastIndex()))
				} else {
					c.logger.Error(ctx, "unexpected error during replication, read next entry", log.Any("error", err), log.Any("lastIndex", c.wal.LastIndex()))
				}
				<-time.After(c.options.reconnectTimeout)
				break
			}

			err = c.wal.WriteEntry(walx.Entry{
				Data:  entry.Data,
				Index: entry.Index,
			})
			if err != nil {
				return errors.WithMessage(err, "wal write entry")
			}

			lastIndex := c.wal.LastIndex()
			if lastIndex%c.options.logIntervalIndex == 0 {
				c.logger.Info(ctx, "replication in progress", log.Any("lastIndex", lastIndex))
			}
		}
	}
}

func (c *Client) begin(ctx context.Context) (replicator.Replicator_BeginClient, error) {
	if c.grpcCli != nil {
		_ = c.grpcCli.Close()
	}

	var err error
	dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	dialOpts := []grpc.DialOption{
		grpc.WithBlock(),
	}
	if c.options.tls != nil {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(c.options.tls)))
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	c.grpcCli, err = grpc.DialContext(
		dialCtx,
		c.remoteAddr,
		dialOpts...,
	)
	if err != nil {
		return nil, errors.WithMessagef(err, "grpc dial to %s", c.remoteAddr)
	}
	replCli := replicator.NewReplicatorClient(c.grpcCli)

	lastIndex := c.wal.LastIndex()
	c.logger.Info(ctx, "begin state replication", log.String("remoteAddress", c.remoteAddr), log.Any("lastIndex", lastIndex))
	reader, err := replCli.Begin(ctx, &replicator.BeginRequest{
		LastIndex:       lastIndex,
		FilteredStreams: c.filteredStreams,
	})
	if err != nil {
		return nil, errors.WithMessage(err, "call begin")
	}

	return reader, nil
}

func (c *Client) logReplicationIndex(ctx context.Context) {
	timer := time.NewTimer(c.options.logIntervalInTime)
	for {
		select {
		case <-timer.C:
			c.logger.Info(ctx, "current replication index", log.Any("lastIndex", c.wal.LastIndex()))
			timer.Reset(c.options.logIntervalInTime)
		case <-c.close:
			return
		}
	}
}

func (c *Client) Close() error {
	c.closed.Store(true)
	if c.grpcCli != nil {
		_ = c.grpcCli.Close()
	}
	<-c.close
	return nil
}
