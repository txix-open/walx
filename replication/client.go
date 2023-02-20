package replication

import (
	"context"
	"errors"
	"fmt"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"walx"
	"walx/replication/replicator"
)

type Wal interface {
	WriteEntry(entry walx.Entry) error
	LastIndex() uint64
}

type Client struct {
	wal        Wal
	remoteAddr string
	grpcCli    *grpc.ClientConn
	cli        replicator.ReplicatorClient
}

func NewClient(wal Wal, remoteAddr string) (*Client, error) {
	grpcCli, err := grpc.Dial(remoteAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("grpc dial to %s: %w", remoteAddr, err)
	}
	replCli := replicator.NewReplicatorClient(grpcCli)
	return &Client{
		remoteAddr: remoteAddr,
		wal:        wal,
		grpcCli:    grpcCli,
		cli:        replCli,
	}, nil
}

func (c *Client) Run(ctx context.Context) error {
	lastIndex := c.wal.LastIndex()
	reader, err := c.cli.Begin(ctx, &replicator.BeginRequest{LastIndex: lastIndex})
	if err != nil {
		return fmt.Errorf("call grpc begin: %w", err)
	}

	for {
		entry, err := reader.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		err = c.wal.WriteEntry(walx.Entry{
			Data:  entry.Data,
			Index: entry.Index,
		})
		if err != nil {
			return fmt.Errorf("wal write entry: %w", err)
		}
	}
}

func (c *Client) Close() error {
	return c.grpcCli.Close()
}
