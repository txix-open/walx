package replication

import (
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/tidwall/wal"
	"google.golang.org/grpc"
	"walx"
	"walx/replication/replicator"
)

type ReaderSource interface {
	OpenReader(lastIndex uint64) *walx2.Reader
}

type Server struct {
	replicator.UnimplementedReplicatorServer
	wal ReaderSource
	srv *grpc.Server
}

func NewServer(wal ReaderSource) *Server {
	srv := grpc.NewServer()
	s := &Server{
		wal: wal,
		srv: srv,
	}
	replicator.RegisterReplicatorServer(srv, s)
	return s
}

func (s *Server) Begin(request *replicator.BeginRequest, server replicator.Replicator_BeginServer) error {
	reader := s.wal.OpenReader(request.LastIndex)
	defer reader.Close()

	for {
		entry, err := reader.Read()
		if errors.Is(err, wal.ErrClosed) {
			return nil
		}
		if err != nil {
			return err
		}

		err = server.Send(&replicator.Entry{
			Data:  entry.Data,
			Index: entry.Index,
		})
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func (s *Server) ListenAndServe(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}

	return s.Serve(lis)
}

func (s *Server) Serve(lis net.Listener) error {
	return s.srv.Serve(lis)
}

func (s *Server) Close() error {
	s.srv.GracefulStop()
	return nil
}
