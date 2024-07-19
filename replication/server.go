package replication

import (
	"context"
	"fmt"
	"github.com/txix-open/walx/stream"
	"google.golang.org/grpc/credentials"
	"io"
	"net"
	"runtime"

	"github.com/pkg/errors"
	"github.com/txix-open/isp-kit/log"
	"github.com/txix-open/isp-kit/requestid"
	"github.com/txix-open/walx"
	"github.com/txix-open/walx/replication/replicator"
	"google.golang.org/grpc"
)

type Server struct {
	replicator.UnimplementedReplicatorServer
	hotWal  *walx.Log
	srv     *grpc.Server
	options *serverOptions
	logger  log.Logger
}

func NewServer(hotWal *walx.Log, log log.Logger, opts ...ServerOption) *Server {
	options := newServerOptions()
	for _, opt := range opts {
		opt(options)
	}
	serverOpts := []grpc.ServerOption{}
	if options.tls != nil {
		serverOpts = append(serverOpts, grpc.Creds(credentials.NewTLS(options.tls)))
	}
	srv := grpc.NewServer(serverOpts...)
	s := &Server{
		hotWal:  hotWal,
		srv:     srv,
		options: options,
		logger:  log,
	}
	replicator.RegisterReplicatorServer(srv, s)
	return s
}

func (s *Server) Begin(request *replicator.BeginRequest, server replicator.Replicator_BeginServer) (err error) {
	ctx := log.ToContext(server.Context(), log.String("clientId", requestid.Next()))

	s.logger.Info(
		ctx,
		"new replication client connected",
		log.Any("lastIndex", request.LastIndex),
		log.Any("filteredStreams", request.FilteredStreams),
	)
	matcher := stream.NewMatcher(request.GetFilteredStreams())

	defer func() {
		s.logger.Info(ctx, "client will be disconnected")
		if err != nil {
			s.logger.Error(ctx, "unexpected error during replication", log.Any("error", err))
		}
	}()

	if !s.hotWal.IsInMemory(request.LastIndex + 1) {
		err := s.sendColdLogs(ctx, matcher, request.LastIndex, server)
		if err != nil {
			return errors.WithMessage(err, "sent cold logs")
		}
		return nil
	}

	reader := s.hotWal.OpenReader(request.LastIndex)
	defer reader.Close()

	defer func() {
		r := recover()
		if r == nil {
			return
		}
		recovered, ok := r.(error)
		if ok {
			err = recovered
		} else {
			err = fmt.Errorf("%v", r)
		}
		stack := make([]byte, 4<<10)
		length := runtime.Stack(stack, false)
		err = errors.Errorf("replication is not available. possibly lag is too big, max lag = 4GB.\n cause: %v %s\n", err, stack[:length])
	}()

	for {
		entry, err := reader.Read()
		if errors.Is(err, walx.ErrClosed) {
			return nil
		}
		if err != nil {
			return errors.WithMessage(err, "read next log entry")
		}

		var entryData []byte
		if matcher.Match(entry) {
			entryData = entry.Data
		}

		err = server.Send(&replicator.Entry{
			Data:  entryData,
			Index: entry.Index,
		})
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return errors.WithMessage(err, "send log entry")
		}
	}
}

func (s *Server) DebugWrite(ctx context.Context, request *replicator.WriteRequest) (*replicator.WriteResponse, error) {
	index, err := s.hotWal.Write(request.Data, func(index uint64) {

	})
	if err != nil {
		return nil, errors.WithMessage(err, "write debug message")
	}
	return &replicator.WriteResponse{Index: index}, nil
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

func (s *Server) sendColdLogs(ctx context.Context, matcher stream.Matcher, index uint64, server replicator.Replicator_BeginServer) error {
	s.logger.Info(ctx, "requested log is out of cache, sending cold logs")
	defer func() {
		s.logger.Info(ctx, "stop sending cold logs")
	}()

	wal, err := s.options.oldSegmentOpener()
	if err != nil {
		return errors.WithMessage(err, "open new wal")
	}
	defer wal.Close()

	reader := wal.OpenReader(index)
	defer reader.Close()

	for i := index; i < wal.LastIndex(); i++ {
		entry, err := reader.Read()
		if err != nil {
			return errors.WithMessage(err, "read next log entry")
		}

		var entryData []byte
		if matcher.Match(entry) {
			entryData = entry.Data
		}

		err = server.Send(&replicator.Entry{
			Data:  entryData,
			Index: entry.Index,
		})
		if err != nil {
			return errors.WithMessage(err, "send log entry")
		}
	}

	return nil
}
