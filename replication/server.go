package replication

import (
	"context"
	"fmt"
	"io"
	"net"
	"runtime"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/txix-open/isp-kit/metrics"
	"github.com/txix-open/walx/stream"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"

	"github.com/pkg/errors"
	"github.com/txix-open/isp-kit/log"
	"github.com/txix-open/isp-kit/requestid"
	"github.com/txix-open/walx"
	"github.com/txix-open/walx/replication/replicator"
	"google.golang.org/grpc"
)

type Server struct {
	replicator.UnimplementedReplicatorServer
	wal     *walx.Log
	srv     *grpc.Server
	options *serverOptions
	logger  log.Logger

	cancelFuncs   map[string]context.CancelFunc
	mu            sync.Mutex
	indexLagGauge *prometheus.GaugeVec
}

func NewServer(wal *walx.Log, log log.Logger, opts ...ServerOption) *Server {
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
		wal:         wal,
		srv:         srv,
		options:     options,
		logger:      log,
		cancelFuncs: make(map[string]context.CancelFunc),
		mu:          sync.Mutex{},
		indexLagGauge: metrics.GetOrRegister(metrics.DefaultRegistry, prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Subsystem:   "wal",
			Name:        "index_lag",
			Help:        "Index lag from current master position",
			ConstLabels: nil,
		}, []string{"client_ip"})),
	}
	replicator.RegisterReplicatorServer(srv, s)
	return s
}

func (s *Server) BeginReplication(request *replicator.BeginRequest, server replicator.Replicator_BeginReplicationServer) (err error) {
	clientId := requestid.Next()
	ctx := log.ToContext(server.Context(), log.String("clientId", clientId))
	ctx, cancel := context.WithCancel(ctx)
	defer func() {
		s.mu.Lock()
		cancel()
		delete(s.cancelFuncs, clientId)
		s.mu.Unlock()
	}()
	s.mu.Lock()
	s.cancelFuncs[clientId] = cancel
	s.mu.Unlock()

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

	reader := s.wal.OpenReader(request.LastIndex)
	defer reader.Close()

	clientIp := s.getClientIp(ctx)
	gauge := s.indexLagGauge.WithLabelValues(clientIp)
	toSend := make([]*replicator.Entry, 0)
	var emptyData []byte
	for {
		entries, err := reader.ReadAtMost(ctx, int(request.Limit))
		switch {
		case errors.Is(err, walx.ErrClosed):
			return nil
		case errors.Is(err, context.Canceled):
			return nil
		case err != nil:
			return errors.WithMessage(err, "read next log entry")
		}

		s.logIndexLag(ctx, gauge, s.wal.LastIndex(), entries.LastIndex(), clientIp)

		toSend = toSend[:0]
		for _, entry := range entries {
			entryData := emptyData
			if matcher.Match(entry) {
				entryData = entry.Data
			}
			toSend = append(toSend, &replicator.Entry{
				Data:  entryData,
				Index: entry.Index,
			})
		}

		err = server.Send(&replicator.Entries{
			Entries: toSend,
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
	index, err := s.wal.Write(request.Data, func(index uint64) {

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
	s.mu.Lock()
	for _, cancel := range s.cancelFuncs {
		cancel()
	}
	s.mu.Unlock()
	s.srv.GracefulStop()
	return nil
}

func (s *Server) logIndexLag(ctx context.Context, gauge prometheus.Gauge, lastIdx uint64, clientIdx uint64, clientIp string) {
	indexLag := int64(lastIdx) - int64(clientIdx)
	gauge.Set(float64(indexLag))

	shouldLog := indexLag >= s.options.minIndexLagToLog && clientIdx%500 == 0
	if shouldLog {
		s.logger.Warn(ctx, fmt.Sprintf("client '%s' is lagging behind on index by %d positions", clientIp, indexLag))
	}
}

func (s *Server) getClientIp(ctx context.Context) string {
	peer, ok := peer.FromContext(ctx)
	if !ok {
		s.logger.Warn(ctx, "can't get peer info from context")
		return ""
	}
	host, _, err := net.SplitHostPort(peer.Addr.String())
	if err != nil {
		s.logger.Warn(ctx, errors.WithMessage(err, "split host & port"))
		return ""
	}
	return host
}
