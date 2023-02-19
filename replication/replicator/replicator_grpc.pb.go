// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.21.12
// source: replication/replicator.proto

package replicator

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// ReplicatorClient is the client API for Replicator service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ReplicatorClient interface {
	Begin(ctx context.Context, in *BeginRequest, opts ...grpc.CallOption) (Replicator_BeginClient, error)
}

type replicatorClient struct {
	cc grpc.ClientConnInterface
}

func NewReplicatorClient(cc grpc.ClientConnInterface) ReplicatorClient {
	return &replicatorClient{cc}
}

func (c *replicatorClient) Begin(ctx context.Context, in *BeginRequest, opts ...grpc.CallOption) (Replicator_BeginClient, error) {
	stream, err := c.cc.NewStream(ctx, &Replicator_ServiceDesc.Streams[0], "/replication.Replicator/Begin", opts...)
	if err != nil {
		return nil, err
	}
	x := &replicatorBeginClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type Replicator_BeginClient interface {
	Recv() (*Entry, error)
	grpc.ClientStream
}

type replicatorBeginClient struct {
	grpc.ClientStream
}

func (x *replicatorBeginClient) Recv() (*Entry, error) {
	m := new(Entry)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// ReplicatorServer is the server API for Replicator service.
// All implementations must embed UnimplementedReplicatorServer
// for forward compatibility
type ReplicatorServer interface {
	Begin(*BeginRequest, Replicator_BeginServer) error
	mustEmbedUnimplementedReplicatorServer()
}

// UnimplementedReplicatorServer must be embedded to have forward compatible implementations.
type UnimplementedReplicatorServer struct {
}

func (UnimplementedReplicatorServer) Begin(*BeginRequest, Replicator_BeginServer) error {
	return status.Errorf(codes.Unimplemented, "method Begin not implemented")
}
func (UnimplementedReplicatorServer) mustEmbedUnimplementedReplicatorServer() {}

// UnsafeReplicatorServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ReplicatorServer will
// result in compilation errors.
type UnsafeReplicatorServer interface {
	mustEmbedUnimplementedReplicatorServer()
}

func RegisterReplicatorServer(s grpc.ServiceRegistrar, srv ReplicatorServer) {
	s.RegisterService(&Replicator_ServiceDesc, srv)
}

func _Replicator_Begin_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(BeginRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(ReplicatorServer).Begin(m, &replicatorBeginServer{stream})
}

type Replicator_BeginServer interface {
	Send(*Entry) error
	grpc.ServerStream
}

type replicatorBeginServer struct {
	grpc.ServerStream
}

func (x *replicatorBeginServer) Send(m *Entry) error {
	return x.ServerStream.SendMsg(m)
}

// Replicator_ServiceDesc is the grpc.ServiceDesc for Replicator service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var Replicator_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "replication.Replicator",
	HandlerType: (*ReplicatorServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Begin",
			Handler:       _Replicator_Begin_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "replication/replicator.proto",
}
