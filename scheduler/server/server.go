package server

import (
	"aliyun/serverless/mini-faas/scheduler/model"
	"aliyun/serverless/mini-faas/scheduler/utils/logger"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"sync"

	"aliyun/serverless/mini-faas/scheduler/core"
	pb "aliyun/serverless/mini-faas/scheduler/proto"
)

type Server struct {
	sync.WaitGroup
	router *core.Router
}

func NewServer(router *core.Router) *Server {
	return &Server{
		router: router,
	}
}

func (s *Server) Start() {
	// Just in case the router has internal loops.
	s.router.Start()
}

func (s *Server) AcquireContainer(ctx context.Context, req *pb.AcquireContainerRequest) (*pb.AcquireContainerReply, error) {
	if req.AccountId == "" {
		return nil, grpc.Errorf(codes.InvalidArgument, "account RequestID cannot be empty")
	}
	if req.FunctionConfig == nil {
		return nil, grpc.Errorf(codes.InvalidArgument, "function config cannot be nil")
	}
	reply, err := s.router.AcquireContainer(ctx, req)
	if err != nil {
		logger.Errorf("request id: %s, function name: %s, Failed to acquire due to %v",
			req.RequestId, req.FunctionName, err)
		return nil, err
	}
	return reply, nil
}

func (s *Server) ReturnContainer(ctx context.Context, req *pb.ReturnContainerRequest) (*pb.ReturnContainerReply, error) {
	err := s.router.ReturnContainer(ctx, &model.ResponseInfo{
		RequestID:             req.RequestId,
		ContainerId:           req.ContainerId,
		MaxMemoryUsageInBytes: req.MaxMemoryUsageInBytes,
		DurationInNanos:       req.DurationInNanos / 1e6,
	})

	if req.ErrorMessage != "" {
		logger.Errorf("request id: %s, ReturnContainerRequest error: %s", req.RequestId, req.ErrorMessage)
	}

	if err != nil {
		logger.Errorf("request id: %s, Failed to return due to %v", req.RequestId, err)
		return nil, err
	}

	return &pb.ReturnContainerReply{}, nil
}
