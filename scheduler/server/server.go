package server

import (
	"aliyun/serverless/mini-faas/scheduler/model"
	"aliyun/serverless/mini-faas/scheduler/utils/logger"
	"sync"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

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
	// s.router.Start()
}

func (s *Server) AcquireContainer(ctx context.Context, req *pb.AcquireContainerRequest) (*pb.AcquireContainerReply, error) {
	if req.AccountId == "" {
		return nil, grpc.Errorf(codes.InvalidArgument, "account RequestID cannot be empty")
	}
	if req.FunctionConfig == nil {
		return nil, grpc.Errorf(codes.InvalidArgument, "function config cannot be nil")
	}
	logger.Infof("request id: %s, request function name: %s", req.RequestId, req.FunctionName)
	now := time.Now().UnixNano()
	reply, err := s.router.AcquireContainer(ctx, req)
	latency := (time.Now().UnixNano() - now) / 1e6
	if err != nil {
		logger.Errorf("request id: %s, function name: %s, Failed to acquire due to %v, Latency: %d",
			req.RequestId, req.FunctionName, err, latency)
		return nil, err
	}
	logger.Infof("request id: %s, AcquireContainer, Latency: %d", req.RequestId, latency)
	requestStatusObj, _ := s.router.RequestMap.Get(req.RequestId)
	requestStatus := requestStatusObj.(*core.RequestStatus)
	requestStatus.ScheduleAcquireContainerLatency = latency
	return reply, nil
}

func (s *Server) ReturnContainer(ctx context.Context, req *pb.ReturnContainerRequest) (*pb.ReturnContainerReply, error) {
	now := time.Now().UnixNano()
	err := s.router.ReturnContainer(ctx, &model.ResponseInfo{
		RequestID:             req.RequestId,
		ContainerId:           req.ContainerId,
		MaxMemoryUsageInBytes: req.MaxMemoryUsageInBytes,
		DurationInNanos:       req.DurationInNanos / 1e6,
	})

	latency := (time.Now().UnixNano() - now) / 1e6

	if req.ErrorMessage != "" {
		logger.Errorf("request id: %s, ReturnContainerRequest error: %s", req.RequestId, req.ErrorMessage)
	}

	if err != nil {
		logger.Errorf("Failed to return due to %v, Latency: %d", err, latency)
		return nil, err
	}

	return &pb.ReturnContainerReply{}, nil
}
