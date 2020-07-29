package server

import (
	"aliyun/serverless/mini-faas/scheduler/utils/logger"
	"encoding/json"
	"sync"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"aliyun/serverless/mini-faas/scheduler/core"
	"aliyun/serverless/mini-faas/scheduler/model"
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
		return nil, grpc.Errorf(codes.InvalidArgument, "account ID cannot be empty")
	}
	if req.FunctionConfig == nil {
		return nil, grpc.Errorf(codes.InvalidArgument, "function config cannot be nil")
	}
	now := time.Now().UnixNano()
	reply, err := s.router.AcquireContainer(ctx, req)
	latency := (time.Now().UnixNano() - now) / 1e6
	if err != nil {
		logger.WithFields(logger.Fields{
			"Operation": "AcquireContainer",
			"Latency":   latency,
			"Error":     true,
		}).Errorf("Failed to acquire due to %v", err)
		return nil, err
	}
	requestStatusObj, _ := s.router.RequestMap.Get(req.RequestId)
	requestStatus := requestStatusObj.(*core.RequestStatus)
	requestStatus.ScheduleAcquireContainerLatency = latency
	return reply, nil
}

func (s *Server) ReturnContainer(ctx context.Context, req *pb.ReturnContainerRequest) (*pb.ReturnContainerReply, error) {
	now := time.Now().UnixNano()
	err := s.router.ReturnContainer(ctx, &model.ResponseInfo{
		ID:                    req.RequestId,
		ContainerId:           req.ContainerId,
		MaxMemoryUsageInBytes: req.MaxMemoryUsageInBytes,
		DurationInNanos:       req.DurationInNanos / 1e6,
	})

	latency := (time.Now().UnixNano() - now) / 1e6

	if err != nil {
		logger.WithFields(logger.Fields{
			"Operation": "ReturnContainer",
			"Latency":   latency,
			"Error":     true,
		}).Errorf("Failed to acquire due to %v", err)
		return nil, err
	}
	// 更新本次调用相关信息
	requestStatusObj, _ := s.router.RequestMap.Get(req.RequestId)
	requestStatus := requestStatusObj.(*core.RequestStatus)
	requestStatus.ScheduleReturnContainerLatency = latency
	requestStatus.ResponseTime = requestStatus.ScheduleAcquireContainerLatency +
		requestStatus.ScheduleReturnContainerLatency + requestStatus.FunctionExecutionDuration
	data, _ := json.MarshalIndent(requestStatus, "", "    ")
	logger.Infof("\nrequest id: %s\n%s\n", req.RequestId, data)

	s.router.RequestMap.Remove(req.RequestId)

	return &pb.ReturnContainerReply{}, nil
}
