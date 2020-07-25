package server

import (
	"aliyun/serverless/mini-faas/scheduler/utils/logger"
	"encoding/json"
	"math"
	"sync"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"aliyun/serverless/mini-faas/scheduler/core"
	"aliyun/serverless/mini-faas/scheduler/model"
	pb "aliyun/serverless/mini-faas/scheduler/proto"
)

type RequestStatus struct {
	FunctionName string
	ContainerId  string
	// ms
	ScheduleAcquireContainerLatency int64
	ScheduleReturnContainerLatency  int64
	FunctionExecutionDuration       int64
	ResponseTime                    int64
	// MB
	RequireMemory  int64
	MaxMemoryUsage int64
}

var RequestStatusMap = make(map[string]*RequestStatus) // request_id -> RequestStatus

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
	RequestStatusMap[req.RequestId] = &RequestStatus{
		FunctionName:                    req.FunctionName,
		ScheduleAcquireContainerLatency: latency,
		RequireMemory:                   int64(float64(req.FunctionConfig.MemoryInBytes) / (math.Pow(float64(1024), float64(2)))),
	}
	logger.WithFields(logger.Fields{
		"Operation":     "AcquireContainer",
		"FunctionName":  req.FunctionName,
		"RequestId":     req.RequestId,
		"Latency":       latency,
		"MemoryInBytes": req.FunctionConfig.MemoryInBytes,
	}).Infof("")
	if err != nil {
		logger.WithFields(logger.Fields{
			"Operation": "AcquireContainer",
			"Latency":   latency,
			"Error":     true,
		}).Errorf("Failed to acquire due to %v", err)
		return nil, err
	}
	return reply, nil
}

func (s *Server) ReturnContainer(ctx context.Context, req *pb.ReturnContainerRequest) (*pb.ReturnContainerReply, error) {
	now := time.Now().UnixNano()
	err := s.router.ReturnContainer(ctx, &model.ResponseInfo{
		ID:          req.RequestId,
		ContainerId: req.ContainerId,
	})

	// 本次调用相关信息
	latency := (time.Now().UnixNano() - now) / 1e6
	requestStatus := RequestStatusMap[req.RequestId]
	requestStatus.ContainerId = req.ContainerId
	requestStatus.ScheduleReturnContainerLatency = latency
	requestStatus.FunctionExecutionDuration = req.DurationInNanos / 1e6
	requestStatus.ResponseTime = requestStatus.ScheduleAcquireContainerLatency +
		requestStatus.ScheduleReturnContainerLatency + requestStatus.FunctionExecutionDuration
	requestStatus.MaxMemoryUsage = int64(float64(req.MaxMemoryUsageInBytes) / (math.Pow(float64(1024), float64(2))))
	logger.WithFields(logger.Fields{
		"Operation":             "ReturnContainer",
		"ContainerId":           req.ContainerId,
		"RequestId":             req.RequestId,
		"ErrorCode":             req.ErrorCode,
		"MaxMemoryUsageInBytes": req.MaxMemoryUsageInBytes,
		"Latency":               latency,
		"DurationInMs":          req.DurationInNanos / 1e6,
	}).Infof("")
	if err != nil {
		logger.WithFields(logger.Fields{
			"Operation": "ReturnContainer",
			"Latency":   latency,
			"Error":     true,
		}).Errorf("Failed to acquire due to %v", err)
		return nil, err
	}
	data, _ := json.MarshalIndent(requestStatus, "", "    ")
	logger.Infof("\nrequest id: %s\n%s\n", req.RequestId, data)

	return &pb.ReturnContainerReply{}, nil
}
