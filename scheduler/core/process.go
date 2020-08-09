package core

import (
	nsPb "aliyun/serverless/mini-faas/nodeservice/proto"
	rmPb "aliyun/serverless/mini-faas/resourcemanager/proto"
	cp "aliyun/serverless/mini-faas/scheduler/config"
	"aliyun/serverless/mini-faas/scheduler/model"
	"aliyun/serverless/mini-faas/scheduler/utils/logger"
	"context"
	"encoding/json"
	"sync/atomic"
)

var r *Router

func Process(router *Router) {
	r = router
	go func() {
		for {
			r.reserveNode()
			if r.nodeMap.num >= cp.MaxNodeNum {
				break
			}
		}
	}()

	go func() {
		for {
			responseInfo := <-ReturnContainerChan
			go processReturnContainer(responseInfo)
		}
	}()
}

func processReturnContainer(res *model.ResponseInfo) {
	rmObj, ok := r.RequestMap.Get(res.RequestID)
	if !ok {
		logger.Errorf("no request found with ContainerId %s", res.RequestID)
		return
	}
	requestStatus := rmObj.(*RequestStatus)
	requestStatus.FunctionExecutionDuration = res.DurationInNanos
	requestStatus.MaxMemoryUsage = res.MaxMemoryUsageInBytes

	// 更新本次调用相关信息
	requestStatus.ResponseTime = requestStatus.ScheduleAcquireContainerLatency +
		requestStatus.FunctionExecutionDuration + requestStatus.ScheduleReturnContainerLatency
	data, _ := json.MarshalIndent(requestStatus, "", "    ")
	logger.Infof("\nrequest id: %s\n%s", res.RequestID, data)
	logger.Infof("%s request finish, function name: %s", res.RequestID, requestStatus.FunctionName)
	r.RequestMap.Remove(res.RequestID)

	functionStatus := requestStatus.functionStatus
	container := requestStatus.containerInfo
	nodeInfo := container.nodeInfo

	if res.MaxMemoryUsageInBytes > functionStatus.ComputeRequireMemory {
		functionStatus.ComputeRequireMemory = res.MaxMemoryUsageInBytes
	}

	atomic.AddInt64(&(container.AvailableMemInBytes), requestStatus.ActualRequireMemory)
	sendContainer(functionStatus, functionStatus.ComputeRequireMemory)

	containerMapObj, _ := functionStatus.NodeContainerMap.Get(nodeInfo.nodeNo)
	containerMap := containerMapObj.(*LockMap)

	container.requests.Remove(res.RequestID)
	nodeInfo.requests.Remove(res.RequestID)

	containerMap.Lock()
	if container.requests.Count() < 1 && container.sendTime < 1 {
		containerMap.Internal.Remove(container.containerNo)
		nodeInfo.containers.Remove(res.ContainerId)
		atomic.AddInt64(&(nodeInfo.availableMemInBytes), requestStatus.RequireMemory)
		go nodeInfo.RemoveContainer(context.Background(), &nsPb.RemoveContainerRequest{
			RequestId:   res.RequestID,
			ContainerId: container.ContainerId,
		})
		logger.Infof("%s RemoveContainer", functionStatus.FunctionName)
	}
	containerMap.Unlock()

	r.nodeMap.Lock()
	if nodeInfo.requests.Count() < 1 && nodeInfo.containers.Count() < 1 {
		r.nodeMap.Internal.Remove(nodeInfo.nodeNo)
		go r.rmClient.ReleaseNode(context.Background(), &rmPb.ReleaseNodeRequest{
			RequestId: res.RequestID,
			Id:        nodeInfo.nodeID,
		})
		logger.Infof("%s ReleaseNode", functionStatus.FunctionName)
	}
	r.nodeMap.Unlock()

}

func sendContainer(functionStatus *FunctionStatus, computeRequireMemory int64) {
	newComputeRequireMemory := computeRequireMemory * cp.SendComputeMemoryRatio
	if newComputeRequireMemory > functionStatus.RequireMemory {
		newComputeRequireMemory = functionStatus.RequireMemory
	}
	for i := 0; i < cp.SendContainerRatio; i++ {
		container := r.getAvailableContainer(functionStatus, computeRequireMemory)
		if container != nil {
			functionStatus.SendContainerChan <- container
			atomic.AddInt32(&(container.sendTime), 1)
		}
	}
}
