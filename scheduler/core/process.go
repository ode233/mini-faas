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

	if res.MaxMemoryUsageInBytes > functionStatus.MaxMemoryUsageInBytes {
		functionStatus.MaxMemoryUsageInBytes = res.MaxMemoryUsageInBytes
	}

	var newComputeRequireMemory int64
	nowComputeRequireMemory := functionStatus.ComputeRequireMemory

	if functionStatus.BaseExecutionTime == 0 {
		functionStatus.BaseExecutionTime = res.DurationInNanos
	} else {
		if requestStatus.ActualRequireMemory == nowComputeRequireMemory {
			radio := float64(res.DurationInNanos) / float64(functionStatus.BaseExecutionTime)
			if radio <= cp.BaseTimeDecreaseRatio {
				functionStatus.BaseTimeDecreaseChangeNum += 1
				if functionStatus.BaseTimeDecreaseChangeNum >= cp.BaseTimeChangeNum {
					newComputeRequireMemory = nowComputeRequireMemory / 2
					if newComputeRequireMemory <= functionStatus.MaxMemoryUsageInBytes {
						newComputeRequireMemory = functionStatus.MaxMemoryUsageInBytes
					}
					atomic.StoreInt64(&(functionStatus.ComputeRequireMemory), newComputeRequireMemory)
					functionStatus.BaseTimeDecreaseChangeNum = 0
					logger.Infof("%s, ComputeRequireMemory decrease: %d", functionStatus.FunctionName, functionStatus.ComputeRequireMemory)
				}
			} else if radio >= cp.BaseTimeIncreaseRatio {
				functionStatus.BaseTimeIncreaseChangeNum += 1
				if functionStatus.BaseTimeIncreaseChangeNum >= cp.BaseTimeChangeNum {
					newComputeRequireMemory = int64(float64(nowComputeRequireMemory) * radio)
					if newComputeRequireMemory >= functionStatus.RequireMemory {
						newComputeRequireMemory = functionStatus.RequireMemory
					}
					atomic.StoreInt64(&(functionStatus.ComputeRequireMemory), newComputeRequireMemory)
					functionStatus.BaseTimeIncreaseChangeNum = 0
					logger.Infof("%s, ComputeRequireMemory increase: %d", functionStatus.FunctionName, functionStatus.ComputeRequireMemory)

				}
			}
		}
	}

	containerMapObj, _ := functionStatus.NodeContainerMap.Get(nodeInfo.nodeNo)
	containerMap := containerMapObj.(*LockMap)

	container.requests.Remove(res.RequestID)
	nodeInfo.requests.Remove(res.RequestID)

	atomic.AddInt64(&(container.AvailableMemInBytes), requestStatus.ActualRequireMemory)
	sendContainer(functionStatus, functionStatus.ComputeRequireMemory)

	containerMap.Lock()
	if container.requests.Count() < 1 && container.sendTime < 1 {
		containerMap.Internal.Remove(container.containerNo)
		nodeInfo.containers.Remove(res.ContainerId)
		atomic.AddInt64(&(nodeInfo.availableMemInBytes), requestStatus.RequireMemory)
		go nodeInfo.RemoveContainer(context.Background(), &nsPb.RemoveContainerRequest{
			RequestId:   res.RequestID,
			ContainerId: container.ContainerId,
		})
		logger.Infof("%s RemoveContainer, id: %s", functionStatus.FunctionName, container.ContainerId)
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
	for i := 0; i < cp.SendContainerRatio; i++ {
		//data, _ := json.MarshalIndent(functionStatus.NodeContainerMap, "", "    ")
		//logger.Infof("sendContainer\n%s",  data)
		container := r.getAvailableContainer(functionStatus, computeRequireMemory)
		if container != nil {
			atomic.AddInt32(&(container.sendTime), 1)
			logger.Infof("sendContainer id %s", container.ContainerId)
			functionStatus.SendContainerChan <- &SendContainerStruct{
				container:            container,
				computeRequireMemory: computeRequireMemory,
			}
		}
	}
}
