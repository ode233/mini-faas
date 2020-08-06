package core

import (
	nsPb "aliyun/serverless/mini-faas/nodeservice/proto"
	rmPb "aliyun/serverless/mini-faas/resourcemanager/proto"
	cp "aliyun/serverless/mini-faas/scheduler/config"
	"aliyun/serverless/mini-faas/scheduler/model"
	"aliyun/serverless/mini-faas/scheduler/utils/logger"
	"context"
	"encoding/json"
	"math"
	"sync/atomic"
)

var r *Router

func Process(router *Router) {
	r = router
	for {
		processChanStruct := <-ProcessChan
		go processReturnContainer(processChanStruct)
	}
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

	functionStatus := requestStatus.functionStatus
	container := requestStatus.containerInfo
	nodeInfo := container.nodeInfo

	if functionStatus.IsFirstRound {
		firstRoundRequestNum := atomic.LoadInt64(&(functionStatus.FirstRoundRequestNum))
		functionStatus.NeedReservedContainerNum = int32(math.Ceil((float64(res.MaxMemoryUsageInBytes*firstRoundRequestNum) / float64(functionStatus.RequireMemory)) * cp.ReserveContainerNumRadio))
		functionStatus.FunctionNumPerContainer = int(functionStatus.RequireMemory / res.MaxMemoryUsageInBytes)
		functionStatus.MeanMaxMemoryUsage = res.MaxMemoryUsageInBytes
		functionStatus.IsFirstRound = false
		logger.Infof("NeedReservedContainerNum: %d, FunctionNumPerContainer: %d, FirstRoundRequestNum: %d, MeanMaxMemoryUsage: %d  ",
			functionStatus.NeedReservedContainerNum, functionStatus.FunctionNumPerContainer, firstRoundRequestNum, res.MaxMemoryUsageInBytes)
	}

	container.requests.Remove(res.RequestID)
	nodeInfo.requests.Remove(res.RequestID)
	atomic.AddInt64(&(container.AvailableMemInBytes), requestStatus.ActualRequireMemory)

	// 更新本次调用相关信息
	requestStatus.ResponseTime = requestStatus.ScheduleAcquireContainerLatency +
		requestStatus.FunctionExecutionDuration + requestStatus.ScheduleReturnContainerLatency
	data, _ := json.MarshalIndent(requestStatus, "", "    ")
	logger.Infof("\nrequest id: %s\n%s", res.RequestID, data)
	logger.Infof("%s request finish, function name: %s", res.RequestID, requestStatus.FunctionName)
	r.RequestMap.Remove(res.RequestID)

	if requestStatus.isFirstRound {
		if functionStatus.NowReservedContainerNum < functionStatus.NeedReservedContainerNum {
			atomic.AddInt32(&(functionStatus.NowReservedContainerNum), 1)
			// 暂不考虑重新创建
			container.isReserved = true
			for i := 0; i < functionStatus.FunctionNumPerContainer; i++ {
				functionStatus.ReturnContainerChan <- container
			}
			return
		}
	}

	// todo 尽量使用reserved container
	if container.isReserved {
		functionStatus.ReturnContainerChan <- container
	} else {
		reserveContainerCache := int(math.Ceil(float64(functionStatus.NeedReservedContainerNum*int32(functionStatus.FunctionNumPerContainer)) * cp.ReserveContainerCacheRadio))
		if len(functionStatus.ReturnContainerChan) < reserveContainerCache {
			if requestStatus.isFirstRound {
				for i := 0; i < (functionStatus.FunctionNumPerContainer); i++ {
					functionStatus.ReturnContainerChan <- container
				}
			} else {
				functionStatus.ReturnContainerChan <- container
			}
		} else {
			if container.requests.Count() < 1 {
				functionStatus.OtherContainerMap.Internal.Remove(container.containerNo)
				go nodeInfo.RemoveContainer(context.Background(), &nsPb.RemoveContainerRequest{
					RequestId:   res.RequestID,
					ContainerId: container.ContainerId,
				})
				logger.Infof("RemoveContainer")
				if nodeInfo.requests.Count() < 1 {
					r.nodeMap.Internal.Remove(nodeInfo.nodeNo)
					go r.rmClient.ReleaseNode(context.Background(), &rmPb.ReleaseNodeRequest{
						RequestId: res.RequestID,
						Id:        nodeInfo.nodeID,
					})
					logger.Infof("ReleaseNode")
				} else {
					atomic.AddInt64(&(nodeInfo.availableMemInBytes), functionStatus.RequireMemory)
				}
			} else {
				if requestStatus.isFirstRound {
					for i := 0; i < functionStatus.FunctionNumPerContainer; i++ {
						functionStatus.ReturnContainerChan <- container
					}
				} else {
					functionStatus.ReturnContainerChan <- container
				}
			}
		}
	}

}
