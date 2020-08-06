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
	"time"
)

var r *Router

func Process(router *Router) {
	r = router
	go func() {
		for {
			reserveNode()
			if r.nodeMap.num >= cp.MaxNodeNum {
				break
			}
		}
	}()
	for {
		processChanStruct := <-ProcessChan
		go processReturnContainer(processChanStruct)
	}
}

func reserveNode() {
	// 超时没有请求到节点就取消
	ctxR, cancelR := context.WithTimeout(context.Background(), cp.Timout)
	defer cancelR()
	now := time.Now().UnixNano()
	replyRn, err := r.rmClient.ReserveNode(ctxR, &rmPb.ReserveNodeRequest{})
	latency := (time.Now().UnixNano() - now) / 1e6
	if err != nil {
		logger.Errorf("Failed to reserve node due to %v, Latency: %d", err, latency)
		time.Sleep(100 * time.Millisecond)
		return
	}
	if replyRn == nil {
		time.Sleep(100 * time.Millisecond)
		return
	}
	logger.Infof("ReserveNode,NodeAddress: %s, Latency: %d", replyRn.Node.Address, latency)

	nodeDesc := replyRn.Node
	nodeNo := string(atomic.AddInt64(&r.nodeMap.num, 1))
	// 本地ReserveNode 返回的可用memory 比 node.GetStats少了一倍, 比赛环境正常
	node, err := NewNode(nodeDesc.Id, nodeNo, nodeDesc.Address, nodeDesc.NodeServicePort, nodeDesc.MemoryInBytes)
	logger.Infof("ReserveNode memory: %d, nodeNo: %s", nodeDesc.MemoryInBytes, nodeNo)
	if err != nil {
		logger.Errorf("Failed to NewNode %v", err)
	}

	//用node.GetStats重置可用内存
	//nodeGS, _ := node.GetStats(context.Background(), &nsPb.GetStatsRequest{})
	//node.availableMemInBytes = nodeGS.NodeStats.AvailableMemoryInBytes

	r.nodeMap.Internal.Set(nodeNo, node)
	logger.Infof("ReserveNode id: %s", nodeDesc.Id)
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

	if functionStatus.IsFirstRound {
		functionStatus.IsFirstRound = false
		logger.Infof("FirstRoundRequestNum: %d", functionStatus.FirstRoundRequestNum)
	}

	if functionStatus.MeanMaxMemoryUsage < res.MaxMemoryUsageInBytes {
		functionStatus.MeanMaxMemoryUsage = res.MaxMemoryUsageInBytes
	}

	container.requests.Remove(res.RequestID)
	nodeInfo.requests.Remove(res.RequestID)
	atomic.AddInt64(&(container.AvailableMemInBytes), requestStatus.ActualRequireMemory)

	sendContainer(functionStatus)

	containerMapObj, ok := functionStatus.NodeContainerMap.Get(nodeInfo.nodeNo)
	if ok {
		containerMap := containerMapObj.(*LockMap)
		if container.requests.Count() < 1 && atomic.LoadInt32(&(container.sendTime)) < 1 {
			containerMap.Internal.Remove(container.containerNo)
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
		}
	}

}

func sendContainer(functionStatus *FunctionStatus) {
	needReservedNum := int(functionStatus.FirstRoundRequestNum) - len(functionStatus.ReturnContainerChan)
	logger.Infof("needReservedNum: %d", needReservedNum)
	meanMaxMemoryUsage := atomic.LoadInt64(&(functionStatus.MeanMaxMemoryUsage))
	if needReservedNum > 0 {
		for i := 1; i <= int(r.nodeMap.num); i++ {
			containerMapObj, ok := functionStatus.NodeContainerMap.Get(string(i))
			if ok {
				logger.Infof("node no: %d", i)
				containerMap := containerMapObj.(*LockMap)
				for j := 1; j <= int(containerMap.num); j++ {
					nowContainerObj, ok := containerMap.Internal.Get(string(j))
					if ok {
						logger.Infof("container no: %d", j)
						nowContainer := nowContainerObj.(*ContainerInfo)
						if nowContainer.AvailableMemInBytes > meanMaxMemoryUsage {
							num := nowContainer.AvailableMemInBytes / meanMaxMemoryUsage
							for i := 0; i < int(num); i++ {
								logger.Infof("sendContainer")
								functionStatus.ReturnContainerChan <- nowContainer
								atomic.AddInt64(&(nowContainer.AvailableMemInBytes), -meanMaxMemoryUsage)
								needReservedNum -= 1
								atomic.AddInt32(&(nowContainer.sendTime), 1)
								if needReservedNum < 1 {
									return
								}
							}
						}
					}
				}
			}
		}
	}
}
