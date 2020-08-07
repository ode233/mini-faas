package core

import (
	nsPb "aliyun/serverless/mini-faas/nodeservice/proto"
	rmPb "aliyun/serverless/mini-faas/resourcemanager/proto"
	cp "aliyun/serverless/mini-faas/scheduler/config"
	"aliyun/serverless/mini-faas/scheduler/model"
	"aliyun/serverless/mini-faas/scheduler/utils/logger"
	"context"
	"encoding/json"
	"strconv"
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
				for _, key := range r.nodeMap.Internal.Keys() {
					logger.Infof("reserveNode nodeNo %v", key)
				}
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
	nodeNo := strconv.Itoa(int(atomic.AddInt32(&r.nodeMap.num, 1)))
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
		logger.Infof("FirstRoundRequestNum: %d", functionStatus.NeedCacheRequestNum)
		nodeGS, _ := nodeInfo.GetStats(context.Background(), &nsPb.GetStatsRequest{})
		data, _ := json.MarshalIndent(nodeGS, "", "    ")
		logger.Infof("GetStatsRequest:\n%s", data)
		go ComputeRequireMemory(functionStatus)
	}

	//if functionStatus.ComputeRequireMemory < res.MaxMemoryUsageInBytes {
	//	functionStatus.ComputeRequireMemory = res.MaxMemoryUsageInBytes
	//}

	container.requests.Remove(res.RequestID)
	nodeInfo.requests.Remove(res.RequestID)
	atomic.AddInt64(&(container.AvailableMemInBytes), requestStatus.ActualRequireMemory)
	atomic.AddInt64(&(nodeInfo.availableMemInBytes), requestStatus.ActualRequireMemory)

	sendContainer(functionStatus, container, nodeInfo)

	containerMapObj, _ := functionStatus.NodeContainerMap.Get(nodeInfo.nodeNo)
	containerMap := containerMapObj.(*LockMap)
	if container.requests.Count() < 1 && atomic.LoadInt32(&(container.sendTime)) < 1 {
		containerMap.Internal.Remove(container.containerNo)
		go nodeInfo.RemoveContainer(context.Background(), &nsPb.RemoveContainerRequest{
			RequestId:   res.RequestID,
			ContainerId: container.ContainerId,
		})
		logger.Infof("RemoveContainer")
	}
	if nodeInfo.requests.Count() < 1 && nodeInfo.containers.Count() < 1 {
		r.nodeMap.Internal.Remove(nodeInfo.nodeNo)
		go r.rmClient.ReleaseNode(context.Background(), &rmPb.ReleaseNodeRequest{
			RequestId: res.RequestID,
			Id:        nodeInfo.nodeID,
		})
		logger.Infof("ReleaseNode")
	}

}

func sendContainer(functionStatus *FunctionStatus, container *ContainerInfo, nodeInfo *NodeInfo) {
	computeRequireMemory := functionStatus.ComputeRequireMemory
	if computeRequireMemory == 0 {
		functionStatus.ReturnContainerChan <- container
		atomic.AddInt64(&(container.AvailableMemInBytes), -functionStatus.RequireMemory)
		atomic.AddInt64(&(nodeInfo.availableMemInBytes), -functionStatus.RequireMemory)
		atomic.AddInt32(&(container.sendTime), 1)
	} else {
		needReservedNum := int(functionStatus.NeedCacheRequestNum) - len(functionStatus.ReturnContainerChan)
		logger.Infof("needReservedNum: %d", needReservedNum)
		if needReservedNum > 0 {
			nodeNum := atomic.LoadInt32(&(r.nodeMap.num))
			for i := 1; i <= int(nodeNum); i++ {
				containerMapObj, ok := functionStatus.NodeContainerMap.Get(strconv.Itoa(i))
				nowNodeObj, ok2 := r.nodeMap.Internal.Get(strconv.Itoa(i))
				if ok && ok2 {
					logger.Infof("node no: %d", i)
					nowNode := nowNodeObj.(*NodeInfo)
					containerMap := containerMapObj.(*LockMap)
					containerNum := atomic.LoadInt32(&(containerMap.num))
					for j := 1; j <= int(containerNum); j++ {
						nowContainerObj, ok := containerMap.Internal.Get(strconv.Itoa(j))
						if ok {
							logger.Infof("container no: %d", j)
							nowContainer := nowContainerObj.(*ContainerInfo)
							if nowContainer.AvailableMemInBytes >= computeRequireMemory {
								num := nowContainer.AvailableMemInBytes / computeRequireMemory
								for i := 0; i < int(num); i++ {
									logger.Infof("sendContainer")
									functionStatus.ReturnContainerChan <- nowContainer
									atomic.AddInt64(&(nowContainer.AvailableMemInBytes), -computeRequireMemory)
									atomic.AddInt64(&(nowNode.availableMemInBytes), -computeRequireMemory)
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
}

func ComputeRequireMemory(functionStatus *FunctionStatus) {
	maxMemoryUsage := int64(0)
	maxCpuUsagePct := float64(0)
	logger.Infof("begin ComputeRequireMemory")
	for _, key := range functionStatus.NodeContainerMap.Keys() {
		logger.Infof("ComputeRequireMemory nodeNo")
		logger.Infof("ComputeRequireMemory nodeNo %s", key)
		nodeInfoObj, ok := r.nodeMap.Internal.Get(key)
		if !ok {
			for _, key1 := range r.nodeMap.Internal.Keys() {
				logger.Infof("nodeMap nodeNo %s", key1)
				if key1 == key {
					logger.Infof("true")
				}
			}
			logger.Infof("not ok ComputeRequireMemory nodeNo %s", key)
			continue
		}
		nodeInfo := nodeInfoObj.(*NodeInfo)
		nodeGS, _ := nodeInfo.GetStats(context.Background(), &nsPb.GetStatsRequest{})

		for _, val := range nodeGS.ContainerStatsList {
			_, ok := functionStatus.ContainerIdList.Get(val.ContainerId)
			if ok {
				if val.MemoryUsageInBytes > maxMemoryUsage {
					maxMemoryUsage = val.MemoryUsageInBytes
				}
				if val.CpuUsagePct > maxCpuUsagePct {
					maxCpuUsagePct = val.CpuUsagePct
				}
			}
		}
	}
	cpuNeedMemory := int64(maxCpuUsagePct * 0.01 * cp.MemoryPerCpu)
	var maxMemory int64
	if cpuNeedMemory > maxMemoryUsage {
		maxMemory = cpuNeedMemory
	} else {
		maxMemory = maxMemoryUsage
	}
	functionStatus.ComputeRequireMemory = maxMemory
	logger.Infof("maxMemoryUsage: %d, cpuNeedMemory: %d", maxMemoryUsage, cpuNeedMemory)
}
