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

	if atomic.LoadInt32(&(functionStatus.IsFirstRound)) == 1 {
		atomic.StoreInt32(&(functionStatus.IsFirstRound), 0)
		logger.Infof("FirstRoundRequestNum: %d", functionStatus.NeedCacheRequestNum)
		//nodeGS, _ := nodeInfo.GetStats(context.Background(), &nsPb.GetStatsRequest{})
		//data, _ := json.MarshalIndent(nodeGS, "", "    ")
		//logger.Infof("GetStatsRequest:\n%s", data)
	}

	functionStatus.Lock()
	ComputeRequireMemory(functionStatus, res.MaxMemoryUsageInBytes)
	if atomic.LoadInt64(&(functionStatus.ComputeRequireMemory)) == 0 {
		sendContainer(functionStatus, container)
	} else {
		atomic.AddInt64(&(container.AvailableMemInBytes), requestStatus.ActualRequireMemory)
		sendContainer(functionStatus, nil)
	}
	functionStatus.Unlock()

	//if functionStatus.ComputeRequireMemory < res.MaxMemoryUsageInBytes {
	//	functionStatus.ComputeRequireMemory = res.MaxMemoryUsageInBytes
	//}

	container.requests.Remove(res.RequestID)
	nodeInfo.requests.Remove(res.RequestID)

	//containerMapObj, _ := functionStatus.NodeContainerMap.Get(nodeInfo.nodeNo)
	//containerMap := containerMapObj.(*LockMap)
	//container.Lock()
	//if container.requests.Count() < 1 && atomic.LoadInt32(&(container.sendTime)) < 1 {
	//	containerMap.Internal.Remove(container.containerNo)
	//	nodeInfo.containers.Remove(res.ContainerId)
	//	atomic.AddInt64(&(nodeInfo.availableMemInBytes), requestStatus.RequireMemory)
	//	go nodeInfo.RemoveContainer(context.Background(), &nsPb.RemoveContainerRequest{
	//		RequestId:   res.RequestID,
	//		ContainerId: container.ContainerId,
	//	})
	//	logger.Infof("%s RemoveContainer", functionStatus.FunctionName)
	//}
	//container.Unlock()
	//if nodeInfo.requests.Count() < 1 && nodeInfo.containers.Count() < 1 {
	//	r.nodeMap.Internal.Remove(nodeInfo.nodeNo)
	//	go r.rmClient.ReleaseNode(context.Background(), &rmPb.ReleaseNodeRequest{
	//		RequestId: res.RequestID,
	//		Id:        nodeInfo.nodeID,
	//	})
	//	logger.Infof("%s ReleaseNode", functionStatus.FunctionName)
	//}

}

func sendContainer(functionStatus *FunctionStatus, container *ContainerInfo) {
	if container != nil {
		atomic.AddInt32(&(container.sendTime), 1)
		functionStatus.SendContainerChan <- &SendContainerStruct{
			container: container,
			memory:    functionStatus.RequireMemory,
		}
	} else {
		nodeNum := atomic.LoadInt32(&(r.nodeMap.num))
		for i := 1; i <= int(nodeNum); i++ {
			containerMapObj, ok := functionStatus.NodeContainerMap.Get(strconv.Itoa(i))
			if ok {
				containerMap := containerMapObj.(*LockMap)
				containerNum := atomic.LoadInt32(&(containerMap.num))
				for j := 1; j <= int(containerNum); j++ {
					nowContainerObj, ok := containerMap.Internal.Get(strconv.Itoa(j))
					if ok {
						nowContainer := nowContainerObj.(*ContainerInfo)
						nowContainer.Lock()
						computeRequireMemory := atomic.LoadInt64(&(functionStatus.ComputeRequireMemory))
						if computeRequireMemory == 0 {
							computeRequireMemory = functionStatus.RequireMemory
						}
						requireMemory := computeRequireMemory + cp.ReserveMemory
						if requireMemory > functionStatus.RequireMemory {
							requireMemory = functionStatus.RequireMemory
						}
						for atomic.LoadInt64(&(nowContainer.AvailableMemInBytes)) >= requireMemory {
							if len(functionStatus.SendContainerChan) <
								int(atomic.LoadInt32(&(functionStatus.NeedCacheRequestNum))) {

								atomic.AddInt32(&(nowContainer.sendTime), 1)
								atomic.AddInt64(&(nowContainer.AvailableMemInBytes), -computeRequireMemory)
								functionStatus.SendContainerChan <- &SendContainerStruct{
									container: nowContainer,
									memory:    computeRequireMemory,
								}
							} else {
								nowContainer.Unlock()
								return
							}
						}
						nowContainer.Unlock()
					}
				}
			}
		}
	}
}

func ComputeRequireMemory(functionStatus *FunctionStatus, nowRequestMaxMemory int64) {
	maxMemoryUsage := int64(0)
	maxCpuUsagePct := float64(0)
	for _, key := range functionStatus.NodeContainerMap.Keys() {
		nodeInfoObj, ok := r.nodeMap.Internal.Get(key)
		if !ok {
			continue
		}
		nodeInfo := nodeInfoObj.(*NodeInfo)
		nodeGS, _ := nodeInfo.GetStats(context.Background(), &nsPb.GetStatsRequest{})

		data, _ := json.MarshalIndent(nodeGS, "", "    ")
		logger.Infof("GetStatsRequest:\n%s", data)

		for _, val := range nodeGS.ContainerStatsList {
			containerObj, ok := functionStatus.ContainerIdList.Get(val.ContainerId)
			if ok {
				container := containerObj.(*ContainerInfo)
				num := container.requests.Count()
				if val.MemoryUsageInBytes == 0 || num == 0 {
					continue
				}
				memoryUsageInBytes := val.MemoryUsageInBytes / int64(num)
				if memoryUsageInBytes > maxMemoryUsage {
					maxMemoryUsage = memoryUsageInBytes
				}
				cpuUsagePct := val.CpuUsagePct / float64(num)
				if cpuUsagePct > maxCpuUsagePct {
					maxCpuUsagePct = cpuUsagePct
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
	if nowRequestMaxMemory > maxMemory {
		maxMemory = nowRequestMaxMemory
	}
	if maxMemory > atomic.LoadInt64(&(functionStatus.ComputeRequireMemory)) &&
		maxMemory <= functionStatus.RequireMemory {
		atomic.StoreInt64(&(functionStatus.ComputeRequireMemory), maxMemory)
	}
	logger.Infof("%s, maxMemoryUsage: %d, cpuNeedMemory: %d", functionStatus.FunctionName, maxMemoryUsage, cpuNeedMemory)
}
