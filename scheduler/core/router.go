package core

import (
	"aliyun/serverless/mini-faas/scheduler/model"
	"aliyun/serverless/mini-faas/scheduler/utils/icmap"
	"aliyun/serverless/mini-faas/scheduler/utils/logger"
	"context"
	uuid "github.com/satori/go.uuid"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	nsPb "aliyun/serverless/mini-faas/nodeservice/proto"
	rmPb "aliyun/serverless/mini-faas/resourcemanager/proto"
	cp "aliyun/serverless/mini-faas/scheduler/config"
	pb "aliyun/serverless/mini-faas/scheduler/proto"
	cmap "github.com/orcaman/concurrent-map"
	"github.com/pkg/errors"
)

type RequestStatus struct {
	// bytes
	ActualRequireMemory int64

	functionStatus *FunctionStatus
	containerInfo  *ContainerInfo
}

type FunctionStatus struct {
	sync.Mutex
	FunctionName              string
	ContainerTotalMemory      int64
	ComputeRequireMemory      int64
	MaxMemoryUsageInBytes     int64
	BaseTimeIncreaseChangeNum int32
	BaseExecutionTime         int64
	TryDecreaseMemory         bool
	SendContainerChan         chan *SendContainerStruct
	NodeContainerMap          icmap.ConcurrentMap // nodeNo -> ContainerMap
}

type ContainerInfo struct {
	sync.Mutex
	ContainerId         string // container_id
	nodeInfo            *NodeInfo
	AvailableMemInBytes int64
	containerNo         int
}

type LockMap struct {
	sync.Mutex
	Internal icmap.ConcurrentMap
	num      int32
}

type SendContainerStruct struct {
	container            *ContainerInfo
	computeRequireMemory int64
}

type Router struct {
	nodeMap     *LockMap           // no -> NodeInfo instance_id == nodeDesc.ContainerId == nodeId
	functionMap cmap.ConcurrentMap // function_name -> FunctionStatus
	RequestMap  cmap.ConcurrentMap // request_id -> RequestStatus
	rmClient    rmPb.ResourceManagerClient
}

func NewRouter(config *cp.Config, rmClient rmPb.ResourceManagerClient) *Router {
	// 取结构体地址表示实例化
	return &Router{
		nodeMap: &LockMap{
			Internal: icmap.New(),
		},
		functionMap: cmap.New(),
		RequestMap:  cmap.New(),
		rmClient:    rmClient,
	}
}

// 给结构体类型的引用添加方法，相当于添加实例方法，直接给结构体添加方法相当于静态方法
func (r *Router) Start() {
	// Just in case the router has Internal loops.
}

func (r *Router) AcquireContainer(ctx context.Context, req *pb.AcquireContainerRequest) (*pb.AcquireContainerReply, error) {
	// 可用于执行的container
	var res *ContainerInfo

	// 取该函数相关信息
	r.functionMap.SetIfAbsent(req.FunctionName, &FunctionStatus{
		FunctionName:         req.FunctionName,
		ContainerTotalMemory: req.FunctionConfig.MemoryInBytes,
		ComputeRequireMemory: req.FunctionConfig.MemoryInBytes,
		SendContainerChan:    make(chan *SendContainerStruct, 300),
		TryDecreaseMemory:    true,
		NodeContainerMap:     icmap.New(),
	})

	fmObj, _ := r.functionMap.Get(req.FunctionName)
	functionStatus := fmObj.(*FunctionStatus)

	var computeRequireMemory int64

	var err error
	computeRequireMemory = functionStatus.ComputeRequireMemory

	// 还没有函数返回时的调用
	if functionStatus.MaxMemoryUsageInBytes == 0 {
		res, err = r.createNewContainer(req, functionStatus, computeRequireMemory)
		if res == nil {
			logger.Warningf("first round createNewContainer error: %v", err)
			sendContainerStruct := r.waitContainer(functionStatus, cp.WaitChannelTimeout)
			if sendContainerStruct != nil {
				logger.Infof("res id: %s, first round wait to use exist container", req.RequestId)
				res = sendContainerStruct.container
				computeRequireMemory = sendContainerStruct.computeRequireMemory
			}
		}
	} else { // 有函数返回时的调用
		sendContainerStruct := r.waitContainer(functionStatus, cp.ChannelTimeout)
		if sendContainerStruct != nil {
			logger.Infof("res id: %s, second round use exist container", req.RequestId)
			res = sendContainerStruct.container
			computeRequireMemory = sendContainerStruct.computeRequireMemory
		} else {
			logger.Infof("second round wait, timeout")
			res = r.getAvailableContainer(functionStatus, computeRequireMemory)
			if res == nil {
				logger.Infof("second round getAvailableContainer fail")
				res, err = r.createNewContainer(req, functionStatus, computeRequireMemory)
				if res == nil {
					logger.Warningf("second round createNewContainer error: %v", err)
					sendContainerStruct := r.waitContainer(functionStatus, cp.WaitChannelTimeout)
					if sendContainerStruct != nil {
						logger.Infof("res id: %s, second round wait to use exist container", req.RequestId)
						res = sendContainerStruct.container
						computeRequireMemory = sendContainerStruct.computeRequireMemory
					}
				}
			}
		}
	}

	if res == nil {
		return nil, err
	}

	requestStatus := &RequestStatus{
		ActualRequireMemory: computeRequireMemory,

		functionStatus: functionStatus,
		containerInfo:  res,
	}

	r.RequestMap.Set(req.RequestId, requestStatus)

	return &pb.AcquireContainerReply{
		NodeId:          res.nodeInfo.nodeID,
		NodeAddress:     res.nodeInfo.address,
		NodeServicePort: res.nodeInfo.port,
		ContainerId:     res.ContainerId,
	}, nil
}

func (r *Router) waitContainer(functionStatus *FunctionStatus, timeoutDuration time.Duration) *SendContainerStruct {
	timeout := time.NewTimer(timeoutDuration)
	now := time.Now().UnixNano()
	select {
	case sendContainerStruct := <-functionStatus.SendContainerChan:
		logger.Warningf("latency %d ", (time.Now().UnixNano()-now)/1e6)
		return sendContainerStruct
	case <-timeout.C:
		return nil
	}
}

func (r *Router) getAvailableContainer(functionStatus *FunctionStatus, computeRequireMemory int64) *ContainerInfo {
	for _, i := range sortedKeys(functionStatus.NodeContainerMap.Keys()) {
		containerMapObj, ok := functionStatus.NodeContainerMap.Get(i)
		if ok {
			containerMap := containerMapObj.(*LockMap)
			//containerMap.Lock()
			for _, j := range sortedKeys(containerMap.Internal.Keys()) {
				nowContainerObj, ok := containerMap.Internal.Get(j)
				if ok {
					nowContainer := nowContainerObj.(*ContainerInfo)
					if atomic.LoadInt64(&(nowContainer.AvailableMemInBytes)) >= computeRequireMemory {
						atomic.AddInt64(&(nowContainer.AvailableMemInBytes), -computeRequireMemory)
						//containerMap.Unlock()
						return nowContainer
					}
				}
			}
			//containerMap.Unlock()
		}
	}
	return nil
}

func (r *Router) createNewContainer(req *pb.AcquireContainerRequest, functionStatus *FunctionStatus, actualRequireMemory int64) (*ContainerInfo, error) {
	var res *ContainerInfo
	createContainerErr := errors.Errorf("")
	// 获取一个node，有满足容器内存要求的node直接返回该node，否则申请一个新的node返回
	// 容器大小取多少？
	node, err := r.getNode(req)
	if node == nil {
		createContainerErr = err
	} else {
		atomic.AddInt64(&(node.availableMemInBytes), -req.FunctionConfig.MemoryInBytes)

		// 在node上创建运行该函数的容器，并保存容器信息
		ctx, cancel := context.WithTimeout(context.Background(), cp.Timout)
		defer cancel()
		now := time.Now().UnixNano()
		replyC, err := node.CreateContainer(ctx, &nsPb.CreateContainerRequest{
			Name: req.FunctionName + uuid.NewV4().String(),
			FunctionMeta: &nsPb.FunctionMeta{
				FunctionName:  req.FunctionName,
				Handler:       req.FunctionConfig.Handler,
				TimeoutInMs:   req.FunctionConfig.TimeoutInMs,
				MemoryInBytes: req.FunctionConfig.MemoryInBytes,
			},
			RequestId: req.RequestId,
		})
		logger.Infof("%s CreateContainer, Latency: %d", functionStatus.FunctionName, (time.Now().UnixNano()-now)/1e6)
		if replyC == nil {
			// 没有创建成功则删除
			atomic.AddInt64(&(node.availableMemInBytes), req.FunctionConfig.MemoryInBytes)
			createContainerErr = errors.Wrapf(err, "failed to create container on %s", node.address)
		} else {
			functionStatus.NodeContainerMap.SetIfAbsent(node.nodeNo, &LockMap{
				num:      0,
				Internal: icmap.New(),
			})
			nodeContainerMapObj, _ := functionStatus.NodeContainerMap.Get(node.nodeNo)
			nodeContainerMap := nodeContainerMapObj.(*LockMap)
			containerNo := int(atomic.AddInt32(&(nodeContainerMap.num), 1))
			res = &ContainerInfo{
				ContainerId:         replyC.ContainerId,
				nodeInfo:            node,
				containerNo:         containerNo,
				AvailableMemInBytes: req.FunctionConfig.MemoryInBytes - actualRequireMemory,
			}
			// 新键的容器还没添加进containerMap所以不用锁
			nodeContainerMap.Internal.Set(containerNo, res)
			logger.Infof("request id: %s, create container", req.RequestId)
		}
	}
	return res, createContainerErr
}

func (r *Router) getNode(req *pb.AcquireContainerRequest) (*NodeInfo, error) {
	var node *NodeInfo

	node = r.getAvailableNode(req)

	if node != nil {
		logger.Infof("rq id: %s, get exist node: %s", req.RequestId, node.nodeID)
		return node, nil
	} else {

		// 这里是否要加锁？
		// 可能当前读的时候是小于，但是其实有一个节点正在添加中了
		// 达到最大限制直接返回
		if r.nodeMap.Internal.Count() >= cp.MaxNodeNum {
			return nil, errors.Errorf("node maximum limit reached")
		}

		var err error
		node, err = r.reserveNode()

		return node, err
	}
}

func (r *Router) reserveNode() (*NodeInfo, error) {
	// 超时没有请求到节点就取消
	ctxR, cancelR := context.WithTimeout(context.Background(), cp.Timout)
	defer cancelR()
	now := time.Now().UnixNano()
	replyRn, err := r.rmClient.ReserveNode(ctxR, &rmPb.ReserveNodeRequest{})
	latency := (time.Now().UnixNano() - now) / 1e6
	if err != nil {
		logger.Errorf("Failed to reserve node due to %v, Latency: %d", err, latency)
		time.Sleep(100 * time.Millisecond)
		return nil, err
	}
	if replyRn == nil {
		time.Sleep(100 * time.Millisecond)
		return nil, err
	}
	logger.Infof("ReserveNode,NodeAddress: %s, Latency: %d", replyRn.Node.Address, latency)

	nodeDesc := replyRn.Node
	nodeNo := int(atomic.AddInt32(&r.nodeMap.num, 1))
	// 本地ReserveNode 返回的可用memory 比 node.GetStats少了一倍, 比赛环境正常
	node, err := NewNode(nodeDesc.Id, nodeNo, nodeDesc.Address, nodeDesc.NodeServicePort, nodeDesc.MemoryInBytes)
	logger.Infof("ReserveNode memory: %d, nodeNo: %s", nodeDesc.MemoryInBytes, nodeNo)
	if err != nil {
		logger.Errorf("Failed to NewNode %v", err)
		return nil, err
	}

	r.nodeMap.Internal.Set(node.nodeNo, node)
	logger.Infof("ReserveNode id: %s", nodeDesc.Id)

	return node, nil
}

// 取满足要求情况下，资源最少的节点，以达到紧密排布, 优先取保留节点
func (r *Router) getAvailableNode(req *pb.AcquireContainerRequest) *NodeInfo {
	var node *NodeInfo
	//r.nodeMap.Lock()

	for _, i := range sortedKeys(r.nodeMap.Internal.Keys()) {
		nodeObj, ok := r.nodeMap.Internal.Get(i)
		if ok {
			nowNode := nodeObj.(*NodeInfo)
			availableMemInBytes := nowNode.availableMemInBytes
			if availableMemInBytes > req.FunctionConfig.MemoryInBytes {
				node = nowNode
				break
			}
		}
	}
	//r.nodeMap.Unlock()
	return node
}

func (r *Router) ReturnContainer(ctx context.Context, res *model.ResponseInfo) error {
	go r.processReturnContainer(res)
	return nil
}

func (r *Router) processReturnContainer(res *model.ResponseInfo) {
	rmObj, ok := r.RequestMap.Get(res.RequestID)
	if !ok {
		logger.Errorf("no request found with ContainerId %s", res.RequestID)
		return
	}
	requestStatus := rmObj.(*RequestStatus)

	// 更新本次调用相关信息

	functionStatus := requestStatus.functionStatus
	container := requestStatus.containerInfo

	r.computeRequireMemory(functionStatus, res, requestStatus)

	atomic.AddInt64(&(container.AvailableMemInBytes), requestStatus.ActualRequireMemory)
	r.sendContainer(functionStatus, functionStatus.ComputeRequireMemory)

	r.tryReleaseResources(res, functionStatus, container)

	r.RequestMap.Remove(res.RequestID)

}

//computeRequireMemory 计算所需内存并更新相关数据
func (r *Router) computeRequireMemory(functionStatus *FunctionStatus, res *model.ResponseInfo, requestStatus *RequestStatus) {
	if res.MaxMemoryUsageInBytes > functionStatus.MaxMemoryUsageInBytes {
		if functionStatus.ComputeRequireMemory == functionStatus.MaxMemoryUsageInBytes {
			functionStatus.ComputeRequireMemory = res.MaxMemoryUsageInBytes
		}
		functionStatus.MaxMemoryUsageInBytes = res.MaxMemoryUsageInBytes
	}

	if requestStatus.ActualRequireMemory >= functionStatus.ContainerTotalMemory {
		if functionStatus.BaseExecutionTime < res.DurationInNanos {
			functionStatus.BaseExecutionTime = res.DurationInNanos
		}
	}

	nowComputeRequireMemory := atomic.LoadInt64(&(functionStatus.ComputeRequireMemory))
	if functionStatus.TryDecreaseMemory && requestStatus.ActualRequireMemory == nowComputeRequireMemory {
		var newComputeRequireMemory int64
		// 先考虑增加内存
		if nowComputeRequireMemory < functionStatus.ContainerTotalMemory {
			radio := float64(res.DurationInNanos) / float64(functionStatus.BaseExecutionTime)
			if radio > cp.MaxBaseTimeRatio {
				functionStatus.BaseTimeIncreaseChangeNum += 1
				if functionStatus.BaseTimeIncreaseChangeNum >= cp.BaseTimeChangeNum {
					newComputeRequireMemory = nowComputeRequireMemory * 2
					functionStatus.BaseTimeIncreaseChangeNum = 0
					functionStatus.TryDecreaseMemory = false
				}
			}
		}
		// 再考虑减小内存
		if functionStatus.TryDecreaseMemory {
			newComputeRequireMemory = nowComputeRequireMemory / 2
			if newComputeRequireMemory < functionStatus.MaxMemoryUsageInBytes {
				newComputeRequireMemory = functionStatus.MaxMemoryUsageInBytes
			}
		}

		atomic.StoreInt64(&(functionStatus.ComputeRequireMemory), newComputeRequireMemory)
	}
}

func (r *Router) sendContainer(functionStatus *FunctionStatus, computeRequireMemory int64) {
	//data, _ := json.MarshalIndent(functionStatus.NodeContainerMap, "", "    ")
	//logger.Infof("sendContainer\n%s",  data)
	container := r.getAvailableContainer(functionStatus, computeRequireMemory)
	if container != nil {
		logger.Infof("sendContainer id %s", container.ContainerId)
		functionStatus.SendContainerChan <- &SendContainerStruct{
			container:            container,
			computeRequireMemory: computeRequireMemory,
		}
	}
}

func (r *Router) tryReleaseResources(res *model.ResponseInfo, functionStatus *FunctionStatus, container *ContainerInfo) {
	if container.AvailableMemInBytes == functionStatus.ContainerTotalMemory {
		for i := 0; i < 10; i++ {
			time.Sleep(cp.ReleaseResourcesTimeout)
			if container.AvailableMemInBytes < functionStatus.ContainerTotalMemory {
				return
			}
		}
		nodeInfo := container.nodeInfo
		containerMapObj, _ := functionStatus.NodeContainerMap.Get(nodeInfo.nodeNo)
		containerMap := containerMapObj.(*LockMap)
		//containerMap.Lock()
		containerMap.Internal.Remove(container.containerNo)
		atomic.AddInt64(&(nodeInfo.availableMemInBytes), functionStatus.ContainerTotalMemory)
		go nodeInfo.RemoveContainer(context.Background(), &nsPb.RemoveContainerRequest{
			RequestId:   res.RequestID,
			ContainerId: container.ContainerId,
		})
		logger.Infof("%s RemoveContainer, id: %s", functionStatus.FunctionName, container.ContainerId)
		//containerMap.Unlock()

		if nodeInfo.availableMemInBytes == nodeInfo.totalMemInBytes {
			for i := 0; i < 10; i++ {
				time.Sleep(cp.ReleaseResourcesTimeout)
				if nodeInfo.availableMemInBytes < nodeInfo.totalMemInBytes {
					return
				}
			}
			//r.nodeMap.Lock()
			r.nodeMap.Internal.Remove(nodeInfo.nodeNo)
			go r.rmClient.ReleaseNode(context.Background(), &rmPb.ReleaseNodeRequest{
				RequestId: res.RequestID,
				Id:        nodeInfo.nodeID,
			})
			logger.Infof("%s ReleaseNode", functionStatus.FunctionName)
			//r.nodeMap.Unlock()
		}
	}
}

func sortedKeys(keys []int) []int {
	sort.Ints(keys)
	return keys
}

func reverseSortedKeys(keys []int) []int {
	sort.Sort(sort.Reverse(sort.IntSlice(keys)))
	return keys
}
