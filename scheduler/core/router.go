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
	actualRequireMemory int64

	functionStatus *FunctionStatus
	containerInfo  *ContainerInfo
}

type FunctionStatus struct {
	functionName          string
	containerTotalMemory  int64
	computeRequireMemory  int64
	maxMemoryUsageInBytes int64
	baseExecutionTime     int64
	tryDecreaseMemory     bool
	sendContainerChan     chan *SendContainerStruct
	nodeContainerMap      icmap.ConcurrentMap // nodeNo -> ContainerMap
}

type ContainerInfo struct {
	sync.Mutex
	containerId         string // container_id
	nodeInfo            *NodeInfo
	availableMemInBytes int64
	containerNo         int
}

type LockMap struct {
	sync.RWMutex
	internal icmap.ConcurrentMap
	num      int32
}

type SendContainerStruct struct {
	container            *ContainerInfo
	computeRequireMemory int64
}

type Router struct {
	nodeMap     *LockMap           // no -> NodeInfo instance_id == nodeDesc.ContainerId == nodeId
	functionMap cmap.ConcurrentMap // function_name -> FunctionStatus
	requestMap  cmap.ConcurrentMap // request_id -> RequestStatus
	rmClient    rmPb.ResourceManagerClient
}

func NewRouter(config *cp.Config, rmClient rmPb.ResourceManagerClient) *Router {
	// 取结构体地址表示实例化
	return &Router{
		nodeMap: &LockMap{
			internal: icmap.New(),
		},
		functionMap: cmap.New(),
		requestMap:  cmap.New(),
		rmClient:    rmClient,
	}
}

// 给结构体类型的引用添加方法，相当于添加实例方法，直接给结构体添加方法相当于静态方法
func (r *Router) Start() {
	// Just in case the router has internal loops.

	go func() {
		logger.Infof("ReserveNode in advance begin ")
		for {
			r.reserveNode(0)
			if r.nodeMap.num >= cp.MaxNodeNum || r.requestMap.Count() > 0 {
				logger.Infof("ReserveNode in advance finish")
				break
			}
		}
	}()
}

func (r *Router) AcquireContainer(ctx context.Context, req *pb.AcquireContainerRequest) (*pb.AcquireContainerReply, error) {
	// 可用于执行的container
	var res *ContainerInfo

	// 取该函数相关信息
	r.functionMap.SetIfAbsent(req.FunctionName, &FunctionStatus{
		functionName:         req.FunctionName,
		containerTotalMemory: req.FunctionConfig.MemoryInBytes,
		computeRequireMemory: req.FunctionConfig.MemoryInBytes,
		sendContainerChan:    make(chan *SendContainerStruct, 300),
		tryDecreaseMemory:    true,
		nodeContainerMap:     icmap.New(),
	})

	fmObj, _ := r.functionMap.Get(req.FunctionName)
	functionStatus := fmObj.(*FunctionStatus)

	var err error
	computeRequireMemory := functionStatus.computeRequireMemory

	// 还没有函数返回时的调用
	if functionStatus.maxMemoryUsageInBytes == 0 {
		res, err = r.createNewContainer(req, functionStatus, computeRequireMemory)
		if res == nil {
			sendContainerStruct := r.waitContainer(functionStatus, cp.LastWaitChannelTimeout)
			if sendContainerStruct != nil {
				res = sendContainerStruct.container
				computeRequireMemory = sendContainerStruct.computeRequireMemory
			}
		}
	} else { // 有函数返回时的调用
		sendContainerStruct := r.waitContainer(functionStatus, cp.FirstWaitChannelTimeout)
		if sendContainerStruct != nil {
			res = sendContainerStruct.container
			computeRequireMemory = sendContainerStruct.computeRequireMemory
		} else {
			res = r.getAvailableContainer(functionStatus, computeRequireMemory)
			if res == nil {
				res, err = r.createNewContainer(req, functionStatus, computeRequireMemory)
				if res == nil {
					sendContainerStruct := r.waitContainer(functionStatus, cp.LastWaitChannelTimeout)
					if sendContainerStruct != nil {
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
		actualRequireMemory: computeRequireMemory,

		functionStatus: functionStatus,
		containerInfo:  res,
	}

	r.requestMap.Set(req.RequestId, requestStatus)

	return &pb.AcquireContainerReply{
		NodeId:          res.nodeInfo.nodeID,
		NodeAddress:     res.nodeInfo.address,
		NodeServicePort: res.nodeInfo.port,
		ContainerId:     res.containerId,
	}, nil
}

func (r *Router) waitContainer(functionStatus *FunctionStatus, timeoutDuration time.Duration) *SendContainerStruct {
	timeout := time.NewTimer(timeoutDuration)
	select {
	case sendContainerStruct := <-functionStatus.sendContainerChan:
		return sendContainerStruct
	case <-timeout.C:
		return nil
	}
}

func (r *Router) getAvailableContainer(functionStatus *FunctionStatus, computeRequireMemory int64) *ContainerInfo {
	for _, i := range sortedKeys(functionStatus.nodeContainerMap.Keys()) {
		containerMapObj, ok := functionStatus.nodeContainerMap.Get(i)
		if ok {
			containerMap := containerMapObj.(*LockMap)
			for _, j := range sortedKeys(containerMap.internal.Keys()) {
				containerObj, ok := containerMap.internal.Get(j)
				if ok {
					container := containerObj.(*ContainerInfo)
					container.Lock()
					if container.availableMemInBytes >= computeRequireMemory {
						container.availableMemInBytes -= computeRequireMemory
						container.Unlock()
						return container
					}
					container.Unlock()
				}
			}
		}
	}
	return nil
}

func (r *Router) createNewContainer(req *pb.AcquireContainerRequest, functionStatus *FunctionStatus, actualRequireMemory int64) (*ContainerInfo, error) {
	var res *ContainerInfo
	var createContainerErr error
	// 获取一个node，有满足容器内存要求的node直接返回该node，否则申请一个新的node返回
	// 容器大小取多少？
	node, err := r.getNode(req)
	if node == nil {
		createContainerErr = err
	} else {
		// 在node上创建运行该函数的容器，并保存容器信息
		ctx, cancel := context.WithTimeout(context.Background(), cp.Timout)
		defer cancel()
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
		if replyC == nil {
			// 没有创建成功则删除
			node.Lock()
			node.availableMemInBytes += req.FunctionConfig.MemoryInBytes
			node.Unlock()
			createContainerErr = errors.Wrapf(err, "failed to create container")
		} else {
			functionStatus.nodeContainerMap.SetIfAbsent(node.nodeNo, &LockMap{
				num:      0,
				internal: icmap.New(),
			})
			nodeContainerMapObj, _ := functionStatus.nodeContainerMap.Get(node.nodeNo)
			nodeContainerMap := nodeContainerMapObj.(*LockMap)
			containerNo := int(atomic.AddInt32(&(nodeContainerMap.num), 1))
			res = &ContainerInfo{
				containerId:         replyC.ContainerId,
				nodeInfo:            node,
				containerNo:         containerNo,
				availableMemInBytes: req.FunctionConfig.MemoryInBytes - actualRequireMemory,
			}
			// 新键的容器还没添加进containerMap所以不用锁
			nodeContainerMap.internal.Set(containerNo, res)
		}
	}
	return res, createContainerErr
}

func (r *Router) getNode(req *pb.AcquireContainerRequest) (*NodeInfo, error) {
	var node *NodeInfo

	node = r.getAvailableNode(req)

	if node != nil {
		return node, nil
	} else {

		if r.nodeMap.internal.Count() >= cp.MaxNodeNum {
			return nil, errors.Errorf("node maximum limit reached")
		}

		var err error
		node, err = r.reserveNode(req.FunctionConfig.MemoryInBytes)

		return node, err
	}
}

func (r *Router) reserveNode(containerNeedMemory int64) (*NodeInfo, error) {
	// 超时没有请求到节点就取消
	ctxR, cancelR := context.WithTimeout(context.Background(), cp.Timout)
	defer cancelR()
	replyRn, err := r.rmClient.ReserveNode(ctxR, &rmPb.ReserveNodeRequest{})
	if err != nil {
		time.Sleep(100 * time.Millisecond)
		return nil, err
	}
	if replyRn == nil {
		time.Sleep(100 * time.Millisecond)
		return nil, err
	}

	nodeDesc := replyRn.Node
	nodeNo := int(atomic.AddInt32(&r.nodeMap.num, 1))
	// 本地ReserveNode 返回的可用memory 比 node.GetStats少了一倍, 比赛环境正常
	node, err := NewNode(nodeDesc.Id, nodeNo, nodeDesc.Address, nodeDesc.NodeServicePort, nodeDesc.MemoryInBytes)
	if err != nil {
		return nil, err
	}
	node.availableMemInBytes -= containerNeedMemory
	r.nodeMap.internal.Set(node.nodeNo, node)
	logger.Infof("ReserveNode")

	return node, nil
}

// 取满足要求情况下，资源最少的节点，以达到紧密排布, 优先取保留节点
func (r *Router) getAvailableNode(req *pb.AcquireContainerRequest) *NodeInfo {
	var res *NodeInfo
	for _, i := range sortedKeys(r.nodeMap.internal.Keys()) {
		nodeObj, ok := r.nodeMap.internal.Get(i)
		if ok {
			node := nodeObj.(*NodeInfo)
			node.Lock()
			if node.availableMemInBytes > req.FunctionConfig.MemoryInBytes {
				node.availableMemInBytes -= req.FunctionConfig.MemoryInBytes
				node.Unlock()
				res = node
				break
			}
			node.Unlock()
		}
	}
	return res
}

func (r *Router) ReturnContainer(ctx context.Context, res *model.ResponseInfo) error {
	go r.processReturnContainer(res)
	return nil
}

func (r *Router) processReturnContainer(res *model.ResponseInfo) {
	rmObj, ok := r.requestMap.Get(res.RequestID)
	if !ok {
		logger.Errorf("can not find RequestID in requestMap")
		return
	}
	requestStatus := rmObj.(*RequestStatus)
	r.requestMap.Remove(res.RequestID)

	// 更新本次调用相关信息

	functionStatus := requestStatus.functionStatus
	container := requestStatus.containerInfo

	r.computeRequireMemory(functionStatus, res, requestStatus)

	container.Lock()
	container.availableMemInBytes += requestStatus.actualRequireMemory
	container.Unlock()

	r.sendContainer(functionStatus, functionStatus.computeRequireMemory)

	r.tryReleaseResources(res, functionStatus, container)

}

//computeRequireMemory 计算所需内存并更新相关数据
func (r *Router) computeRequireMemory(functionStatus *FunctionStatus, res *model.ResponseInfo, requestStatus *RequestStatus) {
	if res.MaxMemoryUsageInBytes > functionStatus.maxMemoryUsageInBytes {
		functionStatus.maxMemoryUsageInBytes = res.MaxMemoryUsageInBytes
		if functionStatus.computeRequireMemory < res.MaxMemoryUsageInBytes {
			functionStatus.computeRequireMemory = res.MaxMemoryUsageInBytes
		}
	}

	if functionStatus.baseExecutionTime == 0 {
		functionStatus.baseExecutionTime = res.DurationInNanos
	}

	nowComputeRequireMemory := functionStatus.computeRequireMemory
	if requestStatus.actualRequireMemory == nowComputeRequireMemory {
		// 先考虑增加内存
		if nowComputeRequireMemory < functionStatus.containerTotalMemory {
			radio := float64(res.DurationInNanos) / float64(functionStatus.baseExecutionTime)
			if radio > cp.MaxBaseTimeRatio {
				doubleMemory := nowComputeRequireMemory * 2
				if doubleMemory < functionStatus.containerTotalMemory {
					functionStatus.computeRequireMemory = doubleMemory
				} else {
					functionStatus.computeRequireMemory = functionStatus.containerTotalMemory
				}
				functionStatus.tryDecreaseMemory = false
			}
		}
		// 再考虑减小内存
		if functionStatus.tryDecreaseMemory {
			halfMemory := nowComputeRequireMemory / 2
			if halfMemory > functionStatus.maxMemoryUsageInBytes {
				functionStatus.computeRequireMemory = halfMemory
			} else {
				functionStatus.tryDecreaseMemory = false
			}
		}
	}
}

func (r *Router) sendContainer(functionStatus *FunctionStatus, computeRequireMemory int64) {
	container := r.getAvailableContainer(functionStatus, computeRequireMemory)
	if container != nil {
		functionStatus.sendContainerChan <- &SendContainerStruct{
			container:            container,
			computeRequireMemory: computeRequireMemory,
		}
	}
}

func (r *Router) tryReleaseResources(res *model.ResponseInfo, functionStatus *FunctionStatus, container *ContainerInfo) {

	// release container
	if container.availableMemInBytes == functionStatus.containerTotalMemory {
		//logger.Infof("first check %s", container.containerId)
		time.Sleep(cp.ReleaseResourcesTimeout)
	} else {
		return
	}

	container.Lock()
	if container.availableMemInBytes == functionStatus.containerTotalMemory {
		container.availableMemInBytes = 0
		container.Unlock()

		node := container.nodeInfo
		containerMapObj, _ := functionStatus.nodeContainerMap.Get(node.nodeNo)
		containerMap := containerMapObj.(*LockMap)

		containerMap.internal.Remove(container.containerNo)
		go node.RemoveContainer(context.Background(), &nsPb.RemoveContainerRequest{
			RequestId:   res.RequestID,
			ContainerId: container.containerId,
		})
		logger.Infof("RemoveContainer")

		node.Lock()
		node.availableMemInBytes += functionStatus.containerTotalMemory
		node.Unlock()

		// release node
		//if node.availableMemInBytes == node.totalMemInBytes {
		//	time.Sleep(cp.ReleaseResourcesTimeout)
		//} else {
		//	return
		//}
		//
		//node.Lock()
		//if node.availableMemInBytes == node.totalMemInBytes {
		//	node.availableMemInBytes = 0
		//	node.Unlock()
		//	r.nodeMap.internal.Remove(node.nodeNo)
		//	go r.rmClient.ReleaseNode(context.Background(), &rmPb.ReleaseNodeRequest{
		//		RequestId: res.RequestID,
		//		Id:        node.nodeID,
		//	})
		//	logger.Infof("ReleaseNode")
		//} else {
		//	node.Unlock()
		//}

	} else {
		//logger.Infof("release double check fail %s", container.containerId)
		container.Unlock()
	}

}

func sortedKeys(keys []int) []int {
	sort.Ints(keys)
	return keys
}
