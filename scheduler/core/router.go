package core

import (
	"aliyun/serverless/mini-faas/scheduler/utils/logger"
	"context"
	"encoding/json"
	"sort"
	"sync"
	"time"

	cmap "github.com/orcaman/concurrent-map"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"

	nsPb "aliyun/serverless/mini-faas/nodeservice/proto"
	rmPb "aliyun/serverless/mini-faas/resourcemanager/proto"
	cp "aliyun/serverless/mini-faas/scheduler/config"
	"aliyun/serverless/mini-faas/scheduler/model"
	pb "aliyun/serverless/mini-faas/scheduler/proto"
)

var mutex sync.Mutex
var NowLogInterval = 0

type RequestStatus struct {
	FunctionName string
	NodeAddress  string
	ContainerId  string
	// ms
	ScheduleAcquireContainerLatency int64
	ScheduleReturnContainerLatency  int64
	FunctionExecutionDuration       int64
	ResponseTime                    int64
	// bytes
	RequireMemory       int64
	MaxMemoryUsage      int64
	ActualRequireMemory int64

	// other info
	IsAttainLogInterval bool
}

type FunctionStatus struct {
	sync.Mutex
	successRequestNum  int64
	meanMaxMemoryUsage int64
	containerMap       cmap.ConcurrentMap
}

type ContainerInfo struct {
	sync.Mutex
	id       string
	address  string
	port     int64
	nodeId   string
	requests map[string]int64 // request_id -> status
}

type Router struct {
	nodeMap     cmap.ConcurrentMap // instance_id -> NodeInfo instance_id == nodeDesc.Id == nodeId
	functionMap cmap.ConcurrentMap // function_name -> ContainerMap (container_id -> ContainerInfo)
	RequestMap  cmap.ConcurrentMap // request_id -> RequestStatus
	rmClient    rmPb.ResourceManagerClient
}

func NewRouter(config *cp.Config, rmClient rmPb.ResourceManagerClient) *Router {
	// 取结构体地址表示实例化
	return &Router{
		nodeMap:     cmap.New(),
		functionMap: cmap.New(),
		RequestMap:  cmap.New(),
		rmClient:    rmClient,
	}
}

// 给结构体类型的引用添加方法，相当于添加实例方法，直接给结构体添加方法相当于静态方法
func (r *Router) Start() {
	// Just in case the router has internal loops.
}

func (r *Router) AcquireContainer(ctx context.Context, req *pb.AcquireContainerRequest) (*pb.AcquireContainerReply, error) {
	// 临时变量，判断是否有空闲的container
	var res *ContainerInfo
	var isAttainLogInterval = false

	if cp.NeedLog {
		mutex.Lock()
		if NowLogInterval == cp.LogPrintInterval {
			isAttainLogInterval = true
			NowLogInterval = 0
		} else {
			NowLogInterval += 1
		}
		mutex.Unlock()
	}

	// 取函数对应的容器信息
	r.functionMap.SetIfAbsent(req.FunctionName, &FunctionStatus{
		successRequestNum:  0,
		meanMaxMemoryUsage: 0,
		containerMap:       cmap.New(),
	})
	fmObj, _ := r.functionMap.Get(req.FunctionName)
	// .是类型转换
	// containerMap存的是执行对应函数的容器
	functionStatus := fmObj.(*FunctionStatus)
	containerMap := functionStatus.containerMap
	// 遍历查看是否有所在节点资源满足的容器
	for _, key := range sortedKeys(containerMap.Keys()) {
		cmObj, _ := containerMap.Get(key)
		container := cmObj.(*ContainerInfo)
		container.Lock()
		nodeInfoObj, _ := r.nodeMap.Get(container.nodeId)
		nodeInfo := nodeInfoObj.(*NodeInfo)
		if req.FunctionConfig.MemoryInBytes < nodeInfo.availableMemInBytes {
			container.requests[req.RequestId] = 1
			res = container
			container.Unlock()
			// 是否还需要选一个最优节点，还是只要内存够就行
			break
		}
		container.Unlock()
	}

	if res == nil { // if no idle container exists
		// 获取一个node，有满足内存要求的node直接返回该node，否则申请一个新的node返回
		node, err := r.getNode(req.AccountId, req.FunctionConfig.MemoryInBytes)
		if err != nil {
			return nil, err
		}

		// 在node上创建运行该函数的容器，并保存容器信息
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
		if err != nil {
			r.handleContainerErr(node, req.FunctionConfig.MemoryInBytes)
			return nil, errors.Wrapf(err, "failed to create container on %s", node.address)
		}
		res = &ContainerInfo{
			id:       replyC.ContainerId,
			address:  node.address,
			port:     node.port,
			nodeId:   node.nodeID,
			requests: make(map[string]int64),
		}
		res.requests[req.RequestId] = 1 // The container hasn't been listed in the containerMap. So we don't need locking here.
		containerMap.Set(res.id, res)
	}

	// 根据平均最大使用内存或要求内存来减少当前节点的内存使用值
	actualRequireMemory := req.FunctionConfig.MemoryInBytes
	nmObj, _ := r.nodeMap.Get(res.nodeId)
	node := nmObj.(*NodeInfo)
	node.Lock()
	if functionStatus.meanMaxMemoryUsage != 0 {
		node.availableMemInBytes -= functionStatus.meanMaxMemoryUsage
		actualRequireMemory = functionStatus.meanMaxMemoryUsage
	} else {
		node.availableMemInBytes -= req.FunctionConfig.MemoryInBytes
	}
	node.Unlock()

	if isAttainLogInterval {
		// 打印节点信息
		now := time.Now().UnixNano()
		nodeGS, _ := node.GetStats(ctx, &nsPb.GetStatsRequest{
			RequestId: req.RequestId,
		})
		latency := (time.Now().UnixNano() - now) / 1e6
		data, _ := json.MarshalIndent(nodeGS, "", "    ")
		logger.Infof("\nnow_request_id: %s\nnode %s status:\nlatency:%d\n%s",
			req.RequestId, res.address, latency, data)
	}

	requestStatus := &RequestStatus{
		FunctionName:        req.FunctionName,
		NodeAddress:         res.address,
		ContainerId:         res.id,
		RequireMemory:       req.FunctionConfig.MemoryInBytes,
		ActualRequireMemory: actualRequireMemory,
		IsAttainLogInterval: isAttainLogInterval,
	}

	r.RequestMap.Set(req.RequestId, requestStatus)

	return &pb.AcquireContainerReply{
		NodeId:          res.nodeId,
		NodeAddress:     res.address,
		NodeServicePort: res.port,
		ContainerId:     res.id,
	}, nil
}

func (r *Router) getNode(accountId string, memoryReq int64) (*NodeInfo, error) {
	for _, key := range sortedKeys(r.nodeMap.Keys()) {
		nmObj, _ := r.nodeMap.Get(key)
		node := nmObj.(*NodeInfo)
		if node.availableMemInBytes > memoryReq {
			return node, nil
		}
	}

	// 需要申请新节点时，打印其他所有节点信息
	logger.Infof("now other node info:")
	for _, key := range sortedKeys(r.nodeMap.Keys()) {
		nmObj, _ := r.nodeMap.Get(key)
		node := nmObj.(*NodeInfo)
		nodeGS, _ := node.GetStats(context.Background(), &nsPb.GetStatsRequest{})
		data, _ := json.MarshalIndent(nodeGS, "", "    ")
		logger.Infof("node %s status:\n%s", node.address, data)
	}

	// 达到最大限制直接返回
	if r.nodeMap.Count() >= cp.MaxNodeNum {
		return nil, errors.New("node maximum limit reached")
	}

	// 30s之内没有请求到节点就取消
	ctxR, cancelR := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelR()
	now := time.Now().UnixNano()
	replyRn, err := r.rmClient.ReserveNode(ctxR, &rmPb.ReserveNodeRequest{
		AccountId: accountId,
	})
	if err != nil {
		logger.WithFields(logger.Fields{
			"Operation": "ReserveNode",
			"Latency":   (time.Now().UnixNano() - now) / 1e6,
			"Error":     true,
		}).Errorf("Failed to reserve node due to %v", err)
		return nil, errors.WithStack(err)
	}
	logger.WithFields(logger.Fields{
		"NodeAddress": replyRn.Node.Address,
		"Operation":   "ReserveNode",
		"Latency":     (time.Now().UnixNano() - now) / 1e6,
	}).Infof("")

	nodeDesc := replyRn.Node
	node, err := NewNode(nodeDesc.Id, nodeDesc.Address, nodeDesc.NodeServicePort, nodeDesc.MemoryInBytes)
	if err != nil {
		// TODO: Release the Node
		return nil, err
	}
	r.nodeMap.Set(nodeDesc.Id, node)
	return node, nil
}

func (r *Router) handleContainerErr(node *NodeInfo, functionMem int64) {
	node.Lock()
	node.availableMemInBytes += functionMem
	node.Unlock()
}

func (r *Router) ReturnContainer(ctx context.Context, res *model.ResponseInfo) error {
	rmObj, ok := r.RequestMap.Get(res.ID)
	if !ok {
		return errors.Errorf("no request found with id %s", res.ID)
	}
	requestStatus := rmObj.(*RequestStatus)
	requestStatus.FunctionExecutionDuration = res.DurationInNanos
	requestStatus.MaxMemoryUsage = res.MaxMemoryUsageInBytes

	fmObj, ok := r.functionMap.Get(requestStatus.FunctionName)
	if !ok {
		return errors.Errorf("no container acquired for the request %s", res.ID)
	}
	functionStatus := fmObj.(*FunctionStatus)
	cmObj, ok := functionStatus.containerMap.Get(res.ContainerId)
	if !ok {
		return errors.Errorf("no container found with id %s", res.ContainerId)
	}
	container := cmObj.(*ContainerInfo)
	container.Lock()
	delete(container.requests, res.ID)
	container.Unlock()

	// 计算meanMaxMemoryUsage
	functionStatus.Lock()
	if functionStatus.meanMaxMemoryUsage == 0 {
		functionStatus.meanMaxMemoryUsage = requestStatus.MaxMemoryUsage
	} else {
		functionStatus.meanMaxMemoryUsage =
			(functionStatus.meanMaxMemoryUsage + requestStatus.MaxMemoryUsage/functionStatus.successRequestNum) /
				(1 + 1/functionStatus.successRequestNum)
	}
	functionStatus.successRequestNum += 1
	functionStatus.Unlock()

	nodeInfoObj, _ := r.nodeMap.Get(container.nodeId)
	nodeInfo := nodeInfoObj.(*NodeInfo)
	nodeInfo.Lock()
	nodeInfo.availableMemInBytes += requestStatus.ActualRequireMemory
	nodeInfo.Unlock()

	return nil
}

func sortedKeys(keys []string) []string {
	sort.Strings(keys)
	return keys
}
