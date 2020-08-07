package core

import (
	"aliyun/serverless/mini-faas/scheduler/model"
	"aliyun/serverless/mini-faas/scheduler/utils/logger"
	"context"
	uuid "github.com/satori/go.uuid"
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

	FunctionTimeout int64

	functionStatus *FunctionStatus
	containerInfo  *ContainerInfo

	isFirstRound bool
}

type FunctionStatus struct {
	FirstRoundRequestNum     int64
	IsFirstRound             bool
	NeedReservedContainerNum int32
	FunctionNumPerContainer  int
	NowReservedContainerNum  int32
	RequireMemory            int64
	ComputeRequireMemory     int64
	ReturnContainerChan      chan *ContainerInfo
	NodeContainerMap         cmap.ConcurrentMap // nodeNo -> ContainerMap
}

type ContainerInfo struct {
	ContainerId         string // container_id
	nodeInfo            *NodeInfo
	AvailableMemInBytes int64
	isReserved          bool
	containerNo         string
	sendTime            int32
	// 用ConcurrentMap读写锁可能会导致删除容器时重复删除
	requests cmap.ConcurrentMap // request_id -> status
}

type LockMap struct {
	sync.Mutex
	Internal cmap.ConcurrentMap
	num      int64
}

type Router struct {
	nodeMap     *LockMap           // no -> NodeInfo instance_id == nodeDesc.ContainerId == nodeId
	functionMap cmap.ConcurrentMap // function_name -> FunctionStatus
	RequestMap  cmap.ConcurrentMap // request_id -> RequestStatus
	rmClient    rmPb.ResourceManagerClient
}

var ProcessChan chan *model.ResponseInfo

func NewRouter(config *cp.Config, rmClient rmPb.ResourceManagerClient) *Router {
	ProcessChan = make(chan *model.ResponseInfo, 1000)
	// 取结构体地址表示实例化
	return &Router{
		nodeMap: &LockMap{
			Internal: cmap.New(),
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
		IsFirstRound:  true,
		RequireMemory: req.FunctionConfig.MemoryInBytes,
		//ComputeRequireMemory:  0,
		ComputeRequireMemory: req.FunctionConfig.MemoryInBytes,
		ReturnContainerChan:  make(chan *ContainerInfo, 1000),
		NodeContainerMap:     cmap.New(),
	})
	fmObj, _ := r.functionMap.Get(req.FunctionName)
	functionStatus := fmObj.(*FunctionStatus)
	isFirstRound := functionStatus.IsFirstRound

	var actualRequireMemory int64

	var waitContainerErr error
	if isFirstRound {
		actualRequireMemory = functionStatus.RequireMemory
		atomic.AddInt64(&(functionStatus.FirstRoundRequestNum), 1)
		var err error
		res, err = r.createNewContainer(req, functionStatus, actualRequireMemory)
		waitContainerErr = errors.Wrapf(err, "fail to createNewContainer")
	} else {
		actualRequireMemory = functionStatus.ComputeRequireMemory
		timeout := time.NewTimer(cp.ChannelTimeout)
		select {
		case res = <-functionStatus.ReturnContainerChan:
			res.requests.Set(req.RequestId, 1)
			atomic.AddInt32(&(res.sendTime), -1)
			logger.Infof("res id: %s, use exist container", req.RequestId)
			break
		case <-timeout.C:
			waitContainerErr = errors.New("ReturnContainerChan timeout")
		}
	}

	if res == nil {
		logger.Warningf("wait reason %v", waitContainerErr)
		now := time.Now().UnixNano()
		var s string
		for {
			timeout := time.NewTimer(cp.Timout)
			select {
			case res = <-functionStatus.ReturnContainerChan:
				res.requests.Set(req.RequestId, 1)
				atomic.AddInt32(&(res.sendTime), -1)
				s = "use exist container"
				break
			case <-timeout.C:
				res, _ = r.createNewContainer(req, functionStatus, actualRequireMemory)
				if res != nil {
					s = "createNewContainer"
					break
				}
			}
			latency := time.Now().UnixNano() - now
			if res != nil {
				logger.Infof("res id: %s, %s second, latency: %d", req.RequestId, s, latency/1e6)
				break
			} else {
				if latency > cp.WaitChannelTimeout.Nanoseconds() {
					return nil, waitContainerErr
				}
			}
		}
	}

	requestStatus := &RequestStatus{
		FunctionName:        req.FunctionName,
		NodeAddress:         res.nodeInfo.address,
		ContainerId:         res.ContainerId,
		RequireMemory:       req.FunctionConfig.MemoryInBytes,
		ActualRequireMemory: actualRequireMemory,
		FunctionTimeout:     req.FunctionConfig.TimeoutInMs,

		containerInfo: res,
		isFirstRound:  isFirstRound,

		functionStatus: functionStatus,
	}

	r.RequestMap.Set(req.RequestId, requestStatus)

	return &pb.AcquireContainerReply{
		NodeId:          res.nodeInfo.nodeID,
		NodeAddress:     res.nodeInfo.address,
		NodeServicePort: res.nodeInfo.port,
		ContainerId:     res.ContainerId,
	}, nil
}

func (r *Router) createNewContainer(req *pb.AcquireContainerRequest, functionStatus *FunctionStatus, actualRequireMemory int64) (*ContainerInfo, error) {
	var res *ContainerInfo
	createContainerErr := errors.Errorf("")
	// 获取一个node，有满足容器内存要求的node直接返回该node，否则申请一个新的node返回
	// 容器大小取多少？
	node, err := r.getNode(actualRequireMemory, req)
	if err != nil {
		createContainerErr = err
	} else {
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
		logger.Infof("CreateContainer, Latency: %d", (time.Now().UnixNano()-now)/1e6)
		if err != nil {
			atomic.AddInt64(&(node.availableMemInBytes), actualRequireMemory)
			// 没有创建成功则删除
			node.requests.Remove(req.RequestId)
			createContainerErr = errors.Wrapf(err, "failed to create container on %s", node.address)
		} else {
			functionStatus.NodeContainerMap.SetIfAbsent(node.nodeNo, &LockMap{
				num:      0,
				Internal: cmap.New(),
			})
			nodeContainerMapObj, _ := functionStatus.NodeContainerMap.Get(node.nodeNo)
			nodeContainerMap := nodeContainerMapObj.(*LockMap)
			containerNo := string(atomic.AddInt64(&(nodeContainerMap.num), 1))
			res = &ContainerInfo{
				ContainerId:         replyC.ContainerId,
				nodeInfo:            node,
				isReserved:          true,
				containerNo:         containerNo,
				AvailableMemInBytes: req.FunctionConfig.MemoryInBytes - actualRequireMemory,
				requests:            cmap.New(),
			}
			// 新键的容器还没添加进containerMap所以不用锁
			res.requests.Set(req.RequestId, 1)
			nodeContainerMap.Internal.Set(containerNo, res)
			logger.Infof("request id: %s, create container", req.RequestId)
		}
	}
	return res, createContainerErr
}

func (r *Router) getNode(actualRequireMemory int64, req *pb.AcquireContainerRequest) (*NodeInfo, error) {
	var node *NodeInfo

	node = r.getAvailableNode(actualRequireMemory, req)

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

		// 超时没有请求到节点就取消
		ctxR, cancelR := context.WithTimeout(context.Background(), cp.Timout)
		defer cancelR()
		now := time.Now().UnixNano()
		replyRn, err := r.rmClient.ReserveNode(ctxR, &rmPb.ReserveNodeRequest{})
		latency := (time.Now().UnixNano() - now) / 1e6
		if err != nil {
			return nil, errors.Errorf("Failed to reserve node due to %v, Latency: %d", err, latency)
		}
		logger.Infof("ReserveNode,NodeAddress: %s, Latency: %d", replyRn.Node.Address, latency)

		nodeDesc := replyRn.Node
		nodeNo := string(atomic.AddInt64(&r.nodeMap.num, 1))
		// 本地ReserveNode 返回的可用memory 比 node.GetStats少了一倍, 比赛环境正常
		node, err := NewNode(nodeDesc.Id, nodeNo, nodeDesc.Address, nodeDesc.NodeServicePort, nodeDesc.MemoryInBytes)
		logger.Infof("ReserveNode memory: %d", nodeDesc.MemoryInBytes)
		if err != nil {
			return nil, errors.Errorf("Failed to NewNode %v", err)
		}

		//用node.GetStats重置可用内存
		//nodeGS, _ := node.GetStats(context.Background(), &nsPb.GetStatsRequest{})
		//node.availableMemInBytes = nodeGS.NodeStats.AvailableMemoryInBytes

		// 不加锁可能出现两个先写，之后读的时候本来有一个是可以保留的，但是读的是都是最新值导致都没法保留
		//r.nodeMap.Lock()
		atomic.AddInt64(&(node.availableMemInBytes), -actualRequireMemory)
		node.requests.Set(req.RequestId, 1)
		r.nodeMap.Internal.Set(nodeNo, node)
		logger.Infof("ReserveNode id: %s", nodeDesc.Id)
		//r.nodeMap.Unlock()

		//申请新节点时，打印所有节点信息
		return node, nil
	}
}

// 取满足要求情况下，资源最少的节点，以达到紧密排布, 优先取保留节点
func (r *Router) getAvailableNode(actualRequireMemory int64, req *pb.AcquireContainerRequest) *NodeInfo {
	var res *NodeInfo
	//r.nodeMap.Lock()

	nodeNum := atomic.LoadInt64(&r.nodeMap.num)
	for i := 1; i <= int(nodeNum); i++ {
		nodeObj, ok := r.nodeMap.Internal.Get(string(i))
		if ok {
			nowNode := nodeObj.(*NodeInfo)
			availableMemInBytes := atomic.LoadInt64(&(nowNode.availableMemInBytes))
			if availableMemInBytes > actualRequireMemory {
				res = nowNode
				// 根据创建的容器所需的内存减少当前节点的内存使用值, 先减，没创建成功再加回来
				atomic.AddInt64(&(res.availableMemInBytes), -actualRequireMemory)
				// 存入request id 防止被误删
				res.requests.Set(req.RequestId, 1)
				break
			}
		}
	}
	//r.nodeMap.Unlock()
	return res
}

func (r *Router) ReturnContainer(ctx context.Context, res *model.ResponseInfo) error {
	timeout := time.NewTimer(cp.ChannelTimeout)
	select {
	case ProcessChan <- res:
		break
	case <-timeout.C:
		logger.Warningf("ReturnContainer timeout")
		break
	}
	return nil
}
