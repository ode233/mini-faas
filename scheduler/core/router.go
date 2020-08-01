package core

import (
	"aliyun/serverless/mini-faas/scheduler/utils/logger"
	"context"
	"encoding/json"
	uuid "github.com/satori/go.uuid"
	"sync"
	"sync/atomic"
	"time"

	nsPb "aliyun/serverless/mini-faas/nodeservice/proto"
	rmPb "aliyun/serverless/mini-faas/resourcemanager/proto"
	cp "aliyun/serverless/mini-faas/scheduler/config"
	"aliyun/serverless/mini-faas/scheduler/model"
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
}

type FunctionStatus struct {
	SuccessRequestNum  int64
	MeanMaxMemoryUsage int64
	functionReturned   chan struct{}
	ContainerMap       *LockMap // container_id -> ContainerInfo
}

type ContainerInfo struct {
	ContainerId         string // container_id
	address             string
	port                int64
	nodeId              string
	AvailableMemInBytes int64
	// 用ConcurrentMap读写锁可能会导致删除容器时重复删除
	requests cmap.ConcurrentMap // request_id -> status
}

type LockMap struct {
	sync.Mutex
	internal cmap.ConcurrentMap
}

type Router struct {
	nodeMap     *LockMap           // instance_id -> NodeInfo instance_id == nodeDesc.ContainerId == nodeId
	functionMap cmap.ConcurrentMap // function_name -> FunctionStatus
	RequestMap  cmap.ConcurrentMap // request_id -> RequestStatus
	rmClient    rmPb.ResourceManagerClient
}

var nowLogInterval = int64(0)

func NewRouter(config *cp.Config, rmClient rmPb.ResourceManagerClient) *Router {
	// 取结构体地址表示实例化
	return &Router{
		nodeMap: &LockMap{
			internal: cmap.New(),
		},
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
	// 可用于执行的container
	var res *ContainerInfo
	var isAttainLogInterval = false

	if cp.NeedLog {
		if nowLogInterval >= cp.LogPrintInterval {
			isAttainLogInterval = true
			atomic.StoreInt64(&nowLogInterval, 0)
		} else {
			atomic.AddInt64(&nowLogInterval, 1)
		}
	}

	// 取该函数相关信息
	r.functionMap.SetIfAbsent(req.FunctionName, &FunctionStatus{
		SuccessRequestNum:  0,
		MeanMaxMemoryUsage: 0,
		ContainerMap: &LockMap{
			internal: cmap.New(),
		},
	})
	fmObj, _ := r.functionMap.Get(req.FunctionName)
	// .是类型转换
	// containerMap存的是执行对应函数的容器
	functionStatus := fmObj.(*FunctionStatus)
	// 根据函数执行时的平均最大使用内存来判断实际所需内存
	actualRequireMemory := req.FunctionConfig.MemoryInBytes
	if functionStatus.MeanMaxMemoryUsage != 0 {
		// 加不加锁没啥影响
		actualRequireMemory = functionStatus.MeanMaxMemoryUsage
	}
	// 获取使用的容器信息
	containerMap := functionStatus.ContainerMap
	containerMap.Lock()
	res = r.findAvailableContainer(containerMap, actualRequireMemory)
	if res != nil {
		// 减少该容器可使用内存
		res.AvailableMemInBytes -= actualRequireMemory
		res.requests.Set(req.RequestId, 1)
		containerMap.Unlock()
		logger.Infof("request id: %s, use exist container", req.RequestId)
	} else { // 没有可用容器
		createContainerErr := errors.Errorf("attach MaxContainerNum")
		// 为什么写在条件里会死锁？
		containerMap.Unlock()
		// 这里是否要加锁？
		if len(functionStatus.ContainerMap.internal) < cp.MaxContainerNum { // 创建新容器
			//functionStatus.Unlock()
			// 获取一个node，有满足容器内存要求的node直接返回该node，否则申请一个新的node返回
			node, err := r.getNode(req.AccountId, req.FunctionConfig.MemoryInBytes, req)
			if err != nil {
				createContainerErr = err
			} else {
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
					atomic.AddInt64(&(node.availableMemInBytes), req.FunctionConfig.MemoryInBytes)
					r.handleContainerErr(node, req.FunctionConfig.MemoryInBytes)
					createContainerErr = errors.Wrapf(err, "failed to create container on %s", node.address)
				} else {
					res = &ContainerInfo{
						ContainerId:         replyC.ContainerId,
						address:             node.address,
						port:                node.port,
						nodeId:              node.nodeID,
						AvailableMemInBytes: req.FunctionConfig.MemoryInBytes - actualRequireMemory,
						requests:            cmap.New(),
					}
					// 新键的容器还没添加进containerMap所以不用锁
					res.requests.Set(req.RequestId, 1)
					containerMap.internal.Set(res.ContainerId, res)
					node.containers.Set(res.ContainerId, 1)
					logger.Infof("request id: %s, create container", req.RequestId)
				}
			}
		}
		if res == nil { // 等待空闲容器
			logger.Errorf("wait reason: %v", createContainerErr)
			//functionStatus.Unlock()
			now := time.Now().UnixNano()
			for {
				// todo use channel
				time.Sleep(200 * time.Millisecond)
				//<-functionStatus.functionReturned
				// 重新获取actualRequireMemory
				actualRequireMemory = functionStatus.MeanMaxMemoryUsage

				containerMap.Lock()
				res = r.findAvailableContainer(containerMap, actualRequireMemory)

				latency := time.Now().UnixNano() - now
				if res != nil {
					// 减少该容器可使用内存
					res.AvailableMemInBytes -= actualRequireMemory
					res.requests.Set(req.RequestId, 1)
					containerMap.Unlock()
					logger.Infof("request id: %s, wait %d to use available container", req.RequestId, latency/1e6)
					break
				}
				containerMap.Unlock()
				if latency > 30*time.Second.Nanoseconds() {
					return nil, createContainerErr
				}
			}
		}
	}

	nodeObj, _ := r.nodeMap.internal.Get(res.nodeId)
	node := nodeObj.(*NodeInfo)
	if isAttainLogInterval {
		// 打印节点信息
		now := time.Now().UnixNano()
		nodeGS, err := node.GetStats(ctx, &nsPb.GetStatsRequest{
			RequestId: req.RequestId,
		})
		latency := (time.Now().UnixNano() - now) / 1e6
		if err != nil {
			logger.Errorf("fail to get node status, latency: %d", latency)
		} else {
			data, _ := json.MarshalIndent(nodeGS, "", "    ")
			logger.Infof("\nnow_request_id: %s"+
				"\nget node status latency:%d"+
				"\nmy compute node AvailableMemInBytes:%d"+
				"\nnode %s status:"+
				"\n%s",
				req.RequestId, latency, node.availableMemInBytes, res.address, data)
			data, _ = json.MarshalIndent(r.functionMap, "", "    ")
			// 比较该节点上容器的实际可用内存值和我计算的可用内存值（我计算的值考虑了这次请求所需的内存）
			logger.Infof("\nnow function map: %s", data)
		}
	}

	requestStatus := &RequestStatus{
		FunctionName:        req.FunctionName,
		NodeAddress:         res.address,
		ContainerId:         res.ContainerId,
		RequireMemory:       req.FunctionConfig.MemoryInBytes,
		ActualRequireMemory: actualRequireMemory,
	}

	r.RequestMap.Set(req.RequestId, requestStatus)

	return &pb.AcquireContainerReply{
		NodeId:          res.nodeId,
		NodeAddress:     res.address,
		NodeServicePort: res.port,
		ContainerId:     res.ContainerId,
	}, nil
}

func (r *Router) getNode(accountId string, memoryReq int64, req *pb.AcquireContainerRequest) (*NodeInfo, error) {
	var node *NodeInfo
	// 取满足要求情况下，资源最少的节点，以达到紧密排布
	r.nodeMap.Lock()
	for _, key := range r.nodeMap.internal.Keys() {
		nodeObj, _ := r.nodeMap.internal.Get(key)
		nowNode := nodeObj.(*NodeInfo)
		if nowNode.availableMemInBytes > memoryReq {
			if node == nil {
				node = nowNode
			} else {
				if nowNode.availableMemInBytes < node.availableMemInBytes {
					node = nowNode
				}
			}
		}
	}
	if node != nil {
		// 根据创建的容器所需的内存减少当前节点的内存使用值, 先减，没创建成功再加回来
		node.availableMemInBytes -= req.FunctionConfig.MemoryInBytes
		r.nodeMap.Unlock()
		return node, nil
	} else {
		r.nodeMap.Unlock()

		// 这里是否要加锁？
		// 达到最大限制直接返回
		if len(r.nodeMap.internal) >= cp.MaxNodeNum {
			return nil, errors.Errorf("node maximum limit reached")
		}

		// 超时没有请求到节点就取消
		ctxR, cancelR := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelR()
		now := time.Now().UnixNano()
		replyRn, err := r.rmClient.ReserveNode(ctxR, &rmPb.ReserveNodeRequest{
			AccountId: accountId,
		})
		if err != nil {
			return nil, errors.Errorf("Failed to reserve node due to %v", err)
		}
		logger.Infof("ReserveNode,NodeAddress: %s, Latency: %d", replyRn.Node.Address, (time.Now().UnixNano()-now)/1e6)

		nodeDesc := replyRn.Node
		// 本地ReserveNode 返回的可用memory 比 node.GetStats少了一倍, 比赛环境正常
		node, err := NewNode(nodeDesc.Id, nodeDesc.Address, nodeDesc.NodeServicePort, nodeDesc.MemoryInBytes)
		logger.Infof("ReserveNode memory: %d", nodeDesc.MemoryInBytes)
		if err != nil {
			return nil, errors.Errorf("Failed to NewNode %v", err)
		}
		r.nodeMap.internal.Set(nodeDesc.Id, node)
		if len(r.nodeMap.internal) < cp.ReserveNodeNum {
			node.isReserved = true
		}

		// 用node.GetStats重置可用内存
		//nodeGS, _ := node.GetStats(context.Background(), &nsPb.GetStatsRequest{})
		//node.availableMemInBytes = nodeGS.NodeStats.AvailableMemoryInBytes

		//申请新节点时，打印所有节点信息
		if cp.NeedLog {
			logger.Infof("get node: %s, all node info:", node.address)
			for _, key := range r.nodeMap.internal.Keys() {
				nodeObj, _ := r.nodeMap.internal.Get(key)
				node := nodeObj.(*NodeInfo)
				nodeGS, err := node.GetStats(context.Background(), &nsPb.GetStatsRequest{})
				if err != nil {
					logger.Errorf("fail to get node status")
				} else {
					data, _ := json.MarshalIndent(nodeGS, "", "    ")
					logger.Infof("node %s status:\n%s", node.address, data)
				}
			}
		}

		return node, nil
	}
}

func (r *Router) handleContainerErr(node *NodeInfo, functionMem int64) {
}

func (r *Router) ReturnContainer(ctx context.Context, res *model.ResponseInfo) error {
	rmObj, ok := r.RequestMap.Get(res.RequestID)
	if !ok {
		return errors.Errorf("no request found with ContainerId %s", res.RequestID)
	}
	requestStatus := rmObj.(*RequestStatus)
	requestStatus.FunctionExecutionDuration = res.DurationInNanos
	requestStatus.MaxMemoryUsage = res.MaxMemoryUsageInBytes

	fmObj, ok := r.functionMap.Get(requestStatus.FunctionName)
	if !ok {
		return errors.Errorf("no container acquired for the request %s", res.RequestID)
	}
	functionStatus := fmObj.(*FunctionStatus)
	containerObj, ok := functionStatus.ContainerMap.internal.Get(res.ContainerId)
	container := containerObj.(*ContainerInfo)
	if !ok {
		return errors.Errorf("no container found with ContainerId %s", res.ContainerId)
	}

	atomic.AddInt64(&(container.AvailableMemInBytes), requestStatus.ActualRequireMemory)
	container.requests.Remove(res.RequestID)

	//go func() {
	//	functionStatus.functionReturned <- struct{}{}
	//}()

	// 需要锁防止是要将被删除的容器
	// 释放容器判断， 使保留的容器都尽量在相同的节点上
	if container.requests.Count() < 1 && len(functionStatus.ContainerMap.internal) > cp.ReserveContainerNum {
		nodeObj, _ := r.nodeMap.internal.Get(container.nodeId)
		node := nodeObj.(*NodeInfo)
		if !node.isReserved {
			node.containers.Remove(container.ContainerId)
			functionStatus.ContainerMap.internal.Remove(container.ContainerId)
			// 不需要管返回值，反正请求完成了释放就行了
			go node.RemoveContainer(ctx, &nsPb.RemoveContainerRequest{
				RequestId:   res.RequestID,
				ContainerId: container.ContainerId,
			})
			logger.Infof("success to release container")
			if node.containers.Count() < 1 {
				r.nodeMap.internal.Remove(node.nodeID)
				go r.rmClient.ReleaseNode(ctx, &rmPb.ReleaseNodeRequest{
					RequestId: res.RequestID,
					Id:        node.nodeID,
				})
				logger.Infof("success to release node")
			} else {
				node.availableMemInBytes += requestStatus.RequireMemory
			}
		}
	}

	// 计算meanMaxMemoryUsage
	if functionStatus.MeanMaxMemoryUsage == 0 {
		functionStatus.MeanMaxMemoryUsage = requestStatus.MaxMemoryUsage
	} else {
		//functionStatus.MeanMaxMemoryUsage =
		//	(functionStatus.MeanMaxMemoryUsage + requestStatus.MaxMemoryUsage/functionStatus.SuccessRequestNum) /
		//		(1 + 1/functionStatus.SuccessRequestNum)

		// 还是先保守取最大使用内存里的最大值
		if functionStatus.MeanMaxMemoryUsage < requestStatus.MaxMemoryUsage {
			functionStatus.MeanMaxMemoryUsage = requestStatus.MaxMemoryUsage
		}
	}

	//functionStatus.SuccessRequestNum += 1

	return nil
}

// 遍历查找满足要求情况下剩余资源最少的容器，以达到紧密排布, 优先选取保留节点
func (r *Router) findAvailableContainer(containerMap *LockMap, actualRequireMemory int64) *ContainerInfo {
	var allNodeContainer *ContainerInfo
	var reservedNodeContainer *ContainerInfo
	for _, key := range containerMap.internal.Keys() {
		containerObj, _ := containerMap.internal.Get(key)
		container := containerObj.(*ContainerInfo)
		if container.AvailableMemInBytes > actualRequireMemory {
			if allNodeContainer == nil {
				allNodeContainer = container
			} else {
				if container.AvailableMemInBytes < allNodeContainer.AvailableMemInBytes {
					allNodeContainer = container
				}
			}
			nodeObj, _ := r.nodeMap.internal.Get(container.nodeId)
			node := nodeObj.(*NodeInfo)
			if node.isReserved {
				if reservedNodeContainer == nil {
					reservedNodeContainer = container
				} else {
					if container.AvailableMemInBytes < reservedNodeContainer.AvailableMemInBytes {
						reservedNodeContainer = container
					}
				}
			}
		}
	}
	if reservedNodeContainer != nil {
		return reservedNodeContainer
	} else {
		return allNodeContainer
	}
}
