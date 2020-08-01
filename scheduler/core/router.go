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
	Internal cmap.ConcurrentMap
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
			Internal: cmap.New(),
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
	res = r.findAvailableContainer(containerMap, actualRequireMemory, req.RequestId)
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
		if functionStatus.ContainerMap.Internal.Count() < cp.MaxContainerNum { // 创建新容器
			// 获取一个node，有满足容器内存要求的node直接返回该node，否则申请一个新的node返回
			// 容器大小取多少？
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
					// 没有创建成功则删除
					node.requests.Remove(req.RequestId)
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
					containerMap.Internal.Set(res.ContainerId, res)
					logger.Infof("request id: %s, create container", req.RequestId)
				}
			}
		}
		if res == nil { // 等待空闲容器
			logger.Errorf("wait reason: %v", createContainerErr)
			now := time.Now().UnixNano()
			for {
				// todo use channel
				time.Sleep(200 * time.Millisecond)
				//<-functionStatus.functionReturned
				// 重新获取actualRequireMemory
				actualRequireMemory = functionStatus.MeanMaxMemoryUsage

				containerMap.Lock()
				res = r.findAvailableContainer(containerMap, actualRequireMemory, req.RequestId)

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

	nodeObj, ok := r.nodeMap.Internal.Get(res.nodeId)
	if !ok {
		data, _ := json.MarshalIndent(r.nodeMap.Internal, "", "    ")
		logger.Errorf("fail to get node info, %s\nnow node map\n:%s", res.nodeId, data)
	} else {
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

	r.nodeMap.Lock()
	node = r.findAvailableNode(memoryReq)

	if node != nil {
		// 根据创建的容器所需的内存减少当前节点的内存使用值, 先减，没创建成功再加回来
		node.availableMemInBytes -= req.FunctionConfig.MemoryInBytes
		// 存入request id 防止被误删
		node.requests.Set(req.RequestId, 1)
		r.nodeMap.Unlock()
		logger.Infof("rq id: %s, get exist node: %s", req.RequestId, node.nodeID)
		return node, nil
	} else {

		// 这里是否要加锁？
		// 可能当前读的时候是小于，但是其实有一个节点正在添加中了
		// 达到最大限制直接返回
		if r.nodeMap.Internal.Count() >= cp.MaxNodeNum {
			return nil, errors.Errorf("node maximum limit reached")
		}
		r.nodeMap.Unlock()

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

		// 不加锁可能出现两个同时写，之后读的时候本来有一个是可以保留的，但是读的是都是最新值导致都没法保留
		r.nodeMap.Lock()
		r.nodeMap.Internal.Set(nodeDesc.Id, node)
		data, _ := json.MarshalIndent(r.nodeMap.Internal, "", "    ")
		logger.Infof("node map %s", data)
		logger.Infof("ReserveNode id: %s", nodeDesc.Id)
		if r.nodeMap.Internal.Count() < cp.ReserveNodeNum {
			node.isReserved = true
		}
		node.requests.Set(req.RequestId, 1)
		r.nodeMap.Unlock()

		// 用node.GetStats重置可用内存
		//nodeGS, _ := node.GetStats(context.Background(), &nsPb.GetStatsRequest{})
		//node.availableMemInBytes = nodeGS.NodeStats.AvailableMemoryInBytes

		//申请新节点时，打印所有节点信息
		if cp.NeedLog {
			logger.Infof("get node: %s, all node info:", node.address)
			for _, key := range r.nodeMap.Internal.Keys() {
				nodeObj, _ := r.nodeMap.Internal.Get(key)
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
	containerMap := functionStatus.ContainerMap
	containerObj, ok := containerMap.Internal.Get(res.ContainerId)
	if !ok {
		return errors.Errorf("no container found with ContainerId %s", res.ContainerId)
	}
	container := containerObj.(*ContainerInfo)

	atomic.AddInt64(&(container.AvailableMemInBytes), requestStatus.ActualRequireMemory)
	container.requests.Remove(res.RequestID)

	//go func() {
	//	functionStatus.functionReturned <- struct{}{}
	//}()

	// RemoveContainer的时候一定要锁，防止要删除的container被使用
	// 释放容器判断， 使保留的容器都尽量在相同的节点上
	containerMap.Lock()
	if container.requests.Count() < 1 && functionStatus.ContainerMap.Internal.Count() > cp.ReserveContainerNum {
		nodeObj, ok := r.nodeMap.Internal.Get(container.nodeId)
		if !ok {
			data, _ := json.MarshalIndent(r.nodeMap.Internal, "", "    ")
			logger.Errorf("fail to get node info, %s\nnow node map\n:%s", container.nodeId, data)
		} else {
			node := nodeObj.(*NodeInfo)
			if !node.isReserved {
				node.requests.Remove(container.ContainerId)
				functionStatus.ContainerMap.Internal.Remove(container.ContainerId)
				// 不需要管返回值，反正请求完成了释放就行了
				go node.RemoveContainer(ctx, &nsPb.RemoveContainerRequest{
					RequestId:   res.RequestID,
					ContainerId: container.ContainerId,
				})
				logger.Infof("success to release container")
				// ReleaseNode的时候一定要锁，防止要删除的node被使用
				// 容器删除的时候才考虑释放node，因为可能需要预留容器
				r.nodeMap.Lock()
				if node.requests.Count() < 1 {
					r.nodeMap.Internal.Remove(node.nodeID)
					go r.rmClient.ReleaseNode(ctx, &rmPb.ReleaseNodeRequest{
						RequestId: res.RequestID,
						Id:        node.nodeID,
					})
					logger.Infof("success to release node, id: %s", node.nodeID)
				} else {
					node.availableMemInBytes += requestStatus.RequireMemory
				}
				r.nodeMap.Unlock()
			}
		}
	}
	containerMap.Unlock()

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
func (r *Router) findAvailableContainer(containerMap *LockMap, actualRequireMemory int64, requestId string) *ContainerInfo {
	var allNodeContainerBest *ContainerInfo
	var reservedNodeContainerBest *ContainerInfo
	for _, key := range containerMap.Internal.Keys() {
		containerObj, _ := containerMap.Internal.Get(key)
		container := containerObj.(*ContainerInfo)
		if container.AvailableMemInBytes > actualRequireMemory {
			if allNodeContainerBest == nil {
				allNodeContainerBest = container
			} else {
				if container.AvailableMemInBytes < allNodeContainerBest.AvailableMemInBytes {
					allNodeContainerBest = container
				}
			}
			nodeObj, ok := r.nodeMap.Internal.Get(container.nodeId)
			if !ok {
				data, _ := json.MarshalIndent(r.nodeMap.Internal, "", "    ")
				logger.Errorf("fail to get node info, %s\nnow node map\n:%s", container.nodeId, data)
			} else {
				node := nodeObj.(*NodeInfo)
				if node.isReserved {
					if reservedNodeContainerBest == nil {
						reservedNodeContainerBest = container
					} else {
						if container.AvailableMemInBytes < reservedNodeContainerBest.AvailableMemInBytes {
							reservedNodeContainerBest = container
						}
					}
				}
			}
		}
	}
	if reservedNodeContainerBest != nil {
		nodeObj, _ := r.nodeMap.Internal.Get(reservedNodeContainerBest.nodeId)
		node := nodeObj.(*NodeInfo)
		node.requests.Set(requestId, 1)
		return reservedNodeContainerBest
	} else {
		if allNodeContainerBest != nil {
			nodeObj, _ := r.nodeMap.Internal.Get(allNodeContainerBest.nodeId)
			node := nodeObj.(*NodeInfo)
			node.requests.Set(requestId, 1)
		}
		return allNodeContainerBest
	}
}

// 取满足要求情况下，资源最少的节点，以达到紧密排布, 优先取保留节点
func (r *Router) findAvailableNode(memoryReq int64) *NodeInfo {
	var allNodeBest *NodeInfo
	var reservedNodeBest *NodeInfo
	for _, key := range r.nodeMap.Internal.Keys() {
		nodeObj, _ := r.nodeMap.Internal.Get(key)
		nowNode := nodeObj.(*NodeInfo)
		if nowNode.availableMemInBytes > memoryReq {
			if allNodeBest == nil {
				allNodeBest = nowNode
			} else {
				if nowNode.availableMemInBytes < allNodeBest.availableMemInBytes {
					allNodeBest = nowNode
				}
			}
			if nowNode.isReserved {
				if reservedNodeBest == nil {
					reservedNodeBest = nowNode
				} else {
					if nowNode.availableMemInBytes < reservedNodeBest.availableMemInBytes {
						reservedNodeBest = nowNode
					}
				}
			}
		}
	}
	if reservedNodeBest != nil {
		return reservedNodeBest
	} else {
		return allNodeBest
	}
}
