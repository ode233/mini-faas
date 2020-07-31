package core

import (
	"aliyun/serverless/mini-faas/scheduler/utils/logger"
	"context"
	"encoding/json"
	uuid "github.com/satori/go.uuid"
	"sync"
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
	sync.Mutex
	SuccessRequestNum  int64
	MeanMaxMemoryUsage int64
	functionReturned   chan struct{}
	ContainerMap       map[string]*ContainerInfo // container_id -> ContainerInfo
}

type ContainerInfo struct {
	ContainerId         string // container_id
	address             string
	port                int64
	nodeId              string
	AvailableMemInBytes int64
	requests            map[string]int // request_id -> status
}

type NodeMap struct {
	sync.Mutex
	internal map[string]*NodeInfo
}

type Router struct {
	nodeMap     *NodeMap           // instance_id -> NodeInfo instance_id == nodeDesc.ContainerId == nodeId
	functionMap cmap.ConcurrentMap // function_name -> FunctionStatus
	RequestMap  cmap.ConcurrentMap // request_id -> RequestStatus
	rmClient    rmPb.ResourceManagerClient
}

var logLock sync.Mutex
var nowLogInterval = 0
var reserveNode = make(map[string]int)

func NewRouter(config *cp.Config, rmClient rmPb.ResourceManagerClient) *Router {
	// 取结构体地址表示实例化
	return &Router{
		nodeMap: &NodeMap{
			internal: map[string]*NodeInfo{},
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
		logLock.Lock()
		if nowLogInterval == cp.LogPrintInterval {
			isAttainLogInterval = true
			nowLogInterval = 0
		} else {
			nowLogInterval += 1
		}
		logLock.Unlock()
	}

	// 取该函数相关信息
	r.functionMap.SetIfAbsent(req.FunctionName, &FunctionStatus{
		SuccessRequestNum:  0,
		MeanMaxMemoryUsage: 0,
		ContainerMap:       map[string]*ContainerInfo{},
	})
	fmObj, _ := r.functionMap.Get(req.FunctionName)
	// .是类型转换
	// containerMap存的是执行对应函数的容器
	functionStatus := fmObj.(*FunctionStatus)
	functionStatus.Lock()
	// 根据函数执行时的平均最大使用内存来判断实际所需内存
	actualRequireMemory := req.FunctionConfig.MemoryInBytes
	if functionStatus.MeanMaxMemoryUsage != 0 {
		actualRequireMemory = functionStatus.MeanMaxMemoryUsage
	}
	// 获取使用的容器信息
	containerMap := functionStatus.ContainerMap
	res = findAvailableContainer(containerMap, actualRequireMemory)
	if res != nil {
		// 减少该容器可使用内存
		res.AvailableMemInBytes -= actualRequireMemory
		res.requests[req.RequestId] = 1
		functionStatus.Unlock()
		logger.Infof("request id: %s, use exist container", req.RequestId)
	} else { // 没有可用容器
		var createContainerErr error
		// 为什么写在条件里会死锁？
		functionStatus.Unlock()
		if len(functionStatus.ContainerMap) < cp.MaxContainerNum { // 创建新容器
			//functionStatus.Unlock()
			r.nodeMap.Lock()
			// 获取一个node，有满足容器内存要求的node直接返回该node，否则申请一个新的node返回
			node, err := r.getNode(req.AccountId, req.FunctionConfig.MemoryInBytes, req.RequestId)
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
					r.handleContainerErr(node, req.FunctionConfig.MemoryInBytes)
					createContainerErr = errors.Wrapf(err, "failed to create container on %s", node.address)
				} else {
					res = &ContainerInfo{
						ContainerId:         replyC.ContainerId,
						address:             node.address,
						port:                node.port,
						nodeId:              node.nodeID,
						AvailableMemInBytes: req.FunctionConfig.MemoryInBytes - actualRequireMemory,
						requests:            map[string]int{},
					}
					// 新键的容器还没添加进containerMap所以不用锁
					res.requests[req.RequestId] = 1
					node.containers[res.ContainerId] = 1
					containerMap[res.ContainerId] = res

					// 根据创建的容器所需的内存减少当前节点的内存使用值
					node.availableMemInBytes -= req.FunctionConfig.MemoryInBytes
					logger.Infof("request id: %s, create container", req.RequestId)
				}
			}
			r.nodeMap.Unlock()
		}
		if res == nil { // 等待空闲容器
			//functionStatus.Unlock()
			now := time.Now().UnixNano()
			for {
				// todo use channel
				time.Sleep(200 * time.Millisecond)
				//<-functionStatus.functionReturned
				functionStatus.Lock()
				// 重新获取actualRequireMemory
				actualRequireMemory = functionStatus.MeanMaxMemoryUsage
				res = findAvailableContainer(containerMap, actualRequireMemory)
				latency := time.Now().UnixNano() - now
				if res != nil {
					// 减少该容器可使用内存
					res.AvailableMemInBytes -= actualRequireMemory
					res.requests[req.RequestId] = 1
					functionStatus.Unlock()
					logger.Infof("request id: %s, wait %d to use available container", req.RequestId, latency/1e6)
					break
				}
				functionStatus.Unlock()
				if latency > 30*time.Second.Nanoseconds() {
					return nil, createContainerErr
				}
			}
		}
	}

	node := r.nodeMap.internal[res.nodeId]
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

func (r *Router) getNode(accountId string, memoryReq int64, requestId string) (*NodeInfo, error) {
	var node *NodeInfo
	// 取满足要求情况下，资源最少的节点，以达到紧密排布
	for key := range r.nodeMap.internal {
		nowNode := r.nodeMap.internal[key]
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
		return node, nil
	} else {
		// 达到最大限制直接返回
		if len(r.nodeMap.internal) >= cp.MaxNodeNum {
			return nil, errors.New("node maximum limit reached")
		}

		// 超时没有请求到节点就取消
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
		// 本地ReserveNode 返回的可用memory 比 node.GetStats少了一倍
		node, err := NewNode(nodeDesc.Id, nodeDesc.Address, nodeDesc.NodeServicePort, nodeDesc.MemoryInBytes)
		logger.Infof("ReserveNode memory: %d", nodeDesc.MemoryInBytes)
		if err != nil {
			logger.Errorf("Failed to NewNode %v", err)
			return nil, err
		}
		if len(reserveNode) < cp.ReserveNodeNum {
			reserveNode[node.nodeID] = 1
		}
		r.nodeMap.internal[nodeDesc.Id] = node

		// 用node.GetStats重置可用内存
		//nodeGS, _ := node.GetStats(context.Background(), &nsPb.GetStatsRequest{})
		//node.availableMemInBytes = nodeGS.NodeStats.AvailableMemoryInBytes

		//申请新节点时，打印所有节点信息
		if cp.NeedLog {
			logger.Infof("get node: %s, all node info:", node.address)
			for key := range r.nodeMap.internal {
				node := r.nodeMap.internal[key]
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
	functionStatus.Lock()
	container, ok := functionStatus.ContainerMap[res.ContainerId]
	if !ok {
		functionStatus.Unlock()
		return errors.Errorf("no container found with ContainerId %s", res.ContainerId)
	}
	container.AvailableMemInBytes += requestStatus.ActualRequireMemory
	delete(container.requests, res.RequestID)

	//go func() {
	//	functionStatus.functionReturned <- struct{}{}
	//}()

	// 释放容器判断， 使保留的容器都尽量在相同的节点上
	if len(container.requests) < 1 && len(functionStatus.ContainerMap) > cp.ReserveContainerNum {
		_, ok = reserveNode[container.nodeId]
		if !ok {
			r.nodeMap.Lock()
			node := r.nodeMap.internal[container.nodeId]
			delete(node.containers, container.ContainerId)
			delete(functionStatus.ContainerMap, container.ContainerId)
			// 不需要管返回值，反正请求完成了释放就行了
			go node.RemoveContainer(ctx, &nsPb.RemoveContainerRequest{
				RequestId:   res.RequestID,
				ContainerId: container.ContainerId,
			})
			logger.Infof("success to release container")
			if len(node.containers) < 1 {
				delete(r.nodeMap.internal, node.nodeID)
				go r.rmClient.ReleaseNode(ctx, &rmPb.ReleaseNodeRequest{
					RequestId: res.RequestID,
					Id:        node.nodeID,
				})
				logger.Infof("success to release node")
			} else {
				node.availableMemInBytes += requestStatus.RequireMemory
			}
			r.nodeMap.Unlock()
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
	functionStatus.SuccessRequestNum += 1
	functionStatus.Unlock()

	return nil
}

// 遍历查找满足要求情况下剩余资源最少的容器，以达到紧密排布
func findAvailableContainer(containerMap map[string]*ContainerInfo, actualRequireMemory int64) *ContainerInfo {
	var res *ContainerInfo
	for key := range containerMap {
		container := containerMap[key]
		if container.AvailableMemInBytes > actualRequireMemory {
			if res == nil {
				res = container
			} else {
				if container.AvailableMemInBytes < res.AvailableMemInBytes {
					res = container
				}
			}
		}
	}
	return res
}
