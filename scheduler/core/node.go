package core

import (
	"fmt"
	cmap "github.com/orcaman/concurrent-map"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"sync"

	pb "aliyun/serverless/mini-faas/nodeservice/proto"
)

type NodeInfo struct {
	sync.Mutex

	nodeID              string
	nodeNo              string
	address             string
	port                int64
	availableMemInBytes int64

	// request,container都要，可能当前的container在发送中
	//因为由于container的创建要等待，那么就无法立马存入container_id，可能导致正在创建container的节点被误认为没有使用，导致被误删。
	requests   cmap.ConcurrentMap // requests_id -> status
	containers cmap.ConcurrentMap

	conn *grpc.ClientConn
	pb.NodeServiceClient
}

func NewNode(nodeID string, nodeNo string, address string, port, memory int64) (*NodeInfo, error) {
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", address, port), grpc.WithInsecure())
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &NodeInfo{
		nodeID:              nodeID,
		nodeNo:              nodeNo,
		address:             address,
		port:                port,
		availableMemInBytes: memory,
		requests:            cmap.New(),
		containers:          cmap.New(),
		conn:                conn,
		NodeServiceClient:   pb.NewNodeServiceClient(conn),
	}, nil
}

// Close closes the connection
func (n *NodeInfo) Close() {
	n.conn.Close()
}
