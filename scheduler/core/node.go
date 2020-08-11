package core

import (
	"fmt"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"sync"

	pb "aliyun/serverless/mini-faas/nodeservice/proto"
)

type NodeInfo struct {
	sync.Mutex

	nodeID              string
	nodeNo              int
	address             string
	port                int64
	availableMemInBytes int64
	totalMemInBytes     int64

	conn *grpc.ClientConn
	pb.NodeServiceClient
}

func NewNode(nodeID string, nodeNo int, address string, port, memory int64) (*NodeInfo, error) {
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
		totalMemInBytes:     memory,
		conn:                conn,
		NodeServiceClient:   pb.NewNodeServiceClient(conn),
	}, nil
}

// Close closes the connection
func (n *NodeInfo) Close() {
	n.conn.Close()
}
