package core

import (
	pb "aliyun/serverless/mini-faas/nodeservice/proto"
	"fmt"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

type NodeInfo struct {
	nodeID              string
	address             string
	port                int64
	availableMemInBytes int64
	totalMemInBytes     int64

	conn *grpc.ClientConn
	pb.NodeServiceClient
}

func NewNode(nodeID string, address string, port, memory int64) (*NodeInfo, error) {
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", address, port), grpc.WithInsecure())
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &NodeInfo{
		nodeID:              nodeID,
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
