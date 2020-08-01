package core

import (
	"fmt"
	cmap "github.com/orcaman/concurrent-map"
	"github.com/pkg/errors"
	"google.golang.org/grpc"

	pb "aliyun/serverless/mini-faas/nodeservice/proto"
)

type NodeInfo struct {
	nodeID              string
	address             string
	port                int64
	availableMemInBytes int64

	isReserved bool

	containers cmap.ConcurrentMap // container_id -> status

	conn *grpc.ClientConn
	pb.NodeServiceClient
}

func NewNode(nodeID, address string, port, memory int64) (*NodeInfo, error) {
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", address, port), grpc.WithInsecure())
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &NodeInfo{
		nodeID:              nodeID,
		address:             address,
		port:                port,
		availableMemInBytes: memory,
		isReserved:          false,
		containers:          cmap.New(),
		conn:                conn,
		NodeServiceClient:   pb.NewNodeServiceClient(conn),
	}, nil
}

// Close closes the connection
func (n *NodeInfo) Close() {
	n.conn.Close()
}
