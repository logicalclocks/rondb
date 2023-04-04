package testclient

import (
	"sync"

	"google.golang.org/grpc"
	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/testutils"
)

var grpcConn *grpc.ClientConn
var grpcConnLock sync.Mutex

// Create only one gRPC connection.
// Tests start to fail if too many connections are opened and closed in a short time
func InitGRPCConnction() (*grpc.ClientConn, error) {
	if grpcConn != nil {
		return grpcConn, nil
	}

	grpcConnLock.Lock()
	grpcConnLock.Unlock()
	var err error
	if grpcConn == nil {
		conf := config.GetAll()
		grpcConn, err = testutils.CreateGrpcConn(conf.Security.UseHopsworksAPIKeys, conf.Security.EnableTLS)
		if err != nil {
			return nil, err
		}
	}
	return grpcConn, nil
}

func GetGRPCConnction() *grpc.ClientConn {
	return grpcConn
}
