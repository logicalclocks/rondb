package testclient

import (
	"google.golang.org/grpc"
	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/testutils"
)

/*
	Create a gRPC connection.
	Important: Try re-using connections for benchmarks, since tests start
	to fail if too many connections are opened and closed in a short time.
*/
func InitGRPCConnction() (*grpc.ClientConn, error) {
	conf := config.GetAll()
	grpcConn, err := testutils.CreateGrpcConn(conf.Security.UseHopsworksAPIKeys, conf.Security.EnableTLS)
	if err != nil {
		return nil, err
	}
	return grpcConn, nil
}
