package testutils

import (
	"context"
	"fmt"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"hopsworks.ai/rdrs/internal/common"
	"hopsworks.ai/rdrs/internal/config"
)

func CreateGrpcConn(t testing.TB, tc TlsContext, withAuth, withTLS bool) (*grpc.ClientConn, error) {
	port := config.Configuration().RestServer.GRPCServerPort

	grpcDialOptions := []grpc.DialOption{}
	if withAuth {
		grpcDialOptions = append(grpcDialOptions, grpc.WithUnaryInterceptor(clientAuthInterceptor))
	}
	if withTLS {
		grpcDialOptions = append(grpcDialOptions, grpc.WithTransportCredentials(credentials.NewTLS(GetClientTLSConfig(t, tc))))
	} else {
		grpcDialOptions = append(grpcDialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	// Set up a connection to the server
	return grpc.Dial(
		fmt.Sprintf("%s:%d", "localhost", port),
		grpcDialOptions...,
	)
}

func clientAuthInterceptor(
	ctx context.Context,
	method string,
	req interface{},
	reply interface{},
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	// Logic before invoking the invoker
	apiKey := common.HOPSWORKS_TEST_API_KEY
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", apiKey)

	// Calls the invoker to execute RPC
	err := invoker(ctx, method, req, reply, cc, opts...)

	// Logic after invoking the invoker ...
	return err
}
