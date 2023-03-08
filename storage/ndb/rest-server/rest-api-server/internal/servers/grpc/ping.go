package grpc

import (
	"context"

	"hopsworks.ai/rdrs/pkg/api"
)

func (s *RonDBServer) Ping(ctx context.Context, reqProto *api.Empty) (*api.Empty, error) {
	return &api.Empty{}, nil
}
