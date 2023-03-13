package grpc

import (
	"context"
	"net/http"

	"google.golang.org/grpc/status"
	"hopsworks.ai/rdrs/internal/common"
	"hopsworks.ai/rdrs/internal/handlers"
	"hopsworks.ai/rdrs/pkg/api"
)

func (s *RonDBServer) Stat(ctx context.Context, reqProto *api.StatRequestProto) (*api.StatResponseProto, error) {
	apiKey, err := s.getApiKey(ctx)
	statResp := api.StatResponse{}
	httpStatus, err := handlers.Handle(s.statsHandler, &apiKey, nil, &statResp)
	statusCode := common.HttpStatusToGrpcCode(httpStatus)
	if err != nil {
		return nil, status.Error(statusCode, err.Error())
	} else if httpStatus != http.StatusOK {
		return nil, status.Error(statusCode, "")
	}
	respProto := api.ConvertStatResponse(&statResp)
	return respProto, nil
}
