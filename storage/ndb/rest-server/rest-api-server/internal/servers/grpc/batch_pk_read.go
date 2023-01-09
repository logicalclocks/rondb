package grpc

import (
	"context"
	"net/http"

	"google.golang.org/grpc/status"
	"hopsworks.ai/rdrs/internal/common"
	"hopsworks.ai/rdrs/internal/handlers"
	"hopsworks.ai/rdrs/pkg/api"
)

func (s *RonDBServer) Batch(ctx context.Context, reqProto *api.BatchRequestProto) (*api.BatchResponseProto, error) {
	apiKey, err := s.getApiKey(ctx)
	request := api.ConvertBatchRequestProto(reqProto)

	var responseIntf api.BatchOpResponse = (api.BatchOpResponse)(&api.BatchResponseGRPC{})
	responseIntf.Init()

	httpStatus, err := handlers.Handle(s.batchPkReadHandler, &apiKey, request, responseIntf)
	statusCode := common.HttpStatusToGrpcCode(httpStatus)
	if err != nil {
		return nil, status.Error(statusCode, err.Error())
	} else if httpStatus != http.StatusOK {
		return nil, status.Error(statusCode, "")
	}

	respProto := api.ConvertBatchOpResponse(responseIntf.(*api.BatchResponseGRPC))
	return respProto, nil
}
