package grpc

import (
	"context"
	"net/http"

	"hopsworks.ai/rdrs/internal/handlers"
	"hopsworks.ai/rdrs/pkg/api"
)

func (s *RonDBServer) Batch(ctx context.Context, reqProto *api.BatchRequestProto) (*api.BatchResponseProto, error) {
	apiKey, err := s.getApiKey(ctx)
	request := api.ConvertBatchRequestProto(reqProto)

	var responseIntf api.BatchOpResponse = (api.BatchOpResponse)(&api.BatchResponseGRPC{})
	responseIntf.Init()

	httpStatus, err := handlers.Handle(s.pkReadHandler, apiKey, request, responseIntf)
	if err != nil {
		return nil, convertError(httpStatus, err.Error())
	} else if httpStatus != http.StatusOK {
		return nil, convertError(httpStatus, "")
	}

	respProto := api.ConvertBatchOpResponse(responseIntf.(*api.BatchResponseGRPC))
	return respProto, nil
}
