package grpc

import (
	"context"
	"net/http"

	"hopsworks.ai/rdrs/internal/handlers"
	"hopsworks.ai/rdrs/pkg/api"
)

func (s *RonDBServer) PKRead(ctx context.Context, reqProto *api.PKReadRequestProto) (*api.PKReadResponseProto, error) {
	apiKey, err := s.getApiKey(ctx)

	request := api.ConvertPKReadRequestProto(reqProto)

	var responseIntf api.PKReadResponse = (api.PKReadResponse)(&api.PKReadResponseGRPC{})
	responseIntf.Init()

	httpStatus, err := handlers.Handle(s.pkReadHandler, apiKey, request, responseIntf)
	if err != nil {
		return nil, convertError(httpStatus, err.Error())
	}
	if httpStatus != http.StatusOK {
		return nil, convertError(httpStatus, "")
	}

	respProto := api.ConvertPKReadResponse(responseIntf.(*api.PKReadResponseGRPC))
	return respProto, nil
}
