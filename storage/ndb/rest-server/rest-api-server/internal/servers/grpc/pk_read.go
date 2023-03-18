package grpc

import (
	"context"
	"net/http"

	"google.golang.org/grpc/status"
	"hopsworks.ai/rdrs/internal/common"
	"hopsworks.ai/rdrs/internal/handlers"
	"hopsworks.ai/rdrs/pkg/api"
)

func (s *RonDBServer) PKRead(ctx context.Context, reqProto *api.PKReadRequestProto) (*api.PKReadResponseProto, error) {
	apiKey, _ := s.getApiKey(ctx)

	request := api.ConvertPKReadRequestProto(reqProto)

	var responseIntf api.PKReadResponse = (api.PKReadResponse)(&api.PKReadResponseGRPC{})
	responseIntf.Init()

	httpStatus, err := handlers.Handle(s.pkReadHandler, &apiKey, request, responseIntf)
	statusCode := common.HttpStatusToGrpcCode(httpStatus)
	if err != nil {
		return nil, status.Error(statusCode, err.Error())
	} else if httpStatus != http.StatusOK {
		return nil, status.Error(statusCode, "")
	}

	respProto := api.ConvertPKReadResponse(responseIntf.(*api.PKReadResponseGRPC))
	return respProto, nil
}
