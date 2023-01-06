package grpc

import (
	"context"
	"net/http"

	"hopsworks.ai/rdrs/internal/handlers"
	"hopsworks.ai/rdrs/pkg/api"
)

func (s *RonDBServer) Stat(ctx context.Context, reqProto *api.StatRequestProto) (*api.StatResponseProto, error) {
	apiKey, err := s.getApiKey(ctx)
	statResp := api.StatResponse{}
	httpStatus, err := handlers.Handle(s.statsHandler, &apiKey, nil, statResp)
	if err != nil {
		return nil, convertError(httpStatus, err.Error())
	} else if httpStatus != http.StatusOK {
		return nil, convertError(httpStatus, "")
	}
	respProto := api.ConvertStatResponse(&statResp)
	return respProto, nil
}
