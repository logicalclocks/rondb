/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2023 Hopsworks AB
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 3.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package grpc

import (
	"context"
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"hopsworks.ai/rdrs/internal/common"
	"hopsworks.ai/rdrs/internal/handlers"
	"hopsworks.ai/rdrs/pkg/api"
)

func (s *RonDBServer) PKRead(ctx context.Context, reqProto *api.PKReadRequestProto) (*api.PKReadResponseProto, error) {

	// metrics
	s.grpcMetrics.PkReadCounter.Inc()

	apiKey, err := s.getApiKey(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	request := api.ConvertPKReadRequestProto(reqProto)

	var responseIntf api.PKReadResponse = (api.PKReadResponse)(&api.PKReadResponseGRPC{})
	responseIntf.Init()

	httpStatus, err := handlers.Handle(&s.pkReadHandler, &apiKey, request, responseIntf)
	statusCode := common.HttpStatusToGrpcCode(httpStatus)
	if err != nil {
		return nil, status.Error(statusCode, err.Error())
	} else if httpStatus != http.StatusOK {
		return nil, status.Error(statusCode, "")
	}

	respProto := api.ConvertPKReadResponse(responseIntf.(*api.PKReadResponseGRPC))
	return respProto, nil
}
