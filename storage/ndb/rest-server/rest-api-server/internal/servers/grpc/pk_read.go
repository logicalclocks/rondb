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
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"hopsworks.ai/rdrs/internal/common"
	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/handlers"
	"hopsworks.ai/rdrs/pkg/api"
)

func (s *RonDBServer) PKRead(ctx context.Context, reqProto *api.PKReadRequestProto) (*api.PKReadResponseProto, error) {

	// metrics
	var statusCode = codes.OK
	start := time.Now().UnixNano()
	updateMetrics := func(opName string) {
		s.rdrsMetrics.EndPointMetrics.AddResponseTime(opName,
			config.GRPC_API_TYPE, config.GRPC_API_TYPE, float64(time.Now().UnixNano()-start))
		s.rdrsMetrics.EndPointMetrics.AddResponseStatus(opName,
			config.GRPC_API_TYPE, config.GRPC_API_TYPE, int(statusCode))
	}
	defer updateMetrics(config.PK_DB_OPERATION)

	apiKey, err := s.getApiKey(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	request := api.ConvertPKReadRequestProto(reqProto)

	var responseIntf api.PKReadResponse = (api.PKReadResponse)(&api.PKReadResponseGRPC{})
	responseIntf.Init()

	httpStatus, err := handlers.Handle(&s.pkReadHandler, &apiKey, request, responseIntf)
	statusCode = common.HttpStatusToGrpcCode(httpStatus)
	if err != nil {
		return nil, status.Error(statusCode, err.Error())
	} else if httpStatus != http.StatusOK {
		return nil, status.Error(statusCode, "")
	}

	respProto := api.ConvertPKReadResponse(responseIntf.(*api.PKReadResponseGRPC))
	return respProto, nil
}
