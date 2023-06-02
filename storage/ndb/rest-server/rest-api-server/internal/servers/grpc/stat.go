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
	"hopsworks.ai/rdrs/internal/handlers"
	"hopsworks.ai/rdrs/pkg/api"
)

func (s *RonDBServer) Stat(ctx context.Context, reqProto *api.StatRequestProto) (*api.StatResponseProto, error) {

	// metrics
	start := time.Now().UnixNano()
	defer s.rdrsMetrics.GRPCMetrics.StatSummary.Observe(float64(time.Now().UnixNano() - start))
	s.rdrsMetrics.GRPCMetrics.StatCounter.Inc()

	apiKey, err := s.getApiKey(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	statResp := api.StatResponse{}
	httpStatus, err := handlers.Handle(&s.statsHandler, &apiKey, nil, &statResp)
	statusCode := common.HttpStatusToGrpcCode(httpStatus)
	if err != nil {
		return nil, status.Error(statusCode, err.Error())
	} else if httpStatus != http.StatusOK {
		return nil, status.Error(statusCode, "")
	}
	respProto := api.ConvertStatResponse(&statResp)
	return respProto, nil
}
