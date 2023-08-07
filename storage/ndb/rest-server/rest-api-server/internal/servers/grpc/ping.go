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
	"time"

	"google.golang.org/grpc/codes"
	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/pkg/api"
)

func (s *RonDBServer) Ping(ctx context.Context, reqProto *api.Empty) (*api.Empty, error) {

	// metrics
	var statusCode = codes.OK
	start := time.Now().UnixNano()
	defer s.rdrsMetrics.EndPointMetrics.AddResponseTime(config.PING_OPERATION, "GRPC", float64(time.Now().UnixNano()-start))
	defer s.rdrsMetrics.EndPointMetrics.AddResponseStatus(config.PING_OPERATION, "GRPC", int(statusCode))

	return &api.Empty{}, nil
}
