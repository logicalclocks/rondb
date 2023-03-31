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
package testclient

import (
	"google.golang.org/grpc"
	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/testutils"
)

/*
Create a gRPC connection.
Important: Try re-using connections for benchmarks, since tests start
to fail if too many connections are opened and closed in a short time.
*/
func InitGRPCConnction() (*grpc.ClientConn, error) {
	conf := config.GetAll()
	grpcConn, err := testutils.CreateGrpcConn(conf.Security.APIKeyParameters.UseHopsworksAPIKeys, conf.Security.TLS.EnableTLS)
	if err != nil {
		return nil, err
	}
	return grpcConn, nil
}
