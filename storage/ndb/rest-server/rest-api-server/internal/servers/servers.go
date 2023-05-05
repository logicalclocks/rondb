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

package servers

import (
	"crypto/tls"
	"fmt"
	"os"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/dal"
	"hopsworks.ai/rdrs/internal/dal/heap"
	"hopsworks.ai/rdrs/internal/log"
	"hopsworks.ai/rdrs/internal/metrics"

	"hopsworks.ai/rdrs/internal/security/apikey"
	"hopsworks.ai/rdrs/internal/security/tlsutils"
	"hopsworks.ai/rdrs/internal/servers/grpc"
	"hopsworks.ai/rdrs/internal/servers/rest"
)

func CreateAndStartDefaultServers(
	heap *heap.Heap,
	apiKeyCache apikey.Cache,
	httpMetrics *metrics.HTTPMetrics,
	grpcMetrics *metrics.GRPCMetrics,
	quit chan os.Signal,
) (cleanup func(), err error) {
	cleanup = func() {}

	// Connect to RonDB
	conf := config.GetAll()
	dalErr := dal.InitRonDBConnection(conf.RonDB)
	if dalErr != nil {
		return cleanup, dalErr
	}
	cleanupRonDB := func() {
		dalErr = dal.ShutdownConnection()
		if dalErr != nil {
			log.Error(dalErr.Error())
		}
	}

	var tlsConfig *tls.Config
	if conf.Security.TLS.EnableTLS {
		tlsConfig, err = tlsutils.GenerateTLSConfig(
			conf.Security.TLS.RequireAndVerifyClientCert,
			conf.Security.TLS.RootCACertFile,
			conf.Security.TLS.CertificateFile,
			conf.Security.TLS.PrivateKeyFile,
		)
		if err != nil {
			cleanupRonDB()
			return cleanup, fmt.Errorf("failed generating tls configuration; error: %w", err)
		}
	}

	grpcServer := grpc.New(tlsConfig, heap, apiKeyCache, grpcMetrics)
	cleanupGrpc, err := grpc.Start(
		grpcServer,
		conf.GRPC.ServerIP,
		conf.GRPC.ServerPort,
		quit,
	)
	if err != nil {
		cleanupRonDB()
		return cleanup, fmt.Errorf("failed starting gRPC server; error: %w", err)
	}

	restServer := rest.New(
		conf.REST.ServerIP,
		conf.REST.ServerPort,
		tlsConfig,
		heap,
		apiKeyCache,
		httpMetrics,
	)

	cleanupRest := restServer.Start(quit)
	return func() {
		cleanupRonDB()
		cleanupGrpc()
		cleanupRest()
	}, nil
}
