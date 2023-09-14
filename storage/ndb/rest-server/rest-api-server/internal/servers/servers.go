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
	"errors"
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
	rdrsMetrics *metrics.RDRSMetrics,
	quit chan os.Signal,
) (cleanup func(), err error) {

	cleanupWrapper := func(cleanupFNs []func()) func() {
		return func() {
			//clean up in reverse order
			for i := len(cleanupFNs) - 1; i >= 0; i-- {
				if cleanupFNs[i] != nil {
					(cleanupFNs[i])()
				}
			}
		}
	}
	cleanupFNs := []func(){}

	conf := config.GetAll()
	if !conf.GRPC.Enable && !conf.REST.Enable {
		return nil, errors.New("both REST and gRPC interfaces are disabled")
	}

	// Connect to RonDB
	dalErr := dal.InitRonDBConnection(conf.RonDB, conf.RonDBMetadataCluster)
	if dalErr != nil {
		return nil, dalErr
	}
	cleanupRonDB := func() {
		dalErr = dal.ShutdownConnection()
		if dalErr != nil {
			log.Error(dalErr.Error())
		}
	}
	cleanupFNs = append(cleanupFNs, cleanupRonDB)

	var tlsConfig *tls.Config
	if conf.Security.TLS.EnableTLS {
		tlsConfig, err = tlsutils.GenerateTLSConfig(
			conf.Security.TLS.RequireAndVerifyClientCert,
			conf.Security.TLS.RootCACertFile,
			conf.Security.TLS.CertificateFile,
			conf.Security.TLS.PrivateKeyFile,
		)
		if err != nil {
			cleanupWrapper(cleanupFNs)()
			return nil, fmt.Errorf("failed generating tls configuration; error: %w", err)
		}
	}

	if conf.GRPC.Enable {
		grpcServer := grpc.New(tlsConfig, heap, apiKeyCache, rdrsMetrics)
		cleanupGrpc, err := grpc.Start(
			grpcServer,
			conf.GRPC.ServerIP,
			conf.GRPC.ServerPort,
			quit,
		)
		if err != nil {
			cleanupWrapper(cleanupFNs)()
			return nil, fmt.Errorf("failed starting gRPC server; error: %w", err)
		}
		cleanupFNs = append(cleanupFNs, cleanupGrpc)
	}

	if conf.REST.Enable {
		restServer := rest.New(
			conf.REST.ServerIP,
			conf.REST.ServerPort,
			tlsConfig,
			heap,
			apiKeyCache,
			rdrsMetrics,
		)

		cleanupRest := restServer.Start(quit)
		cleanupFNs = append(cleanupFNs, cleanupRest)
	}

	return cleanupWrapper(cleanupFNs), nil
}
