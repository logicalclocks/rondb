/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2022 Hopsworks AB
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
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/dal/heap"
	"hopsworks.ai/rdrs/internal/handlers/batchpkread"
	"hopsworks.ai/rdrs/internal/handlers/pkread"
	"hopsworks.ai/rdrs/internal/handlers/stat"
	"hopsworks.ai/rdrs/internal/log"
	"hopsworks.ai/rdrs/pkg/api"
)

func New(serverTLS *tls.Config, heap *heap.Heap) *grpc.Server {
	var grpcServer *grpc.Server
	if serverTLS != nil {
		grpcServer = grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLS)))
	} else {
		grpcServer = grpc.NewServer()
	}
	RonDBServer := NewRonDBServer(heap)
	api.RegisterRonDBRESTServer(grpcServer, RonDBServer)
	return grpcServer
}

func Start(
	grpcServer *grpc.Server,
	host string,
	port uint16,
	quit chan os.Signal,
) (err error, cleanupFunc func()) {
	grpcAddress := fmt.Sprintf("%s:%d", host, port)
	grpcListener, err := net.Listen("tcp", grpcAddress)
	if err != nil {
		return fmt.Errorf("failed listening to gRPC server address '%s'; error: %v", grpcAddress, err), func() {}
	}
	log.Infof("Listening at %s for gRPC server", grpcListener.Addr())
	go func() {
		log.Info("Starting up gRPC server")
		if err := grpcServer.Serve(grpcListener); err != nil {
			log.Errorf("failed to serve gRPC; error: %v", err)
			quit <- syscall.SIGINT
		}
	}()
	return nil, func() {
		log.Info("Gracefully stopping gRPC server")
		grpcServer.GracefulStop()
		/*
			// This seems to already be run with GracefulStop()
			err = grpcListener.Close()
			if err != nil {
				log.Errorf("failed closing gRPC listener; error: %v", err)
			}
		*/
	}
}

// TODO: Add thread-safe logger here
type RonDBServer struct {
	api.UnimplementedRonDBRESTServer
	statsHandler       stat.Handler
	pkReadHandler      pkread.Handler
	batchPkReadHandler batchpkread.Handler
}

func NewRonDBServer(heap *heap.Heap) *RonDBServer {
	return &RonDBServer{
		statsHandler:       stat.New(heap),
		pkReadHandler:      pkread.New(heap),
		batchPkReadHandler: batchpkread.New(heap),
	}
}

func (s *RonDBServer) getApiKey(ctx context.Context) (string, error) {
	conf := config.GetAll()
	if !conf.Security.UseHopsworksAPIKeys {
		return "", nil
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Errorf(codes.Unauthenticated, "Retrieving metadata is failed")
	}
	authHeader, ok := md["authorization"]
	if !ok {
		return "", status.Errorf(codes.Unauthenticated, "authorization token is not supplied")
	}
	if len(authHeader) == 0 {
		return "", status.Errorf(codes.OutOfRange, "authorization token is not supplied")
	}
	return authHeader[0], nil
}
