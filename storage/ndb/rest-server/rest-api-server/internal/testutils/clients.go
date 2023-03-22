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

package testutils

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"hopsworks.ai/rdrs/internal/config"
)

//////////////////////
//////// HTTP ////////
//////////////////////

func SetupHttpClient(t testing.TB) *http.Client {
	tlsConfig := GetClientTLSConfig(t)
	return &http.Client{
		Transport: &http.Transport{TLSClientConfig: tlsConfig},
	}
}

//////////////////////
//////// gRPC ////////
//////////////////////

func CreateGrpcConn(t testing.TB, withAuth, withTLS bool) (*grpc.ClientConn, error) {
	t.Helper()

	grpcDialOptions := []grpc.DialOption{}
	if withAuth {
		grpcDialOptions = append(grpcDialOptions, grpc.WithUnaryInterceptor(clientAuthInterceptor))
	}
	if withTLS {
		tlsConfig := GetClientTLSConfig(t)
		grpcDialOptions = append(grpcDialOptions, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	}

	// Set up a connection to the server
	conf := config.GetAll()
	return grpc.Dial(
		fmt.Sprintf("%s:%d", "localhost", conf.GRPC.ServerPort),
		grpcDialOptions...,
	)
}

func clientAuthInterceptor(
	ctx context.Context,
	method string,
	req interface{},
	reply interface{},
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	// Logic before invoking the invoker
	apiKey := HOPSWORKS_TEST_API_KEY
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", apiKey)

	// Calls the invoker to execute RPC
	err := invoker(ctx, method, req, reply, cc, opts...)

	// Logic after invoking the invoker ...
	return err
}
