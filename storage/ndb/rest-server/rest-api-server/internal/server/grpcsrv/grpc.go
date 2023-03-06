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
package grpcsrv

import (
	"context"
	"encoding/json"
	"net/http"

	"fmt"

	"google.golang.org/grpc/peer"
	"hopsworks.ai/rdrs/internal/handlers"
	"hopsworks.ai/rdrs/internal/log"
	"hopsworks.ai/rdrs/pkg/api"
)

type GRPCServer struct {
	allHandlers *handlers.AllHandlers
	api.UnimplementedRonDBRESTServer
}

var _ api.RonDBRESTServer = (*GRPCServer)(nil)

var server GRPCServer

func GetGRPCServer() *GRPCServer {
	return &server
}

func (s *GRPCServer) RegisterAllHandlers(handlers *handlers.AllHandlers) {
	s.allHandlers = handlers
}

func (s *GRPCServer) PKRead(c context.Context, reqProto *api.PKReadRequestProto) (*api.PKReadResponseProto, error) {

	if s.allHandlers == nil || s.allHandlers.PKReader == nil {
		return nil, fmt.Errorf("PKRead handler is not registered")
	}

	req, apiKey := api.ConvertPKReadRequestProto(reqProto)

	if log.IsTrace() {
		ip := "NA"
		if p, ok := peer.FromContext(c); ok {
			ip = p.Addr.String()
		}
		reqString, _ := json.Marshal(*req)
		log.Tracef("GRPC Primary key request received from %s, Request: \"%s\" %#v\n", ip, reqString)
	}

	var response api.PKReadResponse = (api.PKReadResponse)(&api.PKReadResponseGRPC{})
	response.Init()

	status, err := s.allHandlers.PKReader.PkReadHandler(req, &apiKey, response)
	if err != nil {
		return nil, mkError(status, err)
	}

	if status != http.StatusOK {
		return nil, mkError(status, nil)
	}

	respProto := api.ConvertPKReadResponse(response.(*api.PKReadResponseGRPC))
	return respProto, nil
}

func (s *GRPCServer) Batch(c context.Context, reqProto *api.BatchRequestProto) (*api.BatchResponseProto, error) {

	if s.allHandlers == nil || s.allHandlers.Batcher == nil {
		return nil, fmt.Errorf("Batch ops handler is not registered")
	}

	req, apikey := api.ConvertBatchRequestProto(reqProto)

	if log.IsTrace() {
		ip := "NA"
		if p, ok := peer.FromContext(c); ok {
			ip = p.Addr.String()
		}
		reqString, _ := json.Marshal(req)
		log.Tracef("GRPC batch request received from %s, Request: %s\n", ip, reqString)
	}

	var response = (api.BatchOpResponse)(&api.BatchResponseGRPC{})
	response.Init()

	status, err := s.allHandlers.Batcher.BatchOpsHandler(req, &apikey, response)
	if err != nil {
		return nil, mkError(status, err)
	}

	if status != http.StatusOK {
		return nil, mkError(status, nil)
	}

	respProto := api.ConvertBatchOpResponse(response.(*api.BatchResponseGRPC))
	return respProto, nil
}

func (s *GRPCServer) Stat(ctx context.Context, reqProto *api.StatRequestProto) (*api.StatResponseProto, error) {

	if s.allHandlers == nil || s.allHandlers.Stater == nil {
		return nil, fmt.Errorf("stat handler is not registered")
	}

	if log.IsTrace() {
		ip := "NA"
		if p, ok := peer.FromContext(ctx); ok {
			ip = p.Addr.String()
		}
		log.Tracef("GRPC stat request received from %s\n", ip)
	}

	response := &api.StatResponse{}
	status, err := s.allHandlers.Stater.StatOpsHandler(response)
	if err != nil {
		return nil, mkError(status, err)
	}

	if status != http.StatusOK {
		return nil, mkError(status, nil)
	}

	respProto := api.ConvertStatResponse(response)
	return respProto, nil
}

func mkError(status int, err error) error {
	if err != nil {
		return fmt.Errorf("error code: %d, Error: %v ", status, err)
	} else {
		return fmt.Errorf("error code: %d", status)
	}
}

func (s *GRPCServer) mustEmbedUnimplementedRonDBRestServerServer() {}
