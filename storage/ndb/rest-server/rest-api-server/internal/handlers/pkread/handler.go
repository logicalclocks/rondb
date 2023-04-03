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

package pkread

import (
	"fmt"
	"net/http"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/dal"
	"hopsworks.ai/rdrs/internal/dal/heap"
	"hopsworks.ai/rdrs/internal/handlers/validators"
	"hopsworks.ai/rdrs/internal/security/apikey"
	"hopsworks.ai/rdrs/pkg/api"
)

type Handler struct {
	heap *heap.Heap
}

func New(heap *heap.Heap) Handler {
	return Handler{heap}
}

func (h Handler) Validate(request interface{}) error {
	pkReadParams := request.(*api.PKReadParams)

	if err := validators.ValidateDBIdentifier(pkReadParams.DB); err != nil {
		return fmt.Errorf("db name is invalid; error: %w", err)
	}

	if err := validators.ValidateDBIdentifier(pkReadParams.Table); err != nil {
		return fmt.Errorf("table name is invalid; error: %w", err)
	}

	return ValidateBody(pkReadParams)
}

func (h Handler) Authenticate(apiKey *string, request interface{}) error {
	conf := config.GetAll()
	if !conf.Security.UseHopsworksAPIKeys {
		return nil
	}
	pkReadParams := request.(*api.PKReadParams)
	return apikey.ValidateAPIKey(apiKey, pkReadParams.DB)
}

func (h Handler) Execute(request interface{}, response interface{}) (int, error) {
	pkReadParams := request.(*api.PKReadParams)

	reqBuff, releaseReqBuff := h.heap.GetBuffer()
	defer releaseReqBuff()
	respBuff, releaseResBuff := h.heap.GetBuffer()
	defer releaseResBuff()

	err := CreateNativeRequest(pkReadParams, reqBuff, respBuff)
	if err != nil {
		return http.StatusInternalServerError, err
	}

	dalErr := dal.RonDBPKRead(reqBuff, respBuff)
	if dalErr != nil && dalErr.HttpCode != http.StatusOK { // any other error return immediately
		return dalErr.HttpCode, dalErr
	}

	pkReadResponse := response.(api.PKReadResponse)
	status, err := ProcessPKReadResponse(respBuff, pkReadResponse)
	if err != nil {
		return http.StatusInternalServerError, err
	}

	return int(status), nil
}
