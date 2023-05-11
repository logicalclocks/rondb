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
package batchpkread

import (
	"errors"
	"fmt"
	"net/http"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/dal"
	"hopsworks.ai/rdrs/internal/dal/heap"
	"hopsworks.ai/rdrs/internal/handlers/pkread"
	"hopsworks.ai/rdrs/internal/security/apikey"
	"hopsworks.ai/rdrs/pkg/api"
)

type Handler struct {
	heap        *heap.Heap
	apiKeyCache apikey.Cache
}

func New(heap *heap.Heap, apiKeyCache apikey.Cache) Handler {
	return Handler{heap, apiKeyCache}
}

// TODO: This might have to be instantiated properly
var pkReadHandler pkread.Handler

func (h *Handler) Validate(request interface{}) error {
	pkOperations := request.(*[]*api.PKReadParams)
	if pkOperations == nil {
		return errors.New("operations is missing in payload")
	} else if len(*pkOperations) == 0 {
		return errors.New("list of operations is empty")
	}

	for _, op := range *pkOperations {
		if err := pkReadHandler.Validate(op); err != nil {
			return err
		}
	}
	return nil
}

func (h *Handler) Authenticate(apiKey *string, request interface{}) error {
	conf := config.GetAll()
	if !conf.Security.APIKey.UseHopsworksAPIKeys {
		return nil
	}

	pkOperations := request.(*[]*api.PKReadParams)

	dbMap := make(map[string]bool)
	dbArr := []*string{}

	for _, op := range *pkOperations {
		dbMap[*op.DB] = true
	}

	for dbKey := range dbMap {
		dbArr = append(dbArr, &dbKey)
	}

	err := h.apiKeyCache.ValidateAPIKey(apiKey, dbArr...)
	if err != nil {
		fmt.Printf("Validation failed. key %s, error  %v \n", *apiKey, err)
	}
	return err
}

func (h *Handler) Execute(request interface{}, response interface{}) (int, error) {
	pkOperations := request.(*[]*api.PKReadParams)

	noOps := uint32(len(*pkOperations))
	reqPtrs := make([]*heap.NativeBuffer, noOps)
	respPtrs := make([]*heap.NativeBuffer, noOps)

	var err error
	for idx, pkOp := range *pkOperations {
		reqBuff, releaseReqBuff := h.heap.GetBuffer()
		defer releaseReqBuff()
		respBuff, releaseResBuff := h.heap.GetBuffer()
		defer releaseResBuff()

		reqPtrs[idx] = reqBuff
		respPtrs[idx] = respBuff

		err = pkread.CreateNativeRequest(pkOp, reqBuff, respBuff)
		if err != nil {
			return http.StatusInternalServerError, err
		}
	}

	dalErr := dal.RonDBBatchedPKRead(noOps, reqPtrs, respPtrs)
	if dalErr != nil {
		var message string
		if dalErr.HttpCode >= http.StatusInternalServerError {
			message = dalErr.VerboseError()
		} else {
			message = dalErr.Message
		}
		return dalErr.HttpCode, errors.New(message)
	}

	batchResponse := response.(api.BatchOpResponse)
	status, err := processResponses(&respPtrs, batchResponse)
	if err != nil {
		return status, err
	}

	return http.StatusOK, nil
}

func processResponses(respBuffs *[]*heap.NativeBuffer, response api.BatchOpResponse) (int, error) {
	for idx, respBuff := range *respBuffs {
		pkReadResponseWithCode := response.CreateNewSubResponse()
		pkReadResponse := pkReadResponseWithCode.GetPKReadResponse()

		subRespCode, err := pkread.ProcessPKReadResponse(respBuff, pkReadResponse)
		if err != nil {
			return int(subRespCode), err
		}

		pkReadResponseWithCode.SetCode(&subRespCode)
		err = response.AddSubResponse(idx, pkReadResponseWithCode)
		if err != nil {
			return http.StatusInternalServerError, err
		}
	}
	return http.StatusOK, nil
}
