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
	"math"
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

	var dalErr *dal.DalError
	if noOps >= config.GetAll().Internal.SplitLargeBatchThreshold {
		dalErr = h.executeInSmallerBatches(noOps, reqPtrs, respPtrs)
	} else {
		dalErr = dal.RonDBBatchedPKRead(noOps, reqPtrs, respPtrs)
	}

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

func (h *Handler) executeInSmallerBatches(noOps uint32,
	reqPtrs []*heap.NativeBuffer, respPtrs []*heap.NativeBuffer) *dal.DalError {

	respCh := make(chan *dal.DalError)

	sliceSize := uint32(config.GetAll().Internal.LargeBatchSplitSize)
	numOfSlices := uint32(0)
	if noOps <= sliceSize {
		numOfSlices = 1
	} else {
		numOfSlices = uint32(math.Ceil(float64(noOps) / float64(sliceSize)))
	}

	for slice := uint32(0); slice < numOfSlices; slice++ {
		start := uint32(slice * sliceSize)
		end := uint32(math.Min(float64(((slice + 1) * sliceSize)), float64(noOps)))

		go h.executeInSmallerBatchesInternal(end-start, reqPtrs[start:end], respPtrs[start:end], respCh)
	}

	//wait for responses
	errors := make([]*dal.DalError, numOfSlices)
	for slice := uint32(0); slice < numOfSlices; slice++ {
		errors[slice] = <-respCh
	}

	for slice := uint32(0); slice < numOfSlices; slice++ {
		//return the first error
		if errors[slice] != nil {
			return errors[slice]
		}
	}

	return nil
}

func (h *Handler) executeInSmallerBatchesInternal(noOps uint32,
	reqPtrs []*heap.NativeBuffer, respPtrs []*heap.NativeBuffer,
	respCh chan *dal.DalError) {
	dalErr := dal.RonDBBatchedPKRead(noOps, reqPtrs, respPtrs)
	if dalErr != nil {
		if dalErr.HttpCode != http.StatusOK {
			respCh <- dalErr
			fmt.Printf("Returinign error %v\n", dalErr.VerboseError())
			return
		}
	}
	respCh <- nil
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
		response.AddSubResponse(idx, pkReadResponseWithCode)
	}
	return http.StatusOK, nil
}
