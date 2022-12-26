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
package batchpkread

import (
	"errors"
	"net/http"

	"hopsworks.ai/rdrs/internal/dal"
	"hopsworks.ai/rdrs/internal/handlers/pkread"
	"hopsworks.ai/rdrs/internal/security/apikey"
	"hopsworks.ai/rdrs/pkg/api"
)

type Handler struct{}

// TODO: This might have to be instantiated properly
var pkReadHandler pkread.Handler

func (h Handler) Validate(request interface{}) error {
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

func (h Handler) Authenticate(apiKey *string, request interface{}) error {
	pkOperations := request.(*[]*api.PKReadParams)

	dbMap := make(map[string]bool)
	dbArr := []*string{}

	for _, op := range *pkOperations {
		dbMap[*op.DB] = true
	}

	for dbKey := range dbMap {
		dbArr = append(dbArr, &dbKey)
	}

	return apikey.ValidateAPIKey(apiKey, dbArr...)
}

func (h Handler) Execute(request interface{}, response interface{}) (int, error) {
	pkOperations := request.(*[]*api.PKReadParams)

	noOps := uint32(len(*pkOperations))
	reqPtrs := make([]*dal.NativeBuffer, noOps)
	respPtrs := make([]*dal.NativeBuffer, noOps)

	var err error
	for i, pkOp := range *pkOperations {
		reqPtrs[i], respPtrs[i], err = pkread.CreateNativeRequest(pkOp)
		defer func() {
			err = dal.ReturnBuffer(reqPtrs[i])
			if err != nil {
				panic(err)
			}
			err = dal.ReturnBuffer(respPtrs[i])
			if err != nil {
				panic(err)
			}
		}()
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

func processResponses(respBuffs *[]*dal.NativeBuffer, response api.BatchOpResponse) (int, error) {
	for _, respBuff := range *respBuffs {
		pkReadResponseWithCode := response.CreateNewSubResponse()
		pkReadResponse := pkReadResponseWithCode.GetPKReadResponse()

		subRespCode, err := pkread.ProcessPKReadResponse(respBuff, pkReadResponse)
		if err != nil {
			return int(subRespCode), err
		}

		pkReadResponseWithCode.SetCode(&subRespCode)
		err = response.AppendSubResponse(pkReadResponseWithCode)
		if err != nil {
			return http.StatusInternalServerError, err
		}
	}
	return http.StatusOK, nil
}
