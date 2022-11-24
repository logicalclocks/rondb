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
package batchops

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"regexp"
	"strings"

	"github.com/gin-gonic/gin"
	"hopsworks.ai/rdrs/internal/common"
	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/dal"
	"hopsworks.ai/rdrs/internal/handlers"
	"hopsworks.ai/rdrs/internal/handlers/pkread"
	"hopsworks.ai/rdrs/internal/log"
	"hopsworks.ai/rdrs/internal/security/apikey"
	"hopsworks.ai/rdrs/pkg/api"
)

type Batch struct{}

var _ handlers.Batcher = (*Batch)(nil)
var batch Batch

func GetBatcher() handlers.Batcher {
	return &batch
}

func (b *Batch) BatchOpsHttpHandler(c *gin.Context) {
	operations := api.BatchOpRequest{}
	err := c.ShouldBindJSON(&operations)
	if err != nil {
		if log.IsDebug() {
			body, _ := ioutil.ReadAll(c.Request.Body)
			log.Debugf("Unable to parse request. Error: %v. Body: %s\n", err, body)
		}
		common.SetResponseBodyError(c, http.StatusBadRequest, err)
		return
	}

	if operations.Operations == nil {
		common.SetResponseBodyError(c, http.StatusBadRequest, fmt.Errorf("No valid operations found"))
		return
	}

	pkOperations := make([]*api.PKReadParams, len(*operations.Operations))
	for i, operation := range *operations.Operations {
		pkOperations[i] = &api.PKReadParams{}
		err := parseOperation(&operation, pkOperations[i])
		if err != nil {
			if log.IsDebug() {
				log.Debugf("Error: %v", err)
			}
			common.SetResponseBodyError(c, http.StatusBadRequest, err)
			return
		}
	}

	var response api.BatchOpResponse = (api.BatchOpResponse)(&api.BatchResponseJSON{})
	response.Init()

	status, err := batch.BatchOpsHandler(&pkOperations, getAPIKey(c), response)
	if err != nil {
		common.SetResponseBodyError(c, status, err)
	}

	common.SetResponseBody(c, status, &response)
}

func (b *Batch) BatchOpsHandler(pkOperations *[]*api.PKReadParams, apiKey *string, response api.BatchOpResponse) (int, error) {

	err := checkAPIKey(pkOperations, apiKey)
	if err != nil {
		return http.StatusUnauthorized, err
	}

	noOps := uint32(len(*pkOperations))
	reqPtrs := make([]*dal.NativeBuffer, noOps)
	respPtrs := make([]*dal.NativeBuffer, noOps)

	for i, pkOp := range *pkOperations {
		reqPtrs[i], respPtrs[i], err = pkread.CreateNativeRequest(pkOp)
		defer dal.ReturnBuffer(reqPtrs[i])
		defer dal.ReturnBuffer(respPtrs[i])
		if err != nil {
			return http.StatusInternalServerError, err
		}
	}

	dalErr := dal.RonDBBatchedPKRead(noOps, reqPtrs, respPtrs)
	var message string
	if dalErr != nil {
		if dalErr.HttpCode >= http.StatusInternalServerError {
			message = fmt.Sprintf("%v File: %v, Line: %v ", dalErr.Message, dalErr.ErrFileName, dalErr.ErrLineNo)
		} else {
			message = fmt.Sprintf("%v", dalErr.Message)
		}
		return dalErr.HttpCode, fmt.Errorf("%s", message)
	}

	status, err := processResponses(&respPtrs, response)
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

func parseOperation(operation *api.BatchSubOp, pkReadarams *api.PKReadParams) error {

	//remove leading / character
	if strings.HasPrefix(*operation.RelativeURL, "/") {
		trimmed := strings.Trim(*operation.RelativeURL, "/")
		operation.RelativeURL = &trimmed
	}

	match, err := regexp.MatchString("^[a-zA-Z0-9$_]+/[a-zA-Z0-9$_]+/pk-read",
		*operation.RelativeURL)
	if err != nil {
		return fmt.Errorf("Error parsing relative URL: %w", err)
	} else if !match {
		return fmt.Errorf("Invalid Relative URL: %s", *operation.RelativeURL)
	} else {
		err := makePKReadParams(operation, pkReadarams)
		if err != nil {
			return err
		}
	}
	return nil
}

func makePKReadParams(operation *api.BatchSubOp, pkReadarams *api.PKReadParams) error {
	params := *operation.Body

	//split the relative url to extract path parameters
	splits := strings.Split(*operation.RelativeURL, "/")
	if len(splits) != 3 {
		return fmt.Errorf("Failed to extract database and table information from relative url")
	}

	pkReadarams.DB = &splits[0]
	pkReadarams.Table = &splits[1]
	pkReadarams.Filters = params.Filters
	pkReadarams.ReadColumns = params.ReadColumns
	pkReadarams.OperationID = params.OperationID

	return nil
}

func getAPIKey(c *gin.Context) *string {
	apiKey := c.GetHeader(config.API_KEY_NAME)
	return &apiKey
}

func checkAPIKey(pkOperations *[]*api.PKReadParams, apiKey *string) error {
	// check for Hopsworks api keys
	if config.Configuration().Security.UseHopsWorksAPIKeys {
		if apiKey == nil || *apiKey == "" { // not set
			return fmt.Errorf("Unauthorized. No API key supplied")
		}

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
	return nil
}
