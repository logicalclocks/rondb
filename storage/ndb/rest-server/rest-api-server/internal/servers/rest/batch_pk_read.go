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

package rest

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/gin-gonic/gin"
	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/handlers"
	"hopsworks.ai/rdrs/pkg/api"
)

var operationUrl = regexp.MustCompile("^[a-zA-Z0-9$_]+/[a-zA-Z0-9$_]+/pk-read")

func (h *RouteHandler) BatchPkRead(c *gin.Context) {
	apiKey := c.GetHeader(config.API_KEY_NAME)

	operations := api.BatchOpRequest{}
	err := c.ShouldBindJSON(&operations)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err)
		return
	}

	if operations.Operations == nil {
		c.AbortWithError(http.StatusBadRequest, errors.New("'operations' is missing in payload"))
		return
	}

	// TODO: Place this into Validate() of batchPkReadHandler
	numOperations := len(*operations.Operations)
	pkOperations := make([]*api.PKReadParams, numOperations)
	for i, operation := range *operations.Operations {
		pkOperations[i] = &api.PKReadParams{}
		err := parseOperation(&operation, pkOperations[i])
		if err != nil {
			c.AbortWithError(http.StatusBadRequest, err)
			return
		}
	}

	var responseIntf api.BatchOpResponse = (api.BatchOpResponse)(&api.BatchResponseJSON{})
	responseIntf.Init(numOperations)

	status, err := handlers.Handle(&h.batchPkReadHandler, &apiKey, &pkOperations, responseIntf)
	if err != nil {
		c.AbortWithError(status, err)
		return
	}
	c.JSON(status, responseIntf.(*api.BatchResponseJSON))
}

func parseOperation(operation *api.BatchSubOp, pkReadarams *api.PKReadParams) error {
	// remove leading / character
	if strings.HasPrefix(*operation.RelativeURL, "/") {
		trimmed := strings.Trim(*operation.RelativeURL, "/")
		operation.RelativeURL = &trimmed
	}

	match := operationUrl.MatchString(*operation.RelativeURL)
	if !match {
		return fmt.Errorf("invalid relative URL: %s", *operation.RelativeURL)
	}
	return makePKReadParams(operation, pkReadarams)
}

func makePKReadParams(operation *api.BatchSubOp, pkReadarams *api.PKReadParams) error {
	if operation.Body == nil {
		return errors.New("body of operation is nil")
	}
	params := *operation.Body

	// split the relative url to extract path parameters
	splits := strings.Split(*operation.RelativeURL, "/")
	if len(splits) != 3 {
		return errors.New("failed to extract database and table information from relative url")
	}

	pkReadarams.DB = &splits[0]
	pkReadarams.Table = &splits[1]
	pkReadarams.Filters = params.Filters
	pkReadarams.ReadColumns = params.ReadColumns
	pkReadarams.OperationID = params.OperationID

	return nil
}
