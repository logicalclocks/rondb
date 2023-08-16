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
	"net/http"

	"github.com/gin-gonic/gin"
	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/handlers"
	"hopsworks.ai/rdrs/pkg/api"
)

func (h *RouteHandler) PkRead(c *gin.Context) {

	apiKey := c.GetHeader(config.API_KEY_NAME)
	pkReadParams, err := parsePkReadRequest(c)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err)
		return
	}

	var responseIntf api.PKReadResponse = (api.PKReadResponse)(&api.PKReadResponseJSON{})
	responseIntf.Init()

	status, err := handlers.Handle(&h.pkReadHandler, &apiKey, pkReadParams, responseIntf)
	if err != nil {
		c.AbortWithError(status, err)
		return
	}
	c.JSON(status, responseIntf.(*api.PKReadResponseJSON))
}

/*
Parsing both GET and POST parameters
*/
func parsePkReadRequest(c *gin.Context) (*api.PKReadParams, error) {
	getParams := api.PKReadPP{}
	if err := c.ShouldBindUri(&getParams); err != nil {
		return nil, err
	}

	postParams := api.PKReadBody{}
	if err := c.BindJSON(&postParams); err != nil {
		return nil, err
	}

	return &api.PKReadParams{
		DB:          getParams.DB,
		Table:       getParams.Table,
		Filters:     postParams.Filters,
		ReadColumns: postParams.ReadColumns,
		OperationID: postParams.OperationID,
	}, nil
}
