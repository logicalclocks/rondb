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

	"github.com/bytedance/sonic"
	"github.com/gin-gonic/gin"
	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/handlers"
	"hopsworks.ai/rdrs/pkg/api"
)

func (h *RouteHandler) Stat(c *gin.Context) {
	apiKey := c.GetHeader(config.API_KEY_NAME)
	statResp := api.StatResponse{}
	status, release, err := handlers.Handle(&h.statsHandler, &apiKey, nil, &statResp)
	defer release()
	if err != nil {
		c.AbortWithError(http.StatusInternalServerError, err)
		return
	}

	output, err := sonic.Marshal(statResp)
	if err != nil {
		c.AbortWithError(status, err)
		return
	}
	c.Data(status, "application/json", output)
}
