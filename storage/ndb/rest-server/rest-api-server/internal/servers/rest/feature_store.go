/* Copyright (c) 2023 Hopsworks AB
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

func (h *RouteHandler) FeatureStore(c *gin.Context) {

	apiKey := c.GetHeader(config.API_KEY_NAME)

	fsReq, err := parseFeatureStoreRequest(c)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err)
		return
	}

	fsResp := api.FeatureStoreResponse{}
	status, err := handlers.Handle(&h.featureStoreHandler, &apiKey, fsReq, &fsResp)
	if err != nil {
		c.AbortWithError(status, err)
		return
	}
	c.JSON(status, fsResp)
}

func parseFeatureStoreRequest(c *gin.Context) (*api.FeatureStoreRequest, error) {

	postParams := api.FeatureStoreRequest{}
	if err := c.BindJSON(&postParams); err != nil {
		return nil, err
	}
	return &postParams, nil
}
