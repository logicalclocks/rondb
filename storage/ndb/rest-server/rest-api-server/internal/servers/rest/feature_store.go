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
	"fmt"
	"net/http"

	"github.com/bytedance/sonic"
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
	status, release, err := handlers.Handle(&h.featureStoreHandler, &apiKey, fsReq, &fsResp)
	defer release()
	if err != nil {
		c.AbortWithError(status, err)
		return
	}
	output, err := sonic.Marshal(fsResp)
	if err != nil {
		c.AbortWithError(status, err)
		return
	}
	c.Data(status, "application/json", output)
}

func parseFeatureStoreRequest(c *gin.Context) (*api.FeatureStoreRequest, error) {
	body, err := c.GetRawData()
	if err != nil {
		return nil, err
	}

	postParams := api.FeatureStoreRequest{}
	if err := sonic.Unmarshal(body, &postParams); err != nil {
		return nil, err
	}

	if postParams.FeatureStoreName == nil {
		return nil, fmt.Errorf("Error:Field validation for 'FeatureStoreName' failed")
	}

	if postParams.FeatureViewName == nil {
		return nil, fmt.Errorf("Error:Field validation for 'FeatureViewName' failed")
	}

	if postParams.FeatureViewVersion == nil {
		return nil, fmt.Errorf("Error:Field validation for 'FeatureViewVersion' failed")
	}

	if postParams.Entries == nil || len(*postParams.Entries) == 0 {
		return nil, fmt.Errorf("Error:Field validation for 'Entries' failed. Incorrect primary key.")
	}

	return &postParams, nil
}
