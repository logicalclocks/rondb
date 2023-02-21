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

package stat

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"hopsworks.ai/rdrs/internal/common"
	"hopsworks.ai/rdrs/internal/dal"
	"hopsworks.ai/rdrs/internal/handlers"
	"hopsworks.ai/rdrs/internal/log"
	"hopsworks.ai/rdrs/pkg/api"
)

const PATH = "/stat"

type Stat struct{}

var _ handlers.Stater = (*Stat)(nil)
var stat Stat

func GetStater() handlers.Stater {
	return &stat
}

func (s *Stat) StatOpsHttpHandler(c *gin.Context) {
	if log.IsTrace() {
		log.Tracef("HTTP Stat request received from %s\n", c.ClientIP())
	}
	statResp := api.StatResponse{}
	stats, err := stat.StatOpsHandler(&statResp)
	if err != nil {
		common.SetResponseBodyError(c, http.StatusInternalServerError, err)
		return
	}
	common.SetResponseBody(c, stats, &statResp)
}

func (s *Stat) StatOpsHandler(statResp *api.StatResponse) (int, error) {

	rondbStats, err := dal.GetRonDBStats()
	if err != nil {
		return http.StatusInternalServerError, err
	}

	nativeBuffersStats := dal.GetNativeBuffersStats()
	statResp.MemoryStats = nativeBuffersStats
	statResp.RonDBStats = *rondbStats

	return http.StatusOK, nil
}
