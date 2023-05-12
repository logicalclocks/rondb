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
	"runtime"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"hopsworks.ai/rdrs/internal/dal"
	"hopsworks.ai/rdrs/internal/log"
)

func (h *RouteHandler) Metrics(c *gin.Context) {
	//update the RonDB metrics
	rondbStats, dalErr := dal.GetRonDBStats()
	if dalErr != nil {
		log.Warnf("Failed to collect stats from RonDB %v", dalErr.VerboseError())
	} else {
		h.rdrsMetrics.RonDBMetrics.NdbObjectsTotalCountGauge.Set(float64(rondbStats.NdbObjectsTotalCount))
		h.rdrsMetrics.RonDBMetrics.RonDBConnectionStateGauge.Set(float64(rondbStats.NdbConnectionState))
	}

	// Runtime stats
	h.rdrsMetrics.GoRuntimeMetrics.GoRoutinesGauge.Set(float64(runtime.NumGoroutine()))
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	h.rdrsMetrics.GoRuntimeMetrics.MemoryAllocatedGauge.Set(float64(memStats.Alloc))

	p := promhttp.Handler()
	p.ServeHTTP(c.Writer, c.Request)
}
