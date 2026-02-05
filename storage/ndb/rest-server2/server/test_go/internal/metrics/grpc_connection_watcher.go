/*
 * Copyright (C) 2023 Hopsworks AB
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301,
 * USA.
 */

package metrics

import (
	"context"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/stats"
	"hopsworks.ai/rdrs2/internal/log"
)

type GRPCStatistics struct {
	lock            sync.Mutex
	ConnectionGauge prometheus.Gauge
}

func (c *GRPCStatistics) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	return ctx
}

func (c *GRPCStatistics) HandleRPC(ctx context.Context, s stats.RPCStats) {

}

func (c *GRPCStatistics) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	if log.IsDebug() {
		log.Debugf("New GRPC connection established %v ", info)
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	c.ConnectionGauge.Add(1)
	return ctx
}

func (c *GRPCStatistics) HandleConn(ctx context.Context, s stats.ConnStats) {
	switch s.(type) {
	case *stats.ConnEnd:
		if log.IsDebug() {
			log.Debugf("GRPC connection closed")
		}
		c.lock.Lock()
		defer c.lock.Unlock()
		c.ConnectionGauge.Sub(1)
	}
}
