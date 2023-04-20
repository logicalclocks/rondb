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

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

type HTTPMetrics struct {
	PingCounter        prometheus.Counter
	PkReadCounter      prometheus.Counter
	BatchPkReadCounter prometheus.Counter
	StatCounter        prometheus.Counter
}

type GRPCMetrics struct {
	PingCounter        prometheus.Counter
	PkReadCounter      prometheus.Counter
	BatchPkReadCounter prometheus.Counter
	StatCounter        prometheus.Counter
}

func NewHTTPMetrics() (*HTTPMetrics, func()) {
	protocol := "http"
	metrics := HTTPMetrics{}

	metrics.PingCounter =
		prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: protocol + "_ping_request_count",
				Help: "No of request handled by " + protocol + " ping handler",
			},
		)

	metrics.PkReadCounter =
		prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: protocol + "_pk_read_request_count",
				Help: "No of request handled by " + protocol + " pkread handler",
			},
		)

	metrics.BatchPkReadCounter =
		prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: protocol + "_pk_batch_read_request_count",
				Help: "No of request handled by " + protocol + " batchpk read handler",
			},
		)

	metrics.StatCounter =
		prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: protocol + "_stat_request_count",
				Help: "No of request handled by " + protocol + " stat handler",
			},
		)

	prometheus.MustRegister(metrics.PingCounter)
	prometheus.MustRegister(metrics.PkReadCounter)
	prometheus.MustRegister(metrics.BatchPkReadCounter)
	prometheus.MustRegister(metrics.StatCounter)

	cleanup := func() {
		prometheus.Unregister(metrics.PingCounter)
		prometheus.Unregister(metrics.PkReadCounter)
		prometheus.Unregister(metrics.BatchPkReadCounter)
		prometheus.Unregister(metrics.StatCounter)
	}
	return &metrics, cleanup
}

func NewGRPCMetrics() (*GRPCMetrics, func()) {
	protocol := "gRPC"
	metrics := GRPCMetrics{}

	metrics.PingCounter =
		prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: protocol + "_ping_request_count",
				Help: "No of request handled by " + protocol + " ping handler",
			},
		)

	metrics.PkReadCounter =
		prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: protocol + "_pk_read_request_count",
				Help: "No of request handled by " + protocol + " pkread handler",
			},
		)

	metrics.BatchPkReadCounter =
		prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: protocol + "_pk_batch_read_request_count",
				Help: "No of request handled by " + protocol + " batchpk read handler",
			},
		)

	metrics.StatCounter =
		prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: protocol + "_stat_request_count",
				Help: "No of request handled by " + protocol + " stat handler",
			},
		)

	prometheus.MustRegister(metrics.PingCounter)
	prometheus.MustRegister(metrics.PkReadCounter)
	prometheus.MustRegister(metrics.BatchPkReadCounter)
	prometheus.MustRegister(metrics.StatCounter)

	cleanup := func() {
		prometheus.Unregister(metrics.PingCounter)
		prometheus.Unregister(metrics.PkReadCounter)
		prometheus.Unregister(metrics.BatchPkReadCounter)
		prometheus.Unregister(metrics.StatCounter)
	}
	return &metrics, cleanup
}
