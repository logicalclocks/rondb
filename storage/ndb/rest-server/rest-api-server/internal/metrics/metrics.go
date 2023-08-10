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
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

type RDRSMetrics struct {
	RonDBMetrics     *RonDBMetrics
	EndPointMetrics  *EndPointMetrics
	HTTPMetrics      *HTTPMetrics
	GRPCMetrics      *GRPCMetrics
	GoRuntimeMetrics *GoRuntimeMetrics
}

type GoRuntimeMetrics struct {
	MemoryAllocatedGauge prometheus.Gauge
	GoRoutinesGauge      prometheus.Gauge
}

type RonDBMetrics struct {
	NdbObjectsTotalCountGauge prometheus.Gauge
	RonDBConnectionStateGauge prometheus.Gauge
}

type GRPCMetrics struct {
	GRPCStatistics GRPCStatistics
}

type HTTPMetrics struct {
	HttpConnectionGauge HttpConnectionGauge
}

type EndPointMetrics struct {
	ResponseTimeSummary prometheus.SummaryVec
	ResponseStatusCount prometheus.CounterVec
}

const (
	ENDPOINT = "endpoint"
	METHOD   = "method"
	API_TYPE = "api_type"
	STATUS   = "status"
)

func (h *EndPointMetrics) AddResponseTime(
	endPoint string,
	api_type string,
	method string,
	time float64,
) {
	h.ResponseTimeSummary.With(prometheus.Labels{ENDPOINT: endPoint, API_TYPE: api_type, METHOD: method}).Observe(time)
}

func (h *EndPointMetrics) AddResponseStatus(
	endPoint string,
	api_type string,
	method string,
	status int,
) {
	h.ResponseStatusCount.With(prometheus.Labels{ENDPOINT: endPoint, API_TYPE: api_type, METHOD: method, STATUS: fmt.Sprintf("%d", status)}).Inc()
}

func NewRDRSMetrics() (*RDRSMetrics, func()) {

	metrics := RDRSMetrics{}
	rondbMetrics, rondbMetricsCleanup := newRonDBMetrics()
	httpMetrics, httpMetricsCleanup := newHTTPMetrics()
	endPointMetrics, endPointMetricsCleanup := newEndPointMetrics()
	grpcMetrics, grpcMetricsCleanup := newGRPCMetrics()
	runtimeMetrics, runtimeMetricsCleanup := newGoRuntimeMetrics()
	metrics.RonDBMetrics = rondbMetrics
	metrics.EndPointMetrics = endPointMetrics
	metrics.HTTPMetrics = httpMetrics
	metrics.GRPCMetrics = grpcMetrics
	metrics.GoRuntimeMetrics = runtimeMetrics

	return &metrics, func() {
		rondbMetricsCleanup()
		endPointMetricsCleanup()
		httpMetricsCleanup()
		grpcMetricsCleanup()
		runtimeMetricsCleanup()
	}
}

func newGoRuntimeMetrics() (*GoRuntimeMetrics, func()) {
	protocol := "rdrs_go_runtime"
	metrics := GoRuntimeMetrics{}

	metrics.GoRoutinesGauge =
		prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: protocol + "_number_of_go_routines",
				Help: "Number of active go routines",
			})

	metrics.MemoryAllocatedGauge =
		prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: protocol + "_allocated_memory",
				Help: "Total memory allocated to the process",
			})

	prometheus.MustRegister(metrics.GoRoutinesGauge)
	prometheus.MustRegister(metrics.MemoryAllocatedGauge)
	cleanup := func() {
		prometheus.Unregister(metrics.GoRoutinesGauge)
		prometheus.Unregister(metrics.MemoryAllocatedGauge)
	}
	return &metrics, cleanup
}

func newRonDBMetrics() (*RonDBMetrics, func()) {
	protocol := "rdrs_rondb"
	metrics := RonDBMetrics{}

	metrics.RonDBConnectionStateGauge =
		prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: protocol + "_connection_state",
				Help: "Connection state (0: connected, > 0  not connected)",
			})

	metrics.NdbObjectsTotalCountGauge =
		prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: protocol + "_total_ndb_objects",
				Help: "Total NDB objects",
			})

	prometheus.MustRegister(metrics.RonDBConnectionStateGauge)
	prometheus.MustRegister(metrics.NdbObjectsTotalCountGauge)
	cleanup := func() {
		prometheus.Unregister(metrics.RonDBConnectionStateGauge)
		prometheus.Unregister(metrics.NdbObjectsTotalCountGauge)
	}
	return &metrics, cleanup
}

func newEndPointMetrics() (*EndPointMetrics, func()) {
	protocol := "rdrs_endpoints"
	metrics := EndPointMetrics{}

	metrics.ResponseTimeSummary =
		*prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Name:       protocol + "_response_time_summary",
				Help:       "Summary for response time handled by " + protocol + " handler. Time is in nanoseconds",
				Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.95: 0.01, 0.99: 0.001},
			},
			[]string{ENDPOINT, API_TYPE, METHOD},
		)

	metrics.ResponseStatusCount =
		*prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: protocol + "_response_status_count",
				Help: "No of response status returned by " + protocol,
			},
			[]string{ENDPOINT, API_TYPE, METHOD, STATUS},
		)

	prometheus.MustRegister(metrics.ResponseTimeSummary)
	prometheus.MustRegister(metrics.ResponseStatusCount)

	cleanup := func() {
		prometheus.Unregister(metrics.ResponseTimeSummary)
		prometheus.Unregister(metrics.ResponseStatusCount)
	}
	return &metrics, cleanup
}

func newHTTPMetrics() (*HTTPMetrics, func()) {
	protocol := "rdrs_http"
	metrics := HTTPMetrics{}

	metrics.HttpConnectionGauge = HttpConnectionGauge{}
	metrics.HttpConnectionGauge.ConnectionGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: protocol + "_connection_count",
			Help: "No of open " + protocol + " connections",
		},
	)

	prometheus.MustRegister(metrics.HttpConnectionGauge.ConnectionGauge)

	cleanup := func() {
		prometheus.Unregister(metrics.HttpConnectionGauge.ConnectionGauge)
	}
	return &metrics, cleanup
}

func newGRPCMetrics() (*GRPCMetrics, func()) {
	protocol := "rdrs_grpc"
	metrics := GRPCMetrics{}

	metrics.GRPCStatistics = GRPCStatistics{}
	metrics.GRPCStatistics.ConnectionGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: protocol + "_connection_count",
			Help: "No of open " + protocol + " connections",
		},
	)

	prometheus.MustRegister(metrics.GRPCStatistics.ConnectionGauge)

	cleanup := func() {
		prometheus.Unregister(metrics.GRPCStatistics.ConnectionGauge)
	}
	return &metrics, cleanup
}
