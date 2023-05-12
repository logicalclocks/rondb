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

type RDRSMetrics struct {
	RonDBMetrics *RonDBMetrics
	HTTPMetrics  *HTTPMetrics
	GRPCMetrics  *GRPCMetrics
}

type RonDBMetrics struct {
	NdbObjectsTotalCount prometheus.GaugeVec
	RonDBConnectionState prometheus.GaugeVec
}

type HTTPMetrics struct {
	PingCounter         prometheus.Counter
	PkReadCounter       prometheus.Counter
	BatchPkReadCounter  prometheus.Counter
	StatCounter         prometheus.Counter
	PingSummary         prometheus.Summary
	PkReadSummary       prometheus.Summary
	BatchPkReadSummary  prometheus.Summary
	StatSummary         prometheus.Summary
	HttpConnectionGauge HttpConnectionGauge
}

type GRPCMetrics struct {
	PingCounter        prometheus.Counter
	PkReadCounter      prometheus.Counter
	BatchPkReadCounter prometheus.Counter
	StatCounter        prometheus.Counter
	PingSummary        prometheus.Summary
	PkReadSummary      prometheus.Summary
	BatchPkReadSummary prometheus.Summary
	StatSummary        prometheus.Summary
	GRPCStatistics     GRPCStatistics
}

func NewRDRSMetrics() (*RDRSMetrics, func()) {

	metrics := RDRSMetrics{}
	rondbMetrics, rondbMetricsCleanup := newRonDBMetrics()
	httpMetrics, httpMetricsCleanup := newHTTPMetrics()
	grpcMetrics, grpcMetricsCleanup := newGRPCMetrics()
	metrics.RonDBMetrics = rondbMetrics
	metrics.HTTPMetrics = httpMetrics
	metrics.GRPCMetrics = grpcMetrics

	return &metrics, func() {
		rondbMetricsCleanup()
		httpMetricsCleanup()
		grpcMetricsCleanup()
	}
}

func newRonDBMetrics() (*RonDBMetrics, func()) {
	protocol := "rdrs_rondb"
	metrics := RonDBMetrics{}

	metrics.RonDBConnectionState =
		*prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: protocol + "_connection_state",
				Help: "Connection state (0: disconnected, 1: connected)",
			}, []string{"connection"})

	metrics.NdbObjectsTotalCount =
		*prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: protocol + "_total_ndb_objects",
				Help: "Total NDB objects",
			}, []string{"total_ndb_objects"})

	prometheus.MustRegister(metrics.RonDBConnectionState)
	prometheus.MustRegister(metrics.NdbObjectsTotalCount)
	cleanup := func() {
		prometheus.Unregister(metrics.RonDBConnectionState)
		prometheus.Unregister(metrics.NdbObjectsTotalCount)
	}
	return &metrics, cleanup
}

func newHTTPMetrics() (*HTTPMetrics, func()) {
	protocol := "rdrs_http"
	metrics := HTTPMetrics{}

	metrics.PingCounter =
		prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: protocol + "_ping_request_count",
				Help: "No of request handled by " + protocol + " ping handler",
			},
		)

	metrics.PingSummary =
		prometheus.NewSummary(
			prometheus.SummaryOpts{
				Name:       protocol + "_ping_request_summary",
				Help:       "Summary for ping " + protocol + " handler. Time is in nanoseconds",
				Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.95: 0.01, 0.99: 0.001},
			},
		)

	metrics.PkReadCounter =
		prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: protocol + "_pk_read_request_count",
				Help: "No of request handled by " + protocol + " pkread handler",
			},
		)

	metrics.PkReadSummary =
		prometheus.NewSummary(
			prometheus.SummaryOpts{
				Name:       protocol + "_pk_read_request_summary",
				Help:       "Summary for pk read " + protocol + " handler. Time is in nanoseconds",
				Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.95: 0.01, 0.99: 0.001},
			},
		)

	metrics.BatchPkReadCounter =
		prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: protocol + "_pk_batch_read_request_count",
				Help: "No of request handled by " + protocol + " batchpk read handler",
			},
		)

	metrics.BatchPkReadSummary =
		prometheus.NewSummary(
			prometheus.SummaryOpts{
				Name:       protocol + "_pk_batch_read_request_summary",
				Help:       "Summary for pk batch read " + protocol + " handler. Time is in nanoseconds",
				Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.95: 0.01, 0.99: 0.001},
			},
		)

	metrics.StatCounter =
		prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: protocol + "_stat_request_count",
				Help: "No of request handled by " + protocol + " stat handler",
			},
		)

	metrics.StatSummary =
		prometheus.NewSummary(
			prometheus.SummaryOpts{
				Name:       protocol + "_stat_request_summary",
				Help:       "Summary for stat " + protocol + " handler",
				Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.95: 0.01, 0.99: 0.001},
			},
		)

	metrics.HttpConnectionGauge = HttpConnectionGauge{}
	metrics.HttpConnectionGauge.ConnectionGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: protocol + "_connection_count",
			Help: "No of open " + protocol + " connections",
		},
	)

	prometheus.MustRegister(metrics.PingCounter)
	prometheus.MustRegister(metrics.PingSummary)
	prometheus.MustRegister(metrics.PkReadCounter)
	prometheus.MustRegister(metrics.PkReadSummary)
	prometheus.MustRegister(metrics.BatchPkReadCounter)
	prometheus.MustRegister(metrics.BatchPkReadSummary)
	prometheus.MustRegister(metrics.StatCounter)
	prometheus.MustRegister(metrics.StatSummary)
	prometheus.MustRegister(metrics.HttpConnectionGauge.ConnectionGauge)

	cleanup := func() {
		prometheus.Unregister(metrics.PingCounter)
		prometheus.Unregister(metrics.PingSummary)
		prometheus.Unregister(metrics.PkReadCounter)
		prometheus.Unregister(metrics.PkReadSummary)
		prometheus.Unregister(metrics.BatchPkReadCounter)
		prometheus.Unregister(metrics.BatchPkReadSummary)
		prometheus.Unregister(metrics.StatCounter)
		prometheus.Unregister(metrics.StatSummary)
		prometheus.Unregister(metrics.HttpConnectionGauge.ConnectionGauge)
	}
	return &metrics, cleanup
}

func newGRPCMetrics() (*GRPCMetrics, func()) {
	protocol := "rdrs_grpc"
	metrics := GRPCMetrics{}

	metrics.PingCounter =
		prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: protocol + "_ping_request_count",
				Help: "No of request handled by " + protocol + " ping handler",
			},
		)

	metrics.PingSummary =
		prometheus.NewSummary(
			prometheus.SummaryOpts{
				Name:       protocol + "_ping_request_summary",
				Help:       "Summary for ping " + protocol + " handler. Time is in nanoseconds",
				Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.95: 0.01, 0.99: 0.001},
			},
		)

	metrics.PkReadCounter =
		prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: protocol + "_pk_read_request_count",
				Help: "No of request handled by " + protocol + " pkread handler",
			},
		)

	metrics.PkReadSummary =
		prometheus.NewSummary(
			prometheus.SummaryOpts{
				Name:       protocol + "_pk_read_request_summary",
				Help:       "Summary for pk read " + protocol + " handler. Time is in nanoseconds",
				Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.95: 0.01, 0.99: 0.001},
			},
		)

	metrics.BatchPkReadCounter =
		prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: protocol + "_pk_batch_read_request_count",
				Help: "No of request handled by " + protocol + " batchpk read handler",
			},
		)

	metrics.BatchPkReadSummary =
		prometheus.NewSummary(
			prometheus.SummaryOpts{
				Name:       protocol + "_pk_batch_read_request_summary",
				Help:       "Summary for pk batch read " + protocol + " handler. Time is in nanoseconds",
				Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.95: 0.01, 0.99: 0.001},
			},
		)

	metrics.StatCounter =
		prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: protocol + "_stat_request_count",
				Help: "No of request handled by " + protocol + " stat handler",
			},
		)

	metrics.StatSummary =
		prometheus.NewSummary(
			prometheus.SummaryOpts{
				Name:       protocol + "_stat_request_summary",
				Help:       "Summary for stat " + protocol + " handler. Time is in nanoseconds",
				Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.95: 0.01, 0.99: 0.001},
			},
		)

	metrics.GRPCStatistics = GRPCStatistics{}
	metrics.GRPCStatistics.ConnectionGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: protocol + "_connection_count",
			Help: "No of open " + protocol + " connections",
		},
	)

	prometheus.MustRegister(metrics.PingCounter)
	prometheus.MustRegister(metrics.PingSummary)
	prometheus.MustRegister(metrics.PkReadCounter)
	prometheus.MustRegister(metrics.PkReadSummary)
	prometheus.MustRegister(metrics.BatchPkReadCounter)
	prometheus.MustRegister(metrics.BatchPkReadSummary)
	prometheus.MustRegister(metrics.StatCounter)
	prometheus.MustRegister(metrics.StatSummary)
	prometheus.MustRegister(metrics.GRPCStatistics.ConnectionGauge)

	cleanup := func() {
		prometheus.Unregister(metrics.PingCounter)
		prometheus.Unregister(metrics.PingSummary)
		prometheus.Unregister(metrics.PkReadCounter)
		prometheus.Unregister(metrics.PkReadSummary)
		prometheus.Unregister(metrics.BatchPkReadCounter)
		prometheus.Unregister(metrics.BatchPkReadSummary)
		prometheus.Unregister(metrics.StatCounter)
		prometheus.Unregister(metrics.StatSummary)
		prometheus.Unregister(metrics.GRPCStatistics.ConnectionGauge)
	}
	return &metrics, cleanup
}
