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
	"net"
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"hopsworks.ai/rdrs/internal/log"
)

type HttpConnectionGauge struct {
	lock            sync.Mutex
	ConnectionGauge prometheus.Gauge
}

// OnStateChange records open connections in response to connection
// state changes. Set net/http Server.ConnState to this method as value.
func (cw *HttpConnectionGauge) OnStateChange(conn net.Conn, state http.ConnState) {
	switch state {
	case http.StateNew:
		cw.Increment()
	case http.StateHijacked, http.StateClosed:
		cw.Decrement()
	}
}

func (cw *HttpConnectionGauge) Increment() {
	if log.IsDebug() {
		log.Debugf("New HTTP connection established")
	}

	cw.lock.Lock()
	defer cw.lock.Unlock()
	cw.ConnectionGauge.Inc()
}

func (cw *HttpConnectionGauge) Decrement() {
	if log.IsDebug() {
		log.Debugf("HTTP connection closed")
	}

	cw.lock.Lock()
	defer cw.lock.Unlock()
	cw.ConnectionGauge.Dec()
}
