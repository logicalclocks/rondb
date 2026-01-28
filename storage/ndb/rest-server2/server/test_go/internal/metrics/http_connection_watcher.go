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
	"net"
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"hopsworks.ai/rdrs2/internal/log"
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
