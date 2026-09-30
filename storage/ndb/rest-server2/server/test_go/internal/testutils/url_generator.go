/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2023, 2025 Hopsworks AB
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

package testutils

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/version"
)

// ConnectHost returns a host a CLIENT can dial. conf.REST.ServerIP is the
// server's BIND address; the wildcard addresses are not reliably dialable
// (macOS intermittently fails concurrent connects to 0.0.0.0 with
// EADDRNOTAVAIL), so map them to loopback.
func ConnectHost(bindAddr string) string {
	if bindAddr == "0.0.0.0" {
		return "127.0.0.1"
	}
	if bindAddr == "::" {
		// IPv6 wildcard: an IPv6-only listener is not reachable on
		// 127.0.0.1, so dial the IPv6 loopback.
		return "::1"
	}
	return bindAddr
}

// hostPort builds a URL authority from a bind address and port, bracketing
// IPv6 literals (net.JoinHostPort) so "::1" yields "[::1]:4406" and not the
// invalid "::1:4406". ConnectHost stays unbracketed for raw-dial callers.
func hostPort(bindAddr string, port uint16) string {
	return net.JoinHostPort(ConnectHost(bindAddr), strconv.Itoa(int(port)))
}

func NewPingURL() string {
	conf := config.GetAll()
	url := fmt.Sprintf("%s/%s/%s",
		hostPort(conf.REST.ServerIP, conf.REST.ServerPort),
		version.API_VERSION,
		config.PING_OPERATION,
	)
	appendURLProtocol(&url)
	return url
}

func NewStatURL() string {
	conf := config.GetAll()
	url := fmt.Sprintf("%s/%s/%s",
		hostPort(conf.REST.ServerIP, conf.REST.ServerPort),
		version.API_VERSION,
		config.STAT_OPERATION,
	)
	appendURLProtocol(&url)
	return url
}

func NewHealthURL() string {
	conf := config.GetAll()
	url := fmt.Sprintf("%s/%s/%s",
		hostPort(conf.REST.ServerIP, conf.REST.ServerPort),
		version.API_VERSION,
		config.HEALTH_OPERATION,
	)
	appendURLProtocol(&url)
	return url
}

// The same two endpoints on the dedicated probe listener (REST.ProbePort).
// Same scheme as the main port: the probe listener mirrors its TLS setting.

func NewProbePingURL() string {
	conf := config.GetAll()
	url := fmt.Sprintf("%s/%s/%s",
		hostPort(conf.REST.ServerIP, conf.REST.ProbePort),
		version.API_VERSION,
		config.PING_OPERATION,
	)
	appendURLProtocol(&url)
	return url
}

func NewProbeHealthURL() string {
	conf := config.GetAll()
	url := fmt.Sprintf("%s/%s/%s",
		hostPort(conf.REST.ServerIP, conf.REST.ProbePort),
		version.API_VERSION,
		config.HEALTH_OPERATION,
	)
	appendURLProtocol(&url)
	return url
}

func NewRonSQLURL() string {
	conf := config.GetAll()
	url := fmt.Sprintf("%s/%s/%s",
		hostPort(conf.REST.ServerIP, conf.REST.ServerPort),
		version.API_VERSION,
		config.RONSQL_OPERATION,
	)
	appendURLProtocol(&url)
	return url
}

func NewPKReadURL(db string, table string) string {
	conf := config.GetAll()
	url := fmt.Sprintf("%s%s%s",
		hostPort(conf.REST.ServerIP, conf.REST.ServerPort),
		config.DB_OPS_EP_GROUP,
		config.PK_DB_OPERATION,
	)
	url = strings.Replace(url, ":"+config.DB_PP, db, 1)
	url = strings.Replace(url, ":"+config.TABLE_PP, table, 1)
	appendURLProtocol(&url)
	return url
}

func NewScanURL(db string, table string) string {
	conf := config.GetAll()
	url := fmt.Sprintf("%s%s%s",
		hostPort(conf.REST.ServerIP, conf.REST.ServerPort),
		config.DB_OPS_EP_GROUP,
		config.SCAN_OPERATION,
	)
	url = strings.Replace(url, ":"+config.DB_PP, db, 1)
	url = strings.Replace(url, ":"+config.TABLE_PP, table, 1)
	appendURLProtocol(&url)
	return url
}

func NewBatchPKReadURL(db string, table string) string {
	url := fmt.Sprintf("%s/%s/%s",
		db, table, config.PK_DB_OPERATION,
	)
	return url
}

func NewBatchPKReadURLVar2(db string, table string) string {
	url := fmt.Sprintf("/%s/%s/%s",
		db, table, config.PK_DB_OPERATION,
	)
	return url
}

func NewBatchPKReadURLVar3(db string, table string) string {
	url := fmt.Sprintf("%s/%s/%s/",
		db, table, config.PK_DB_OPERATION,
	)
	return url
}

func NewBatchPKReadURLVar4(db string, table string) string {
	url := fmt.Sprintf("/%s/%s/%s/",
		db, table, config.PK_DB_OPERATION,
	)
	return url
}

func NewBatchPKReadURLVar5(db string, table string) string {
	url := fmt.Sprintf("////%s/%s/%s///",
		db, table, config.PK_DB_OPERATION,
	)
	return url
}

func NewBatchPKReadURLVar6() string {
	url := fmt.Sprintf("//////////////")
	return url
}

func NewBatchPKReadURLVar7(db string, table string) string {
	url := fmt.Sprintf("////%s/%s%s///",
		db, table, config.PK_DB_OPERATION,
	)
	return url
}

func NewBatchPKReadURLVar8(db string, table string) string {
	url := fmt.Sprintf("////%s%s/%s///",
		db, table, config.PK_DB_OPERATION,
	)
	return url
}

func NewBatchReadURL() string {
	conf := config.GetAll()
	url := fmt.Sprintf("%s/%s/%s",
		hostPort(conf.REST.ServerIP, conf.REST.ServerPort),
		version.API_VERSION,
		config.BATCH_OPERATION,
	)
	appendURLProtocol(&url)
	return url
}

func NewFeatureStoreURL() string {
	conf := config.GetAll()
	url := fmt.Sprintf("%s/%s/%s",
		hostPort(conf.REST.ServerIP, conf.REST.ServerPort),
		version.API_VERSION,
		config.FEATURE_STORE_OPERATION,
	)
	appendURLProtocol(&url)
	return url
}

func NewBatchFeatureStoreURL() string {
	conf := config.GetAll()
	url := fmt.Sprintf("%s/%s/%s",
		hostPort(conf.REST.ServerIP, conf.REST.ServerPort),
		version.API_VERSION,
		config.BATCH_FEATURE_STORE_OPERATION,
	)
	appendURLProtocol(&url)
	return url
}

func appendURLProtocol(url *string) {
	conf := config.GetAll()
	if conf.Security.TLS.EnableTLS {
		*url = fmt.Sprintf("https://%s", *url)
	} else {
		*url = fmt.Sprintf("http://%s", *url)
	}
}
