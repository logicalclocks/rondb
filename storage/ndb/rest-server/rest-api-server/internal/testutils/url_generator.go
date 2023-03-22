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

package testutils

import (
	"fmt"
	"strings"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/version"
)

func NewPingURL() string {
	conf := config.GetAll()
	url := fmt.Sprintf("%s:%d/%s/%s",
		conf.REST.ServerIP,
		conf.REST.ServerPort,
		version.API_VERSION,
		config.PING_OPERATION,
	)
	appendURLProtocol(&url)
	return url
}

func NewStatURL() string {
	conf := config.GetAll()
	url := fmt.Sprintf("%s:%d/%s/%s",
		conf.REST.ServerIP,
		conf.REST.ServerPort,
		version.API_VERSION,
		config.STAT_OPERATION,
	)
	appendURLProtocol(&url)
	return url
}

func NewPKReadURL(db string, table string) string {
	conf := config.GetAll()
	url := fmt.Sprintf("%s:%d%s%s",
		conf.REST.ServerIP,
		conf.REST.ServerPort,
		config.DB_OPS_EP_GROUP,
		config.PK_DB_OPERATION,
	)
	url = strings.Replace(url, ":"+config.DB_PP, db, 1)
	url = strings.Replace(url, ":"+config.TABLE_PP, table, 1)
	appendURLProtocol(&url)
	return url
}

func NewBatchReadURL() string {
	conf := config.GetAll()
	url := fmt.Sprintf("%s:%d/%s/%s",
		conf.REST.ServerIP,
		conf.REST.ServerPort,
		version.API_VERSION,
		config.BATCH_OPERATION,
	)
	appendURLProtocol(&url)
	return url
}

func appendURLProtocol(url *string) {
	conf := config.GetAll()
	if conf.Security.EnableTLS {
		*url = fmt.Sprintf("https://%s", *url)
	} else {
		*url = fmt.Sprintf("http://%s", *url)
	}
}
