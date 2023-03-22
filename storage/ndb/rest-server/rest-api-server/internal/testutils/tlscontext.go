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
	"crypto/tls"
	"testing"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/security/tlsutils"
)

func GetClientTLSConfig(t testing.TB) *tls.Config {
	t.Helper()
	conf := config.GetAll()

	clientTLSConfig := &tls.Config{}
	if conf.Security.RootCACertFile != "" {
		clientTLSConfig.RootCAs = tlsutils.TrustedCAs(conf.Security.RootCACertFile)
	}

	if conf.Security.RequireAndVerifyClientCert {
		clientCert, err := tls.LoadX509KeyPair(
			conf.Security.TestParameters.ClientCertFile,
			conf.Security.TestParameters.ClientKeyFile,
		)
		if err != nil {
			t.Fatal(err)
		}
		clientTLSConfig.Certificates = []tls.Certificate{clientCert}
	}
	return clientTLSConfig
}
