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

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/security/tlsutils"
)

func GetClientTLSConfig() (*tls.Config, error) {
	conf := config.GetAll()

	clientTLSConfig := &tls.Config{}
	if conf.Security.TLS.RootCACertFile != "" {
		clientTLSConfig.RootCAs = tlsutils.TrustedCAs(conf.Security.TLS.RootCACertFile)
	}

	if conf.Security.TLS.RequireAndVerifyClientCert {
		clientCert, err := tls.LoadX509KeyPair(
			conf.Security.TLS.TestParameters.ClientCertFile,
			conf.Security.TLS.TestParameters.ClientKeyFile,
		)
		if err != nil {
			return nil, err
		}
		clientTLSConfig.Certificates = []tls.Certificate{clientCert}
	}
	return clientTLSConfig, nil
}
