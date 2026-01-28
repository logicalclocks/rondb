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

	// For testing, we will connect to a machine, probably localhost, and during
	// handshake receive a server certificate that is not valid for that machine.
	// We could set `clientTLSConfig.InsecureSkipVerify = true` to accept the
	// certificate anyways, but this will accept any certificate unconditionally,
	// even if it's e.g. issued via the wrong CA. Instead, we override the
	// ServerName used for validation.
	clientTLSConfig.ServerName = "rdrs.service.consul"

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
