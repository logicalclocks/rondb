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
package tlsutils

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
)

func TrustedCAs(rootCACertFile string) *x509.CertPool {
	rootCAs, _ := x509.SystemCertPool()
	if rootCAs == nil {
		rootCAs = x509.NewCertPool()
	}

	err := appendCertToPool(rootCACertFile, rootCAs)
	if err != nil {
		panic(err)
	}
	return rootCAs
}

func appendCertToPool(certFile string, pool *x509.CertPool) error {
	certs, err := os.ReadFile(certFile)
	if err != nil {
		return fmt.Errorf("failed to append %q to RootCAs: %v", certFile, err)
	}

	// Append our cert to the system pool
	if ok := pool.AppendCertsFromPEM(certs); !ok {
		return errors.New("no certs appended, using system certs only")
	}
	return nil
}

func GenerateTLSConfig(
	requireClientCert bool,
	rootCACertfile, certfile, privateKeyFile string,
) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		MinVersion:               tls.VersionTLS13,
		PreferServerCipherSuites: true,
	}

	if requireClientCert {
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	} else {
		tlsConfig.ClientAuth = tls.NoClientCert
	}

	if rootCACertfile != "" {
		tlsConfig.ClientCAs = TrustedCAs(rootCACertfile)
	}

	serverCert, err := tls.LoadX509KeyPair(certfile, privateKeyFile)
	if err != nil {
		return nil, err
	}
	tlsConfig.Certificates = []tls.Certificate{serverCert}

	// tlsConfig.BuildNameToCertificate()
	return tlsConfig, nil
}
