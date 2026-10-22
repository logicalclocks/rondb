/*
   Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

// Package tlsconf builds the TLS client configuration that every rondb-cli
// connection uses. Without a CA file (--tls-ca) the server certificate is
// not verified, so TLS only encrypts the traffic. With a CA file, the server
// certificate must be signed by one of its CAs and match the host name that
// rondb-cli connects to.
package tlsconf

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// rootCAs holds the CAs loaded by SetCAFile; nil means no verification.
var rootCAs *x509.CertPool

// SetCAFile loads the PEM CA certificates in caFile. Every configuration
// that Client returns afterwards verifies server certificates against them,
// so call it before opening any connection.
func SetCAFile(caFile string) error {
	pemData, err := os.ReadFile(caFile)
	if err != nil {
		return fmt.Errorf("failed to read TLS CA file: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemData) {
		return fmt.Errorf("no PEM certificate found in TLS CA file %s", caFile)
	}
	rootCAs = pool
	return nil
}

// Client returns a new TLS client configuration. The MySQL driver, net/http
// and go-redis take the server name to verify from the host they dial.
func Client() *tls.Config {
	if rootCAs == nil {
		return &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: true,
		}
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    rootCAs,
	}
}
