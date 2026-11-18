/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2026 Hopsworks AB
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

// Package mtls checks Security.TLS.RequireAndVerifyClientCert: a REST server
// that requires client certificates serves a TLS client whose certificate
// was issued under Security.TLS.RootCACertFile, and refuses a client that
// presents no certificate or one from another CA.
//
// It is driven by mysql-test/suite/rdrs2-golang/t/mtls.test, which starts
// such a server, and self-skips unless RDRS_CONFIG_FILE_MTLS is set, so the
// normal pass over all packages does not run it.
package mtls

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/security/tlsutils"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/version"
)

// selfSignedCert returns a client certificate that no CA has issued.
func selfSignedCert(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("cannot generate a key: %v", err)
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "stranger"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template,
		&key.PublicKey, key)
	if err != nil {
		t.Fatalf("cannot create a certificate: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func TestRequireAndVerifyClientCert(t *testing.T) {
	cfgPath := os.Getenv("RDRS_CONFIG_FILE_MTLS")
	if cfgPath == "" {
		t.Skip("RDRS_CONFIG_FILE_MTLS not set (run via mtr rdrs2-golang.mtls)")
	}
	raw, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("cannot read %s: %v", cfgPath, err)
	}
	var conf config.AllConfigs
	if err := json.Unmarshal(raw, &conf); err != nil {
		t.Fatalf("cannot parse %s: %v", cfgPath, err)
	}
	tlsConf := conf.Security.TLS
	if !tlsConf.EnableTLS || !tlsConf.RequireAndVerifyClientCert ||
		tlsConf.RootCACertFile == "" {
		t.Fatal("the instance must have EnableTLS, " +
			"RequireAndVerifyClientCert and RootCACertFile set")
	}

	validCert, err := tls.LoadX509KeyPair(
		tlsConf.TestParameters.ClientCertFile,
		tlsConf.TestParameters.ClientKeyFile)
	if err != nil {
		t.Fatalf("cannot load the client certificate: %v", err)
	}
	strangerCert := selfSignedCert(t)

	url := fmt.Sprintf("https://%s/%s/%s",
		net.JoinHostPort(testutils.ConnectHost(conf.REST.ServerIP),
			strconv.Itoa(int(conf.REST.ServerPort))),
		version.API_VERSION, config.PING_OPERATION)
	rootCAs := tlsutils.TrustedCAs(tlsConf.RootCACertFile)

	// ping sends one GET /ping on a new connection. It presents clientCert,
	// or no certificate if clientCert is nil.
	ping := func(clientCert *tls.Certificate) (int, error) {
		client := &http.Client{
			Timeout: 10 * time.Second,
			Transport: &http.Transport{
				DisableKeepAlives: true,
				TLSClientConfig: &tls.Config{
					RootCAs: rootCAs,
					// The name testutils.GetClientTLSConfig uses: the
					// server certificate is not issued for the host we
					// dial.
					ServerName: "rdrs.service.consul",
					// A callback and not tls.Config.Certificates: with the
					// latter the client presents nothing when the
					// certificate does not match the CA names in the
					// server's request, so the server would never get to
					// verify the certificate of the unknown CA.
					GetClientCertificate: func(
						*tls.CertificateRequestInfo) (*tls.Certificate, error) {
						if clientCert == nil {
							return &tls.Certificate{}, nil
						}
						return clientCert, nil
					},
				},
			},
		}
		resp, err := client.Get(url)
		if err != nil {
			return 0, err
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		return resp.StatusCode, nil
	}

	// First the client that must be served. If the server is down or the
	// test certificates are wrong, this fails, and the refusals below cannot
	// pass for the wrong reason.
	status, err := ping(&validCert)
	if err != nil {
		t.Fatalf("a client certificate issued under RootCACertFile was "+
			"refused: %v", err)
	}
	if status != http.StatusOK {
		t.Fatalf("ping with a valid client certificate: status %d, want 200",
			status)
	}

	for _, tc := range []struct {
		name string
		cert *tls.Certificate
	}{
		{"no client certificate", nil},
		{"client certificate from an unknown CA", &strangerCert},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, err := ping(tc.cert)
			if err == nil {
				t.Fatalf("the request was served with status %d, it must "+
					"be refused during the TLS handshake", status)
			}
			t.Logf("refused: %v", err)
		})
	}
}
