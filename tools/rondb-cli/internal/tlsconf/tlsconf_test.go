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

package tlsconf

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeCA writes a self-signed CA certificate in PEM format to a temporary
// file and returns its path.
func writeCA(t *testing.T) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "rondb-cli test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "ca.pem")
	block := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(path, block, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestClient(t *testing.T) {
	defer func() { rootCAs = nil }()

	// Without --tls-ca, server certificates are not verified.
	cfg := Client()
	if !cfg.InsecureSkipVerify || cfg.RootCAs != nil || cfg.MinVersion != tls.VersionTLS12 {
		t.Fatalf("no CA file: got InsecureSkipVerify=%v RootCAs=%v MinVersion=%#x",
			cfg.InsecureSkipVerify, cfg.RootCAs, cfg.MinVersion)
	}

	// A missing file or a file without a PEM certificate is rejected and
	// leaves verification off.
	if err := SetCAFile(filepath.Join(t.TempDir(), "missing.pem")); err == nil {
		t.Fatal("missing CA file: want an error")
	}
	notPEM := filepath.Join(t.TempDir(), "not.pem")
	if err := os.WriteFile(notPEM, []byte("not a certificate\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := SetCAFile(notPEM); err == nil {
		t.Fatal("CA file without a PEM certificate: want an error")
	}
	if cfg := Client(); !cfg.InsecureSkipVerify || cfg.RootCAs != nil {
		t.Fatal("a rejected CA file turned verification on")
	}

	// With a CA file, server certificates are verified against it.
	if err := SetCAFile(writeCA(t)); err != nil {
		t.Fatal(err)
	}
	cfg = Client()
	if cfg.InsecureSkipVerify || cfg.RootCAs == nil || cfg.MinVersion != tls.VersionTLS12 {
		t.Fatalf("CA file: got InsecureSkipVerify=%v RootCAs=%v MinVersion=%#x",
			cfg.InsecureSkipVerify, cfg.RootCAs, cfg.MinVersion)
	}
}
