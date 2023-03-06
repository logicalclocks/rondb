/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2022 Hopsworks AB
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
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"

	"hopsworks.ai/rdrs/internal/common"
	"hopsworks.ai/rdrs/internal/config"
)

var ipAddresses []net.IP
var dnsNames []string

func init() {
	ipAddresses = []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback}
	dnsNames = []string{"localhost"}
}

func rootCA(dir string) (string, string, error) {
	// Cert template
	serialNumber, _ := rand.Int(rand.Reader, big.NewInt(32))
	rootCATemplate := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Hopsworks AB Root CA"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(60 * time.Minute),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	// Public + Private key
	rootCAKeyPair, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)

	rootCert, _ := x509.CreateCertificate(rand.Reader, rootCATemplate,
		rootCATemplate, &rootCAKeyPair.PublicKey, rootCAKeyPair)

	// pem encode
	rootCACertPem := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: rootCert,
	})

	rootCAKeyPairBytes, err := x509.MarshalPKCS8PrivateKey(rootCAKeyPair)
	if err != nil {
		return "", "", err
	}
	rootCAKeyPairPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: rootCAKeyPairBytes,
	})

	os.Mkdir(dir, 0755)
	rootCACertFile := filepath.Join(dir, "root-cert.pem")
	rootCAKeyFile := filepath.Join(dir, "root-key.pem")

	if err := os.WriteFile(rootCAKeyFile, rootCAKeyPairPEM, 0600); err != nil {
		return "", "", err
	}

	if err := os.WriteFile(rootCACertFile, rootCACertPem, 0600); err != nil {
		return "", "", err
	}

	return rootCACertFile, rootCAKeyFile, nil
}

func serverCerts(certsDir string, rootCaCertFile, rootCaKeyFile string) (string, string, error) {
	// Cert template
	serialNumber, _ := rand.Int(rand.Reader, big.NewInt(32))
	serverCertTemplate := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Hopsworks AB RonDB Rest Server"},
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().Add(60 * time.Minute),
		IPAddresses: ipAddresses,
		DNSNames:    dnsNames,
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
	}

	// Public + Private key
	serverKeyPair, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)

	cert, err := readCACert(rootCaCertFile)
	if err != nil {
		return "", "", err
	}
	key, err := readCAKey(rootCaKeyFile)
	if err != nil {
		return "", "", nil
	}

	serverCert, _ := x509.CreateCertificate(rand.Reader, serverCertTemplate,
		cert, &serverKeyPair.PublicKey, key)

	// pem encode
	serverCertPem := new(bytes.Buffer)
	pem.Encode(serverCertPem, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: serverCert,
	})

	serverKeyPairBytes, err := x509.MarshalPKCS8PrivateKey(serverKeyPair)
	if err != nil {
		return "", "", err
	}

	serverKeyPairPEM := new(bytes.Buffer)
	pem.Encode(serverKeyPairPEM, &pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: serverKeyPairBytes,
	})

	os.Mkdir(certsDir, 0755)
	serverCertFile := filepath.Join(certsDir, "server-cert.pem")
	serverKeyFile := filepath.Join(certsDir, "server-key.pem")

	if err := os.WriteFile(serverKeyFile, serverKeyPairPEM.Bytes(), 0600); err != nil {
		return "", "", err
	}

	if err := os.WriteFile(serverCertFile, serverCertPem.Bytes(), 0600); err != nil {
		return "", "", err
	}

	return serverCertFile, serverKeyFile, nil
}

func clientCerts(certsDir string, rootCaCertFile, rootCaKeyFile string) (string, string, error) {

	// Cert template
	serialNumber, _ := rand.Int(rand.Reader, big.NewInt(32))
	clientCertTemplate := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Hopsworks AB RonDB Rest Server"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(60 * time.Minute),
		IPAddresses:           ipAddresses,
		DNSNames:              dnsNames,
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	// Public + Private key
	clientKeyPair, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)

	cert, err := readCACert(rootCaCertFile)
	if err != nil {
		return "", "", err
	}
	key, err := readCAKey(rootCaKeyFile)
	if err != nil {
		return "", "", nil
	}
	clientCert, _ := x509.CreateCertificate(rand.Reader, clientCertTemplate,
		cert, &clientKeyPair.PublicKey, key)

	// pem encode
	clientCertPem := new(bytes.Buffer)
	pem.Encode(clientCertPem, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: clientCert,
	})

	clientKeyPairBytes, err := x509.MarshalPKCS8PrivateKey(clientKeyPair)
	if err != nil {
		return "", "", nil
	}

	clientKeyPairPEM := new(bytes.Buffer)
	pem.Encode(clientKeyPairPEM, &pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: clientKeyPairBytes,
	})

	os.Mkdir(certsDir, 0755)
	clientCertFile := filepath.Join(certsDir, "client-cert.pem")
	clientKeyFile := filepath.Join(certsDir, "client-key.pem")

	if err := os.WriteFile(clientKeyFile, clientKeyPairPEM.Bytes(), 0600); err != nil {
		return "", "", nil
	}

	if err := os.WriteFile(clientCertFile, clientCertPem.Bytes(), 0600); err != nil {
		return "", "", nil
	}

	return clientCertFile, clientKeyFile, nil
}

func TrustedCAs(rootCACertFile string) *x509.CertPool {
	rootCAs, _ := x509.SystemCertPool()
	if rootCAs == nil {
		rootCAs = x509.NewCertPool()
	}

	appendCertToPool(rootCACertFile, rootCAs)
	return rootCAs
}

func appendCertToPool(certFile string, pool *x509.CertPool) error {
	certs, err := ioutil.ReadFile(certFile)
	if err != nil {
		return fmt.Errorf("Failed to append %q to RootCAs: %v", certFile, err)
	}

	// Append our cert to the system pool
	if ok := pool.AppendCertsFromPEM(certs); !ok {
		return fmt.Errorf("No certs appended, using system certs only")
	}
	return nil
}

func readCACert(rootCACertFile string) (*x509.Certificate, error) {
	bytes, err := ioutil.ReadFile(rootCACertFile)
	if err != nil {
		return nil, fmt.Errorf("Failed read certificate %q. Error: %v", rootCACertFile, err)
	}

	block, _ := pem.Decode(bytes)
	rootCACert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse cert %v", err)
	}

	return rootCACert, nil
}

func readCAKey(rootCAKeyFile string) (interface{}, error) {
	bytes, err := ioutil.ReadFile(rootCAKeyFile)
	if err != nil {
		return nil, fmt.Errorf("Failed read key %q. Error: %v", rootCAKeyFile, err)
	}

	block, _ := pem.Decode(bytes)
	rootCAKey, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse cert %v", err)
	}
	return rootCAKey, nil
}

func SetupCerts(tc *common.TestContext) error {
	certsDir := filepath.Join(os.TempDir(), "certs-for-unit-testing")
	rootCACertFile, rootCAKeyFile, err := rootCA(certsDir)
	if err != nil {
		return err
	}

	conf := config.GetAll()

	tc.RootCACertFile = rootCACertFile
	tc.RootCAKeyFile = rootCAKeyFile
	conf.Security.RootCACertFile = rootCACertFile

	serverCertFile, serverKeyFile, err := serverCerts(certsDir, rootCACertFile, rootCAKeyFile)
	if err != nil {
		return err
	}
	conf.Security.CertificateFile = serverCertFile
	conf.Security.PrivateKeyFile = serverKeyFile

	if conf.Security.RequireAndVerifyClientCert {
		clientCertFile, clientKeyFile, err := clientCerts(certsDir, rootCACertFile, rootCAKeyFile)
		if err != nil {
			return err
		}
		tc.ClientCertFile = clientCertFile
		tc.ClientKeyFile = clientKeyFile
	}

	return nil
}

func DeleteCerts(tc *common.TestContext) error {
	return os.RemoveAll(tc.CertsDir)
}
