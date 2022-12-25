package testutils

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

	"hopsworks.ai/rdrs/internal/config"
)

var ipAddresses []net.IP
var dnsNames []string

func init() {
	ipAddresses = []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback}
	dnsNames = []string{"localhost"}
}

func CreateAllTLSCerts() (tlsCtx TlsContext, cleanup func(), err error) {
	cleanup = func() {}

	certsDir := filepath.Join(os.TempDir(), "certs-for-unit-testing")
	err = os.RemoveAll(certsDir) // In case it has been already created
	if err != nil {
		return
	}

	initialRootCAFile := config.Configuration().Security.RootCACertFile
	initialCertFile := config.Configuration().Security.CertificateFile
	initialPrivateKeyFile := config.Configuration().Security.PrivateKeyFile

	cleanup = func() {
		os.RemoveAll(certsDir)
		// TODO: We should not be overwriting this in the first place
		config.Configuration().Security.RootCACertFile = initialRootCAFile
		config.Configuration().Security.CertificateFile = initialCertFile
		config.Configuration().Security.PrivateKeyFile = initialPrivateKeyFile
	}

	rootCACertFile, rootCAKeyFile, err := createRootCA(certsDir)
	if err != nil {
		return
	}

	tlsCtx.RootCACertFile = rootCACertFile
	tlsCtx.RootCAKeyFile = rootCAKeyFile
	config.Configuration().Security.RootCACertFile = rootCACertFile

	serverCertFile, serverKeyFile, err := createServerCerts(certsDir, rootCACertFile, rootCAKeyFile)
	if err != nil {
		return
	}
	config.Configuration().Security.CertificateFile = serverCertFile
	config.Configuration().Security.PrivateKeyFile = serverKeyFile

	if config.Configuration().Security.RequireAndVerifyClientCert {
		var clientCertFilepath, clientKeyFilepath string
		clientCertFilepath, clientKeyFilepath, err = createClientCerts(certsDir, rootCACertFile, rootCAKeyFile)
		if err != nil {
			return
		}
		tlsCtx.ClientCertFile = clientCertFilepath
		tlsCtx.ClientKeyFile = clientKeyFilepath
	}

	return
}

func createRootCA(certificateDir string) (rootCACertFilepath string, rootCAKeyFilepath string, err error) {
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
	rootCAKeyPair, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return
	}

	rootCert, err := x509.CreateCertificate(
		rand.Reader,
		rootCATemplate,
		rootCATemplate,
		&rootCAKeyPair.PublicKey,
		rootCAKeyPair,
	)
	if err != nil {
		return
	}

	// pem encode
	rootCACertPem := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: rootCert,
	})

	rootCAKeyPairBytes, err := x509.MarshalPKCS8PrivateKey(rootCAKeyPair)
	if err != nil {
		return
	}
	rootCAKeyPairPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: rootCAKeyPairBytes,
	})

	if err = os.MkdirAll(certificateDir, 0755); err != nil {
		return
	}
	rootCACertFilepath = filepath.Join(certificateDir, "root-cert.pem")
	rootCAKeyFilepath = filepath.Join(certificateDir, "root-key.pem")

	if err = os.WriteFile(rootCAKeyFilepath, rootCAKeyPairPEM, 0600); err != nil {
		return
	}

	err = os.WriteFile(rootCACertFilepath, rootCACertPem, 0600)
	return
}

func createServerCerts(certsDir string, rootCaCertFile, rootCaKeyFile string) (
	serverCertFilepath string, serverKeyFilepath string, err error) {
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
		return
	}
	key, err := readCAKey(rootCaKeyFile)
	if err != nil {
		return
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
		return
	}

	serverKeyPairPEM := new(bytes.Buffer)
	pem.Encode(serverKeyPairPEM, &pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: serverKeyPairBytes,
	})

	if err = os.MkdirAll(certsDir, 0755); err != nil {
		return
	}
	serverCertFilepath = filepath.Join(certsDir, "server-cert.pem")
	serverKeyFilepath = filepath.Join(certsDir, "server-key.pem")

	if err = os.WriteFile(serverKeyFilepath, serverKeyPairPEM.Bytes(), 0600); err != nil {
		return
	}

	os.WriteFile(serverCertFilepath, serverCertPem.Bytes(), 0600)
	return
}

func createClientCerts(certsDir string, rootCaCertFile, rootCaKeyFile string) (
	clientCertFilepath string, clientKeyFilepath string, err error) {

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
		return
	}
	key, err := readCAKey(rootCaKeyFile)
	if err != nil {
		return
	}
	clientCert, _ := x509.CreateCertificate(
		rand.Reader,
		clientCertTemplate,
		cert,
		&clientKeyPair.PublicKey,
		key,
	)

	// pem encode
	clientCertPem := new(bytes.Buffer)
	err = pem.Encode(clientCertPem, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: clientCert,
	})
	if err != nil {
		return
	}

	clientKeyPairBytes, err := x509.MarshalPKCS8PrivateKey(clientKeyPair)
	if err != nil {
		return
	}

	clientKeyPairPEM := new(bytes.Buffer)
	err = pem.Encode(clientKeyPairPEM, &pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: clientKeyPairBytes,
	})
	if err != nil {
		return
	}

	if err = os.MkdirAll(certsDir, 0755); err != nil {
		return
	}
	clientCertFilepath = filepath.Join(certsDir, "client-cert.pem")
	clientKeyFilepath = filepath.Join(certsDir, "client-key.pem")

	if err = os.WriteFile(clientKeyFilepath, clientKeyPairPEM.Bytes(), 0600); err != nil {
		return
	}

	err = os.WriteFile(clientCertFilepath, clientCertPem.Bytes(), 0600)
	return
}

func readCACert(rootCACertFile string) (*x509.Certificate, error) {
	bytes, err := ioutil.ReadFile(rootCACertFile)
	if err != nil {
		return nil, fmt.Errorf("Failed read certificate %q. Error: %w", rootCACertFile, err)
	}

	block, _ := pem.Decode(bytes)
	rootCACert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse cert %w", err)
	}

	return rootCACert, nil
}

func readCAKey(rootCAKeyFile string) (interface{}, error) {
	bytes, err := ioutil.ReadFile(rootCAKeyFile)
	if err != nil {
		return nil, fmt.Errorf("Failed read key %q. Error: %w", rootCAKeyFile, err)
	}

	block, _ := pem.Decode(bytes)
	rootCAKey, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse cert. Error: %w", err)
	}
	return rootCAKey, nil
}
