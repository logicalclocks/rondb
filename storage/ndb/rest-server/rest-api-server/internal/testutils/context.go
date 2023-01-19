package testutils

import (
	"crypto/tls"
	"testing"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/security/tlsutils"
)

// These are some parameters needed for testing
type TlsContext struct {
	CertsDir       string
	RootCACertFile string
	RootCAKeyFile  string
	ClientCertFile string
	ClientKeyFile  string
}

func GetClientTLSConfig(t testing.TB, tlsCtx TlsContext) *tls.Config {
	clientTLSConfig := &tls.Config{}
	if tlsCtx.RootCACertFile != "" {
		clientTLSConfig.RootCAs = tlsutils.TrustedCAs(tlsCtx.RootCACertFile)
	}

	if config.Configuration().Security.RequireAndVerifyClientCert {
		clientCert, err := tls.LoadX509KeyPair(tlsCtx.ClientCertFile, tlsCtx.ClientKeyFile)
		if err != nil {
			t.Fatal(err.Error())
		}
		clientTLSConfig.Certificates = []tls.Certificate{clientCert}
	}
	return clientTLSConfig
}
