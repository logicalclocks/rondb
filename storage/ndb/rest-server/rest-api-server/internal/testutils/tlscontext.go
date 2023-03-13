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
