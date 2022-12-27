package servers

import (
	"crypto/tls"
	"fmt"
	"os"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/dal"
	"hopsworks.ai/rdrs/internal/log"
	"hopsworks.ai/rdrs/internal/security/authcache"
	"hopsworks.ai/rdrs/internal/security/tlsutils"
	"hopsworks.ai/rdrs/internal/servers/grpc"
	"hopsworks.ai/rdrs/internal/servers/rest"
)

func CreateAndStartDefaultServers(quit chan os.Signal) (err error, cleanup func()) {
	cleanup = func() {}

	err = dal.InitializeBuffers()
	if err != nil {
		return
	}
	actualCleanup := func() {
		err = dal.ReleaseAllBuffers()
		if err != nil {
			log.Error(err.Error())
		}
	}
	// Connect to RonDB
	conf := config.GetAll()
	connectString := fmt.Sprintf("%s:%d", conf.REST.ServerIP, conf.REST.ServerPort)
	err = dal.InitRonDBConnection(connectString, true)
	if err != nil {
		actualCleanup()
		return
	}
	actualCleanup = func() {
		actualCleanup()
		dalErr := dal.ShutdownConnection()
		if dalErr != nil {
			log.Error(dalErr.Error())
		}
	}

	var tlsConfig *tls.Config
	if conf.Security.EnableTLS {
		tlsConfig, err = tlsutils.GenerateTLSConfig(
			conf.Security.RequireAndVerifyClientCert,
			conf.Security.RootCACertFile,
			conf.Security.CertificateFile,
			conf.Security.PrivateKeyFile,
		)
		if err != nil {
			actualCleanup()
			return
		}
	}

	grpcServer := grpc.New(tlsConfig)
	err, cleanupGrpc := grpc.Start(
		grpcServer,
		conf.GRPC.ServerIP,
		conf.GRPC.ServerPort,
		quit,
	)
	if err != nil {
		actualCleanup()
		return
	}
	actualCleanup = func() {
		actualCleanup()
		cleanupGrpc()
	}

	restServer := rest.New(
		conf.REST.ServerIP,
		conf.REST.ServerPort,
		tlsConfig,
	)
	cleanupRest := restServer.Start(quit)
	actualCleanup = func() {
		actualCleanup()
		cleanupRest()

		// Clean API Key Cache
		authcache.Reset()
	}
	return nil, actualCleanup
}
