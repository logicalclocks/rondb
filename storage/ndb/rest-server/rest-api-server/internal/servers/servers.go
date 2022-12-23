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

	// Connect to RonDB
	dal.InitializeBuffers()
	dbIp := config.Configuration().RonDBConfig.IP
	dbPort := config.Configuration().RonDBConfig.Port
	err = dal.InitRonDBConnection(fmt.Sprintf("%s:%d", dbIp, dbPort), true)
	if err != nil {
		return
	}
	cleanup = func() {
		dalErr := dal.ShutdownConnection()
		if dalErr != nil {
			log.Error(dalErr.Error())
		}
		dal.ReleaseAllBuffers()
	}

	var tlsConfig *tls.Config
	if config.Configuration().Security.EnableTLS {
		tlsConfig, err = tlsutils.GenerateTLSConfig(
			config.Configuration().Security.RequireAndVerifyClientCert,
			config.Configuration().Security.RootCACertFile,
			config.Configuration().Security.CertificateFile,
			config.Configuration().Security.PrivateKeyFile,
		)
		if err != nil {
			return
		}
	}

	grpcServer := grpc.New(tlsConfig)
	grpcIp := config.Configuration().RestServer.GRPCServerIP
	grpcPort := config.Configuration().RestServer.GRPCServerPort
	err, cleanupGrpc := grpc.Start(grpcServer, grpcIp, grpcPort, quit)
	if err != nil {
		return
	}
	cleanup = func() {
		cleanup()
		cleanupGrpc()
	}

	restIp := config.Configuration().RestServer.RESTServerIP
	restPort := config.Configuration().RestServer.RESTServerPort
	restServer := rest.New(restIp, restPort, tlsConfig)
	cleanupRest := restServer.Start(quit)
	cleanup = func() {
		cleanup()
		cleanupRest()

		// Clean API Key Cache
		authcache.Reset()
	}
	return
}
