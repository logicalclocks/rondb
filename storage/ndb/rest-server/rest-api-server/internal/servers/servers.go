package servers

import (
	"crypto/tls"
	"fmt"
	"os"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/dal"
	"hopsworks.ai/rdrs/internal/dal/heap"
	"hopsworks.ai/rdrs/internal/log"
	"hopsworks.ai/rdrs/internal/security/authcache"
	"hopsworks.ai/rdrs/internal/security/tlsutils"
	"hopsworks.ai/rdrs/internal/servers/grpc"
	"hopsworks.ai/rdrs/internal/servers/rest"
)

func CreateAndStartDefaultServers(heap *heap.Heap, quit chan os.Signal) (err error, cleanup func()) {
	cleanup = func() {}

	// Connect to RonDB
	conf := config.GetAll()
	connectString := fmt.Sprintf("%s:%d", conf.REST.ServerIP, conf.REST.ServerPort)
	err = dal.InitRonDBConnection(connectString, true)
	if err != nil {
		return
	}
	actualCleanup := func() {
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

	grpcServer := grpc.New(tlsConfig, heap)
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
		heap,
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
