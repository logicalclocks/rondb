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

package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/dal"
	"hopsworks.ai/rdrs/internal/handlers"
	"hopsworks.ai/rdrs/internal/log"
	"hopsworks.ai/rdrs/internal/security/apikey/authcache"
	"hopsworks.ai/rdrs/internal/security/tlsutils"
	"hopsworks.ai/rdrs/internal/server/grpcsrv"
	"hopsworks.ai/rdrs/pkg/api"
	"hopsworks.ai/rdrs/version"
	// _ "github.com/ianlancetaylor/cgosymbolizer" // enable this for stack trace of c layer
)

type Router interface {
	SetupRouter(handlers *handlers.AllHandlers) error
	StartRouter() error
	StopRouter() error
	GetServer() (*http.Server, *grpc.Server)
}

type RouterConext struct {
	// REST Server
	RESTServerIP   string
	RESTServerPort uint16
	GRPCServerIP   string
	GRPCServerPort uint16
	APIVersion     string
	Engine         *gin.Engine

	// RonDB
	DBIP   string
	DBPort uint16

	//server
	HttpServer *http.Server
	GRPCServer *grpc.Server

	//Handlers
	handlers *handlers.AllHandlers
}

var _ Router = (*RouterConext)(nil)

func (rc *RouterConext) SetupRouter(handlers *handlers.AllHandlers) error {
	gin.SetMode(gin.ReleaseMode)
	rc.Engine = gin.New()

	rc.registerHandlers(handlers)

	// connect to RonDB
	dal.InitializeBuffers()
	err := dal.InitRonDBConnection(fmt.Sprintf("%s:%d", rc.DBIP, rc.DBPort), true)
	if err != nil {
		return err
	}

	address := fmt.Sprintf("%s:%d", rc.RESTServerIP, rc.RESTServerPort)
	rc.HttpServer = &http.Server{
		Addr:    address,
		Handler: rc.Engine,
	}

	return nil
}

func (rc *RouterConext) registerHandlers(handlers *handlers.AllHandlers) error {
	// register handlers
	// pk
	if handlers.PKReader != nil {
		group := rc.Engine.Group(config.DB_OPS_EP_GROUP)
		group.POST(config.PK_DB_OPERATION, handlers.PKReader.PkReadHttpHandler)
		log.Infof("REST Server registering POST %s%s", config.DB_OPS_EP_GROUP, config.PK_DB_OPERATION)
	}

	// batch
	if handlers.Batcher != nil {
		uri := "/" + version.API_VERSION + "/" + config.BATCH_OPERATION
		rc.Engine.POST(uri, handlers.Batcher.BatchOpsHttpHandler)
		log.Infof("REST Server registering POST %s", uri)
	}

	// stat
	if handlers.Stater != nil {
		uri := "/" + version.API_VERSION + "/" + config.STAT_OPERATION
		rc.Engine.GET(uri, handlers.Stater.StatOpsHttpHandler)
		log.Infof("REST Server registering GET %s", uri)
	}

	// GRPC
	grpcsrv.GetGRPCServer().RegisterAllHandlers(handlers)

	rc.handlers = handlers
	return nil
}

func (rc *RouterConext) StartRouter() error {

	log.Infof("REST Server Listening on %s:%d, GRPC Server Listening on %s:%d ",
		rc.RESTServerIP, rc.RESTServerPort, rc.GRPCServerIP, rc.GRPCServerPort)

	var serverTLS *tls.Config
	var err error
	conf := config.GetAll()

	if conf.Security.EnableTLS {
		serverTLS, err = serverTLSConfig()
		if err != nil {
			return fmt.Errorf("Unable to set server TLS config. Error %v", err)
		}
	}

	httpListener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", rc.RESTServerIP, rc.RESTServerPort))
	if err != nil {
		log.Fatalf("HTTP server error in listener. Error: %v", err)
	}

	grpcListener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", rc.GRPCServerIP, rc.GRPCServerPort))
	if err != nil {
		log.Fatalf("GRPC server error in listener. Error: %v", err)
	}

	go func() { // Start REST Server
		if conf.Security.EnableTLS {
			rc.HttpServer.TLSConfig = serverTLS
			err = rc.HttpServer.ServeTLS(httpListener, conf.Security.CertificateFile,
				conf.Security.PrivateKeyFile)
		} else {
			err = rc.HttpServer.Serve(httpListener)
		}
		if err != nil {
			log.Infof("HTTP server error in serve. Error: %v", err)
		}
	}()

	go func() { // Start GRPC Server
		if conf.Security.EnableTLS {
			rc.GRPCServer = grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLS)))
		} else {
			rc.GRPCServer = grpc.NewServer()
		}
		GRPCServer := grpcsrv.GetGRPCServer()
		api.RegisterRonDBRESTServer(rc.GRPCServer, GRPCServer)
		rc.GRPCServer.Serve(grpcListener)
	}()

	return nil
}

func serverTLSConfig() (*tls.Config, error) {
	tlsConfig := &tls.Config{
		MinVersion:               tls.VersionTLS13,
		PreferServerCipherSuites: true,
	}

	conf := config.GetAll()

	if conf.Security.RequireAndVerifyClientCert {
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	} else {
		tlsConfig.ClientAuth = tls.NoClientCert
	}

	if conf.Security.RootCACertFile != "" {
		tlsConfig.ClientCAs = tlsutils.TrustedCAs(conf.Security.RootCACertFile)
	}

	serverCert, err := tls.LoadX509KeyPair(conf.Security.CertificateFile,
		conf.Security.PrivateKeyFile)
	if err != nil {
		return nil, err
	}
	tlsConfig.Certificates = []tls.Certificate{serverCert}

	// tlsConfig.BuildNameToCertificate()
	return tlsConfig, nil
}

func (rc *RouterConext) StopRouter() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Stop REST Server
	if err := rc.HttpServer.Shutdown(ctx); err != nil {
		log.Errorf("Server forced to shutdown: %v", err)
	}

	// Stop GRPC Server
	rc.GRPCServer.Stop()

	// Stop RonDB Connection
	dalErr := dal.ShutdownConnection()
	dal.ReleaseAllBuffers()

	if dalErr != nil {
		log.Errorf("Failed to stop RonDB API. Error %v", dalErr)
	}

	// Clean API Key Cache
	authcache.Reset()

	return nil
}

func CreateRouterContext() Router {
	conf := config.GetAll()
	router := RouterConext{
		RESTServerIP:   conf.REST.ServerIP,
		RESTServerPort: conf.REST.ServerPort,

		GRPCServerIP:   conf.GRPC.ServerIP,
		GRPCServerPort: conf.GRPC.ServerPort,

		APIVersion: version.API_VERSION,

		DBIP:   conf.RonDB.Mgmds[0].IP,
		DBPort: conf.RonDB.Mgmds[0].Port,

		HttpServer: &http.Server{},
	}
	return &router
}

func (rc *RouterConext) GetServer() (*http.Server, *grpc.Server) {
	return rc.HttpServer, rc.GRPCServer
}
