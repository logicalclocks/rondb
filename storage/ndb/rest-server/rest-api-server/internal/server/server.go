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
	"errors"
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
	"hopsworks.ai/rdrs/internal/security/apikey"
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

type RouterContext struct {
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

var _ Router = (*RouterContext)(nil)

func (rc *RouterContext) SetupRouter(handlers *handlers.AllHandlers) error {
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

func (rc *RouterContext) registerHandlers(handlers *handlers.AllHandlers) error {
	// register handlers
	// pk
	if handlers.PKReader != nil {
		group := rc.Engine.Group(config.DB_OPS_EP_GROUP)
		group.POST(config.PK_DB_OPERATION, handlers.PKReader.PkReadHttpHandler)
	}

	// batch
	if handlers.Batcher != nil {
		rc.Engine.POST("/"+version.API_VERSION+"/"+config.BATCH_OPERATION,
			handlers.Batcher.BatchOpsHttpHandler)
	}

	// stat
	if handlers.Stater != nil {
		rc.Engine.GET("/"+version.API_VERSION+"/"+config.STAT_OPERATION,
			handlers.Stater.StatOpsHttpHandler)
	}

	// GRPC
	grpcsrv.GetGRPCServer().RegisterAllHandlers(handlers)

	rc.handlers = handlers
	return nil
}

func (rc *RouterContext) StartRouter() error {

	restApiAddress := fmt.Sprintf("%s:%d", rc.RESTServerIP, rc.RESTServerPort)
	grpcAddress := fmt.Sprintf("%s:%d", rc.GRPCServerIP, rc.GRPCServerPort)
	log.Infof("REST Server Listening on %s, GRPC Server Listening on %s ",
		restApiAddress, grpcAddress)

	var serverTLS *tls.Config
	var err error

	if config.Configuration().Security.EnableTLS {
		if config.Configuration().Security.CertificateFile == "" ||
			config.Configuration().Security.PrivateKeyFile == "" {
			return errors.New("Server Certificate/Key not set")
		}

		serverTLS, err = serverTLSConfig()
		if err != nil {
			return fmt.Errorf("Unable to set server TLS config. Error: %w", err)
		}
	}

	httpListener, err := net.Listen("tcp", restApiAddress)
	if err != nil {
		log.Fatalf("Failed listening to REST server address '%s'. Error: %v", restApiAddress, err)
	}

	grpcListener, err := net.Listen("tcp", grpcAddress)
	if err != nil {
		log.Fatalf("Failed listening to GRPC server address '%s'. Error: %v", grpcAddress, err)
	}

	// TODO: Fail program if one of these two servers fails starting

	// Start REST Server
	go func() {
		if config.Configuration().Security.EnableTLS {
			rc.HttpServer.TLSConfig = serverTLS
			err = rc.HttpServer.ServeTLS(
				httpListener,
				config.Configuration().Security.CertificateFile,
				config.Configuration().Security.PrivateKeyFile,
			)
		} else {
			err = rc.HttpServer.ListenAndServe()
		}

		if errors.Is(err, http.ErrServerClosed) {
			log.Info("Http server closed")
		} else if err != nil {
			log.Errorf("Http failed serving. Error: %w", err)
		}
	}()

	// Start GRPC Server
	go func() {
		// TODO: Make credentials optional
		rc.GRPCServer = grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLS)))
		GRPCServer := grpcsrv.GetGRPCServer()
		api.RegisterRonDBRESTServer(rc.GRPCServer, GRPCServer)
		if err := rc.GRPCServer.Serve(grpcListener); err != nil {
			log.Errorf("failed to serve grpc. Error: %w", err)
		}
	}()

	return nil
}

func serverTLSConfig() (*tls.Config, error) {
	tlsConfig := &tls.Config{
		MinVersion:               tls.VersionTLS13,
		PreferServerCipherSuites: true,
	}

	if config.Configuration().Security.RequireAndVerifyClientCert {
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	} else {
		tlsConfig.ClientAuth = tls.NoClientCert
	}

	if config.Configuration().Security.RootCACertFile != "" {
		tlsConfig.ClientCAs = tlsutils.TrustedCAs(config.Configuration().Security.RootCACertFile)
	}

	serverCert, err := tls.LoadX509KeyPair(config.Configuration().Security.CertificateFile,
		config.Configuration().Security.PrivateKeyFile)
	if err != nil {
		return nil, err
	}
	tlsConfig.Certificates = []tls.Certificate{serverCert}

	// tlsConfig.BuildNameToCertificate()
	return tlsConfig, nil
}

func (rc *RouterContext) StopRouter() error {
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
		log.Errorf("Failed to stop RonDB API. Error: %v", dalErr)
	}

	// Clean API Key Cache
	apikey.Reset()

	return nil
}

func CreateRouterContext() Router {
	router := RouterContext{
		RESTServerIP:   config.Configuration().RestServer.RESTServerIP,
		RESTServerPort: config.Configuration().RestServer.RESTServerPort,

		GRPCServerIP:   config.Configuration().RestServer.GRPCServerIP,
		GRPCServerPort: config.Configuration().RestServer.GRPCServerPort,

		APIVersion: version.API_VERSION,

		DBIP:   config.Configuration().RonDBConfig.IP,
		DBPort: config.Configuration().RonDBConfig.Port,

		HttpServer: &http.Server{},
	}
	return &router
}

func (rc *RouterContext) GetServer() (*http.Server, *grpc.Server) {
	return rc.HttpServer, rc.GRPCServer
}
