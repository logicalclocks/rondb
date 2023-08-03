/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2023 Hopsworks AB
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

package rest

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"os"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/dal/heap"
	fsmeta "hopsworks.ai/rdrs/internal/feature_store"
	"hopsworks.ai/rdrs/internal/handlers/batchfeaturestore"
	"hopsworks.ai/rdrs/internal/handlers/batchpkread"
	"hopsworks.ai/rdrs/internal/handlers/feature_store"
	"hopsworks.ai/rdrs/internal/handlers/pkread"
	"hopsworks.ai/rdrs/internal/handlers/stat"
	"hopsworks.ai/rdrs/internal/log"
	"hopsworks.ai/rdrs/internal/metrics"
	"hopsworks.ai/rdrs/internal/security/apikey"
)

type RonDBRestServer struct {
	// TODO: Add thread-safe logger
	server *http.Server
}

func New(
	host string,
	port uint16,
	tlsConfig *tls.Config,
	heap *heap.Heap,
	apiKeyCache apikey.Cache,
	rdrsMetrics *metrics.RDRSMetrics,
) *RonDBRestServer {
	restApiAddress := fmt.Sprintf("%s:%d", host, port)
	log.Infof("Initialising REST API server with network address: '%s'", restApiAddress)
	gin.SetMode(gin.ReleaseMode)
	router := gin.New() // gin.Default() for better logging
	registerHandlers(router, heap, apiKeyCache, rdrsMetrics)
	return &RonDBRestServer{
		server: &http.Server{
			Addr:      restApiAddress,
			Handler:   router,
			TLSConfig: tlsConfig,
			ConnState: rdrsMetrics.HTTPMetrics.HttpConnectionGauge.OnStateChange,
		},
	}
}

func (s *RonDBRestServer) Start(quit chan os.Signal) (cleanupFunc func()) {
	go func() {
		var err error
		conf := config.GetAll()
		if conf.Security.TLS.EnableTLS {
			err = s.server.ListenAndServeTLS(
				conf.Security.TLS.CertificateFile,
				conf.Security.TLS.PrivateKeyFile,
			)
		} else {
			err = s.server.ListenAndServe()
		}
		if errors.Is(err, http.ErrServerClosed) {
			log.Info("REST server closed")
		} else if err != nil {
			log.Errorf("REST server failed; error: %v", err)
			quit <- syscall.SIGINT
		}
	}()
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := s.server.Shutdown(ctx)
		if err != nil {
			log.Errorf("failed shutting down REST API server; error: %v", err)
		}
	}
}

type RouteHandler struct {
	// TODO: Add thread-safe logger
	statsHandler             stat.Handler
	pkReadHandler            pkread.Handler
	batchPkReadHandler       batchpkread.Handler
	rdrsMetrics              *metrics.RDRSMetrics
	featureStoreHandler      feature_store.Handler
	batchFeatureStoreHandler batchfeaturestore.Handler
}

func registerHandlers(router *gin.Engine, heap *heap.Heap, apiKeyCache apikey.Cache, rdrsMetrics *metrics.RDRSMetrics) {
	router.Use(ErrorHandler)
	router.Use(LogHandler(rdrsMetrics.HTTPMetrics))

	versionGroup := router.Group(config.VERSION_GROUP)

	batchPkReadHandler := batchpkread.New(heap, apiKeyCache)
	var fvMeta = fsmeta.NewFeatureViewMetaDataCache()
	featureStoreHandler := feature_store.New(fvMeta, apiKeyCache, batchPkReadHandler)

	routeHandler := &RouteHandler{
		statsHandler:             stat.New(heap, apiKeyCache),
		pkReadHandler:            pkread.New(heap, apiKeyCache),
		batchPkReadHandler:       batchPkReadHandler,
		rdrsMetrics:              rdrsMetrics,
		featureStoreHandler:      featureStoreHandler,
		batchFeatureStoreHandler: batchfeaturestore.New(fvMeta, apiKeyCache, batchPkReadHandler),
	}

	// ping
	versionGroup.GET("/"+config.PING_OPERATION, routeHandler.Ping)

	// stat
	versionGroup.GET("/"+config.STAT_OPERATION, routeHandler.Stat)

	// batch
	versionGroup.POST("/"+config.BATCH_OPERATION, routeHandler.BatchPkRead)

	// pk read
	tableSpecificGroup := versionGroup.Group(config.DB_TABLE_PP)
	tableSpecificGroup.POST(config.PK_DB_OPERATION, routeHandler.PkRead)

	// prometheus
	router.GET("/metrics", routeHandler.Metrics)

	// feature store
	versionGroup.POST("/"+config.FEATURE_STORE_OPERATION, routeHandler.FeatureStore)
	versionGroup.POST("/"+config.BATCH_FEATURE_STORE_OPERATION, routeHandler.BatchFeatureStore)

}

func LogHandler(m *metrics.HTTPMetrics) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now().UnixNano()
		c.Next()
		defer m.AddResponseTime(c.Request.RequestURI, c.Request.Method, float64(time.Now().UnixNano()-start))
		defer m.AddResponseStatus(c.Request.RequestURI, c.Request.Method, c.Writer.Status())
	}
}

// TODO: Pass logger to this like in https://stackoverflow.com/a/69948929/9068781
func ErrorHandler(c *gin.Context) {
	c.Next()

	if log.IsDebug() {
		for i, ginErr := range c.Errors {
			log.Debugf("GIN error nr %d: %s", i, ginErr.Error())
		}
	}

	if len(c.Errors) > 0 {
		// Just get the last error to the client
		// status -1 doesn't overwrite existing status code
		c.JSON(-1, c.Errors[len(c.Errors)-1].Error())
	}
}
