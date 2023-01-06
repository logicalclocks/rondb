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
	"hopsworks.ai/rdrs/internal/handlers/batchpkread"
	"hopsworks.ai/rdrs/internal/handlers/pkread"
	"hopsworks.ai/rdrs/internal/handlers/stat"
	"hopsworks.ai/rdrs/internal/log"
)

type RonDBRestServer struct {
	// TODO: Add thread-safe logger
	server *http.Server
}

func New(host string, port uint16, tlsConfig *tls.Config, heap *heap.Heap) *RonDBRestServer {
	restApiAddress := fmt.Sprintf("%s:%d", host, port)
	log.Infof("Initialising REST API server; Network address: '%s'", restApiAddress)
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	registerHandlers(router, heap)
	return &RonDBRestServer{
		server: &http.Server{
			Addr:      restApiAddress,
			Handler:   router,
			TLSConfig: tlsConfig,
		},
	}
}

func (s *RonDBRestServer) Start(quit chan os.Signal) (cleanupFunc func()) {
	go func() {
		var err error
		conf := config.GetAll()
		if conf.Security.EnableTLS {
			err = s.server.ListenAndServeTLS(
				conf.Security.CertificateFile,
				conf.Security.PrivateKeyFile,
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
	statsHandler       stat.Handler
	pkReadHandler      pkread.Handler
	batchPkReadHandler batchpkread.Handler
}

func registerHandlers(router *gin.Engine, heap *heap.Heap) {
	versionGroup := router.Group(config.VERSION_GROUP)

	routeHandler := &RouteHandler{
		statsHandler:       stat.New(heap),
		pkReadHandler:      pkread.New(heap),
		batchPkReadHandler: batchpkread.New(heap),
	}

	// ping
	versionGroup.GET("/ping", routeHandler.Ping)

	// stat
	versionGroup.GET("/"+config.STAT_OPERATION, routeHandler.Stat)

	// batch
	versionGroup.POST("/"+config.BATCH_OPERATION, routeHandler.BatchPkRead)

	// pk read
	tableSpecificGroup := versionGroup.Group(config.DB_OPS_EP_GROUP)
	tableSpecificGroup.POST(config.PK_DB_OPERATION, routeHandler.PkRead)
}
