package batchpkread

import (
	"math/rand"
	"os"
	"testing"
	"time"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/dal/heap"
	"hopsworks.ai/rdrs/internal/log"
	"hopsworks.ai/rdrs/internal/servers"
	"hopsworks.ai/rdrs/internal/testutils"
	"hopsworks.ai/rdrs/resources/testdbs"
)

/*
	Wraps all unit tests in this package
*/
func TestMain(m *testing.M) {

	if !*testutils.WithRonDB {
		return
	}

	conf := config.GetAll()

	log.InitLogger(conf.Log)

	var err error
	var cleanup func()
	if conf.Security.EnableTLS {
		cleanup, err = testutils.CreateAllTLSCerts()
		if err != nil {
			log.Errorf(err.Error())
			return
		}
		defer cleanup()
	}

	// TODO: Explain why?
	rand.Seed(int64(time.Now().Nanosecond()))

	allDBs := testdbs.GetAllDBs()
	err, removeDatabases := testutils.CreateDatabases(conf.Security.UseHopsworksAPIKeys, allDBs...)
	if err != nil {
		log.Errorf("failed creating databases; error: %v", err)
		return
	}
	defer removeDatabases()

	newHeap, releaseBuffers, err := heap.New()
	if err != nil {
		log.Errorf("failed creating new heap; error: %v ", err)
		return
	}
	defer releaseBuffers()

	// Wait for interrupt signal to gracefully shutdown the server
	quit := make(chan os.Signal)
	err, cleanupServers := servers.CreateAndStartDefaultServers(newHeap, quit)
	if err != nil {
		log.Errorf("failed creating default servers; error: %v ", err)
		return
	}
	defer cleanupServers()
	log.Info("Successfully started up default servers")
	time.Sleep(500 * time.Millisecond)

	defer func() {
		stats := newHeap.GetNativeBuffersStats()
		if stats.BuffersCount != stats.FreeBuffers {
			log.Errorf("Number of free buffers do not match. Expecting: %d, Got: %d",
				stats.BuffersCount, stats.FreeBuffers)
			return
		}
	}()
	runTests := m.Run()
	os.Exit(runTests)
}
