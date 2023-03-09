package integrationtests

import (
	"fmt"
	"math/rand"
	"os"
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
func InitialiseTesting(conf config.AllConfigs, createOnlyTheseDBs ...string) (cleanup func(), err error) {

	if !*testutils.WithRonDB {
		return
	}

	cleanupTLSCerts := func() {}
	if conf.Security.EnableTLS {
		cleanupTLSCerts, err = testutils.CreateAllTLSCerts()
		if err != nil {
			return cleanup, err
		}
	}

	// TODO: Explain why?
	rand.Seed(int64(time.Now().Nanosecond()))

	var dbsToCreate []string
	if len(createOnlyTheseDBs) > 0 {
		dbsToCreate = createOnlyTheseDBs
	} else {
		dbsToCreate = testdbs.GetAllDBs()
	}
	err, dropDatabases := testutils.CreateDatabases(conf.Security.UseHopsworksAPIKeys, dbsToCreate...)
	if err != nil {
		cleanupTLSCerts()
		return cleanup, fmt.Errorf("failed creating databases; error: %v", err)
	}

	newHeap, releaseBuffers, err := heap.New()
	if err != nil {
		cleanupTLSCerts()
		dropDatabases()
		return cleanup, fmt.Errorf("failed creating new heap; error: %v ", err)
	}

	// Wait for interrupt signal to gracefully shutdown the server
	quit := make(chan os.Signal)
	err, cleanupServers := servers.CreateAndStartDefaultServers(newHeap, quit)
	if err != nil {
		cleanupTLSCerts()
		dropDatabases()
		releaseBuffers()
		return cleanup, fmt.Errorf("failed creating default servers; error: %v ", err)
	}

	log.Info("Successfully started up default servers")
	time.Sleep(500 * time.Millisecond)

	return func() {
		cleanupTLSCerts()
		dropDatabases()
		releaseBuffers()
		cleanupServers()

		stats := newHeap.GetNativeBuffersStats()
		if stats.BuffersCount != stats.FreeBuffers {
			log.Errorf("Number of free buffers do not match. Expecting: %d, Got: %d",
				stats.BuffersCount, stats.FreeBuffers)
			return
		}
	}, nil
}
