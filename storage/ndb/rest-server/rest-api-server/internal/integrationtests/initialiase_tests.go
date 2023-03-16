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

	//creating databases for each test run is very slow
	//if sentinel DB exists then skip creating DBs
	//drop the "sentinel" DB if you want to recreate all the databases.
	//for MTR the cleanup is done in mysql-test/suite/rdrs/include/rdrs_cleanup.inc
	if !testutils.SentinelDBExists() {
		err, _ := testutils.CreateDatabases(conf.Security.UseHopsworksAPIKeys, dbsToCreate...)
		if err != nil {
			cleanupTLSCerts()
			return cleanup, fmt.Errorf("failed creating databases; error: %v", err)
		}
	}

	newHeap, releaseBuffers, err := heap.New()
	if err != nil {
		cleanupTLSCerts()
		return cleanup, fmt.Errorf("failed creating new heap; error: %v ", err)
	}

	// Wait for interrupt signal to gracefully shutdown the server
	quit := make(chan os.Signal)
	err, cleanupServers := servers.CreateAndStartDefaultServers(newHeap, quit)
	if err != nil {
		releaseBuffers()
		cleanupTLSCerts()
		return cleanup, fmt.Errorf("failed creating default servers; error: %v ", err)
	}

	log.Info("Successfully started up default servers")
	time.Sleep(500 * time.Millisecond)

	return func() {
		// Running defer here in case checking the heap fails
		defer cleanupTLSCerts()
		defer releaseBuffers()
		defer cleanupServers()

		stats := newHeap.GetNativeBuffersStats()
		if stats.BuffersCount != stats.FreeBuffers {
			log.Errorf("Number of free buffers do not match. Expecting: %d, Got: %d",
				stats.BuffersCount, stats.FreeBuffers)
		}
	}, nil
}
