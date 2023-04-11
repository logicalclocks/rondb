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

package integrationtests

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/dal/heap"
	"hopsworks.ai/rdrs/internal/log"
	"hopsworks.ai/rdrs/internal/security/apikey/hopsworkscache"

	"hopsworks.ai/rdrs/internal/servers"
	"hopsworks.ai/rdrs/internal/testutils"
	"hopsworks.ai/rdrs/resources/testdbs"
)

var enableProfiling = flag.Bool("profile", false, "enable CPU profiling")

func profilingEnabled() bool {
	return *enableProfiling || (os.Getenv("PROFILE_CPU") == "1")
}

/*
Wraps all unit tests in this package
*/
func InitialiseTesting(conf config.AllConfigs, createOnlyTheseDBs ...string) (func(), error) {

	/*
		This tends to deliver better benchmarking results
		e.g. pkreads (but not ping):
		runtime.GOMAXPROCS(runtime.NumCPU() * 2)
	*/
	runtime.GOMAXPROCS(conf.Internal.GOMAXPROCS)

	index := 0
	cleanupFNs := make([]*func(), 10)
	cleanupFN := func() {
		//clean up in reverse order
		for i := len(cleanupFNs) - 1; i >= 0; i-- {
			if cleanupFNs[i] != nil {
				(*cleanupFNs[i])()
			}
		}
	}

	if !*testutils.WithRonDB {
		return nil, nil
	}

	//---------------------------- TLS ----------------------------------------
	if conf.Security.TLS.EnableTLS {
		cleanupTLSCerts, err := testutils.CreateAllTLSCerts()
		if err != nil {
			return nil, err
		}
		cleanupFNs[index] = &cleanupTLSCerts
		index++
	}

	//---------------------------- DATABASES ----------------------------------
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
		err, _ := testutils.CreateDatabases(conf.Security.APIKey.UseHopsworksAPIKeys, dbsToCreate...)
		if err != nil {
			cleanupFN()
			return nil, fmt.Errorf("failed creating databases; error: %v", err)
		}
	}

	//---------------------------- HEAP ---------------------------------------
	newHeap, releaseBuffers, err := heap.New()
	if err != nil {
		cleanupFN()
		return nil, fmt.Errorf("failed creating new heap; error: %v ", err)
	}
	cleanupFNs[index] = &releaseBuffers
	index++

	//---------------------------- API KEY Cache ------------------------------
	apiKeyCache := hopsworkscache.New()
	if err != nil {
		cleanupFN()
		return nil, fmt.Errorf("failed creating new API Key Cache; error: %v ", err)
	}
	apiKeyCleanup := func() {
		apiKeyCache.Cleanup()
	}
	cleanupFNs[index] = &apiKeyCleanup
	index++

	//---------------------------- Servers ------------------------------------
	// Wait for interrupt signal to gracefully shutdown the server
	quit := make(chan os.Signal)
	err, cleanupServers := servers.CreateAndStartDefaultServers(newHeap, apiKeyCache, quit)
	if err != nil {
		cleanupFN()
		return nil, fmt.Errorf("failed creating default servers; error: %v ", err)
	}
	cleanupFNs[index] = &cleanupServers
	index++

	// some times the servers take some time to start and units tests fail due to connection failures
	time.Sleep(500 * time.Millisecond)
	log.Debug("Successfully started up servers")

	// Check if profiling is enabled
	if profilingEnabled() {
		f, err := os.Create("profile.out")
		if err != nil {
			cleanupFN()
			return nil, fmt.Errorf("could not create profile.out; error: %w ", err)
		}
		fileCloser := func() { f.Close() }
		cleanupFNs[index] = &fileCloser
		index++

		// Start profiling
		if err := pprof.StartCPUProfile(f); err != nil {
			cleanupFN()
			return nil, fmt.Errorf("could not start CPU profile; error: %w ", err)
		}
	}

	return cleanupFN, nil
}
