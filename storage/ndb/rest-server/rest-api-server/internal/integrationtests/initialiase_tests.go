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
	"runtime/pprof"
	"time"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/dal/heap"
	"hopsworks.ai/rdrs/internal/log"
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

	conn, err := InitGRPCConnction()
	if err != nil {
		cleanupServers()
		releaseBuffers()
		cleanupTLSCerts()
		return cleanup, err
	}
	cleanupGRPCConn := func() {
		conn.Close()
	}

	// Check if profiling is enabled
	if profilingEnabled() {
		// Start profiling
		f, err := os.Create("profile.out")
		if err != nil {
			cleanupGRPCConn()
			cleanupServers()
			releaseBuffers()
			cleanupTLSCerts()
			return cleanup, fmt.Errorf("could not create profile.out; error: %v ", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			cleanupGRPCConn()
			cleanupServers()
			releaseBuffers()
			cleanupTLSCerts()
			return cleanup, fmt.Errorf("could not start CPU profile; error: %v ", err)
		}
	}

	return func() {
		// Running defer here in case checking the heap fails
		defer cleanupTLSCerts()
		defer releaseBuffers()
		defer cleanupServers()
		defer cleanupGRPCConn()
		defer pprof.StopCPUProfile()

		stats := newHeap.GetNativeBuffersStats()
		if stats.BuffersCount != stats.FreeBuffers {
			log.Errorf("Number of free buffers do not match. Expecting: %d, Got: %d",
				stats.BuffersCount, stats.FreeBuffers)
		}
	}, nil
}
