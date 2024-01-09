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

package apikey

import (
	"fmt"
	"math/rand"
	"os"
	"runtime/debug"
	"testing"
	"time"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/dal"
	"hopsworks.ai/rdrs2/internal/log"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/resources/testdbs"
)

/*
Wraps all unit tests in this package
*/
func TestMain(m *testing.M) {

	// We do this so that we can exit with error code 1 without discarding defer() functions
	retcode := 0
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("caught a panic in main(); making sure we are not returning with exit code 0;\nrecovery: %s\nstacktrace:\n%s", r, debug.Stack())
			retcode = 1
		}
		os.Exit(retcode)
	}()

	conf := config.GetAll()
	log.InitLogger(conf.Log)

	//creating databases for each test run is very slow
	//if sentinel DB exists then skip creating DBs
	//drop the "sentinel" DB if you want to recreate all the databases.
	//for MTR the cleanup is done in mysql-test/suite/rdrs/include/rdrs_cleanup.inc
	if !testutils.SentinelDBExists() {
		_, err := testutils.CreateDatabases(conf.Security.APIKey.UseHopsworksAPIKeys, testdbs.GetAllDBs()...)
		if err != nil {
			retcode = 1
			log.Errorf("failed creating databases; error: %v", err)
			return
		}
	}

	dalErr := dal.InitRonDBConnection(conf.RonDB, conf.RonDBMetadataCluster)
	if dalErr != nil {
		retcode = 1
		log.Errorf("failed to initialise RonDB connection; error: %s", dalErr.VerboseError())
		return
	}
	defer dal.ShutdownConnection()

	//init rand
	rand.Seed(time.Now().Unix())

	retcode = m.Run()
}
