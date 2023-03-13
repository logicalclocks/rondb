package apikey

import (
	"fmt"
	"os"
	"runtime/debug"
	"testing"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/log"
	"hopsworks.ai/rdrs/internal/testutils"
	"hopsworks.ai/rdrs/resources/testdbs"
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
		err, _ := testutils.CreateDatabases(conf.Security.UseHopsworksAPIKeys, testdbs.GetAllDBs()...)
		if err != nil {
			log.Panicf("failed creating databases; error: %v", err)
		}
	}

	retcode = m.Run()
}
