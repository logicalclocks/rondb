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
package apikey

import (
	"fmt"
	"testing"
	"time"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/dal"
	"hopsworks.ai/rdrs/internal/security/authcache"
	"hopsworks.ai/rdrs/internal/testutils"
)

func TestAPIKey(t *testing.T) {
	if !*testutils.WithRonDB {
		t.Skip("skipping test without RonDB")
	}

	conf := config.GetAll()
	connectString := fmt.Sprintf("%s:%d",
		conf.RonDB.MgmdIP,
		conf.RonDB.MgmdPort,
	)

	dalErr := dal.InitRonDBConnection(connectString, true)
	if dalErr != nil {
		t.Fatalf("failed to initialise RonDB connection; error: %s", dalErr.VerboseError())
	}
	defer dal.ShutdownConnection()

	testutils.CreateDatabases(t, []string{"DB001", "DB002"}...)
	defer testutils.DropDatabases(t, []string{"DB001", "DB002"}...)

	apiKey := "bkYjEz6OTZyevbqT.ocHajJhnE0ytBh8zbYj3IXupyMqeMZp8PW464eTxzxqP5afBjodEQUgY0lmL33ub"
	err := ValidateAPIKey(&apiKey, nil)
	if err == nil {
		t.Fatalf("Supplied wrong prefix. This should have failed; error: %v", err)
	}

	apiKey = "bkYjEz6OTZyevbqT."
	err = ValidateAPIKey(&apiKey)
	if err == nil {
		t.Fatalf("No secret. This should have failed; error: %v", err)
	}

	apiKey = "bkYjEz6OTZyevbq.ocHajJhnE0ytBh8zbYj3IXupyMqeMZp8PW464eTxzxqP5afBjodEQUgY0lmL33ub"
	err = ValidateAPIKey(&apiKey)
	if err == nil {
		t.Fatalf("Wrong length prefix. This should have failed; error: %v", err)
	}

	// correct api key but wrong db. this api key can not access test3 db
	apiKey = testutils.HOPSWORKS_TEST_API_KEY
	db1 := "test3"
	err = ValidateAPIKey(&apiKey, &db1)
	if err == nil {
		t.Fatalf("This should have failed; error: %v", err)
	}

	// correct api key
	apiKey = testutils.HOPSWORKS_TEST_API_KEY
	db1 = "DB001"
	err = ValidateAPIKey(&apiKey, &db1)
	if err != nil {
		t.Fatalf("No error expected; error: %v", err)
	}

	// valid api key but no db
	apiKey = testutils.HOPSWORKS_TEST_API_KEY
	err = ValidateAPIKey(&apiKey, nil)
	if err == nil {
		t.Fatalf("This should have failed; error: %v", err)
	}

	// no errors
	apiKey = testutils.HOPSWORKS_TEST_API_KEY
	db1 = "DB001"
	db2 := "DB002"
	err = ValidateAPIKey(&apiKey, &db1, &db2)
	if err != nil {
		t.Fatalf("No error expected; err: %v", err)
	}
}

// check that cache is updated every N secs
func TestAPIKeyCache1(t *testing.T) {
	if !*testutils.WithRonDB {
		t.Skip("skipping test without RonDB")
	}

	conf := config.GetAll()
	connectString := fmt.Sprintf("%s:%d",
		conf.RonDB.MgmdIP,
		conf.RonDB.MgmdPort,
	)

	dalErr := dal.InitRonDBConnection(connectString, true)
	if dalErr != nil {
		t.Fatalf("failed to initialise RonDB connection; error: %s", dalErr.VerboseError())
	}
	defer dal.ShutdownConnection()

	testutils.CreateDatabases(t, []string{"DB001", "DB002"}...)
	defer testutils.DropDatabases(t, []string{"DB001", "DB002"}...)

	apiKey := testutils.HOPSWORKS_TEST_API_KEY
	db1 := "DB001"
	err := ValidateAPIKey(&apiKey, &db1)
	if err != nil {
		t.Fatalf("No error expected; error: %v", err)
	}

	lastUpdated1 := authcache.RefreshExpiration(testutils.HOPSWORKS_TEST_API_KEY)

	apiKey = testutils.HOPSWORKS_TEST_API_KEY
	db1 = "DB001"
	err = ValidateAPIKey(&apiKey, &db1)
	if err != nil {
		t.Fatalf("No error expected; error: %v", err)
	}

	lastUpdated2 := authcache.RefreshExpiration(testutils.HOPSWORKS_TEST_API_KEY)

	if lastUpdated1 != lastUpdated2 {
		t.Fatalf("Cache update time is expected to be the same; error: %v", err)
	}

	time.Sleep(time.Duration(conf.Security.HopsWorksAPIKeysCacheValiditySec))

	apiKey = testutils.HOPSWORKS_TEST_API_KEY
	db1 = "DB001"
	err = ValidateAPIKey(&apiKey, &db1)
	if err != nil {
		t.Fatalf("No error expected; error: %v", err)
	}

	lastUpdated3 := authcache.RefreshExpiration(testutils.HOPSWORKS_TEST_API_KEY)

	lastUpdated2p := lastUpdated2.Add(time.Duration(conf.Security.HopsWorksAPIKeysCacheValiditySec))
	if lastUpdated2 != lastUpdated3 && lastUpdated2p.Before(lastUpdated3) {
		t.Fatalf("Cache time is not updated properly; error: %v", err)
	}
}

// check that cache is updated every N secs even if the user is not authorized to access a DB
func TestAPIKeyCache2(t *testing.T) {
	if !*testutils.WithRonDB {
		t.Skip("skipping test without RonDB")
	}

	conf := config.GetAll()
	connectString := fmt.Sprintf("%s:%d",
		conf.RonDB.MgmdIP,
		conf.RonDB.MgmdPort,
	)

	dalErr := dal.InitRonDBConnection(connectString, true)
	if dalErr != nil {
		t.Fatalf("failed to initialise RonDB connection; error: %s", dalErr.VerboseError())
	}
	defer dal.ShutdownConnection()

	testutils.CreateDatabases(t, []string{"DB001", "DB002"}...)
	defer testutils.DropDatabases(t, []string{"DB001", "DB002"}...)

	apiKey := testutils.HOPSWORKS_TEST_API_KEY
	db3 := "DB003"
	err := ValidateAPIKey(&apiKey, &db3)
	if err == nil {
		t.Fatalf("Expected it to fail; error: %v", err)
	}

	lastUpdated1 := authcache.RefreshExpiration(testutils.HOPSWORKS_TEST_API_KEY)

	apiKey = testutils.HOPSWORKS_TEST_API_KEY
	db1 := "DB001"
	err = ValidateAPIKey(&apiKey, &db1)
	if err != nil {
		t.Fatalf("No error expected; error: %v", err)
	}

	lastUpdated2 := authcache.RefreshExpiration(testutils.HOPSWORKS_TEST_API_KEY)

	if lastUpdated1 != lastUpdated2 {
		t.Fatalf("Cache update time is expected to be the same; error: %v", err)
	}
}
