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
	"testing"
	"time"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/dal"
	"hopsworks.ai/rdrs/internal/testutils"
)

func TestAPIKey(t *testing.T) {

	if !*testutils.WithRonDB {
		t.Skip("skipping test without RonDB")
	}

	conf := config.GetAll()
	if !conf.Security.UseHopsworksAPIKeys {
		t.Log("tests may fail because Hopsworks API keys are deactivated")
	}

	conString := config.GenerateMgmdConnectString(conf)

	dal.InitRonDBConnection(conString, true)
	defer dal.ShutdownConnection()

	testutils.CreateDatabases(t, []string{"db001", "db002"}...)
	defer testutils.DropDatabases(t, []string{"db001", "db002"}...)

	apiKey := "bkYjEz6OTZyevbqT.ocHajJhnE0ytBh8zbYj3IXupyMqeMZp8PW464eTxzxqP5afBjodEQUgY0lmL33ub"
	err := ValidateAPIKey(&apiKey, nil)
	if err == nil {
		t.Fatalf("Supplied wrong prefix. This should have failed. ")
	}

	apiKey = "bkYjEz6OTZyevbqT."
	err = ValidateAPIKey(&apiKey)
	if err == nil {
		t.Fatalf("No secret. This should have failed")
	}

	apiKey = "bkYjEz6OTZyevbq.ocHajJhnE0ytBh8zbYj3IXupyMqeMZp8PW464eTxzxqP5afBjodEQUgY0lmL33ub"
	err = ValidateAPIKey(&apiKey)
	if err == nil {
		t.Fatalf("Wrong length prefix. This should have failed")
	}

	// correct api key but wrong db. this api key can not access test3 db
	apiKey = testutils.HOPSWORKS_TEST_API_KEY
	db1 := "test3"
	err = ValidateAPIKey(&apiKey, &db1)
	if err == nil {
		t.Fatalf("This should have failed")
	}

	// correct api key
	apiKey = testutils.HOPSWORKS_TEST_API_KEY
	db1 = "db001"
	err = ValidateAPIKey(&apiKey, &db1)
	if err != nil {
		t.Fatalf("No error expected; err: %v", err)
	}

	// valid api key but no db
	apiKey = testutils.HOPSWORKS_TEST_API_KEY
	err = ValidateAPIKey(&apiKey, nil)
	if err == nil {
		t.Fatalf("This should have failed")
	}

	// no errors
	apiKey = testutils.HOPSWORKS_TEST_API_KEY
	db1 = "db001"
	db2 := "db002"
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
	if !conf.Security.UseHopsworksAPIKeys {
		t.Log("tests may fail because Hopsworks API keys are deactivated")
	}

	conString := config.GenerateMgmdConnectString(conf)

	dal.InitRonDBConnection(conString, true)
	defer dal.ShutdownConnection()

	testutils.CreateDatabases(t, []string{"db001", "db002"}...)
	defer testutils.DropDatabases(t, []string{"db001", "db002"}...)

	apiKey := testutils.HOPSWORKS_TEST_API_KEY
	db1 := "db001"
	err := ValidateAPIKey(&apiKey, &db1)
	if err != nil {
		t.Fatalf("No error expected; err: %v", err)
	}

	lastUpdated1 := cacheUpdateTime(testutils.HOPSWORKS_TEST_API_KEY)

	apiKey = testutils.HOPSWORKS_TEST_API_KEY
	db1 = "db001"
	err = ValidateAPIKey(&apiKey, &db1)
	if err != nil {
		t.Fatalf("No error expected; err: %v", err)
	}

	lastUpdated2 := cacheUpdateTime(testutils.HOPSWORKS_TEST_API_KEY)

	if lastUpdated1 != lastUpdated2 {
		t.Fatalf("Cache update time is expected to be the same")
	}

	time.Sleep(time.Duration(conf.Security.HopsworksAPIKeysCacheValiditySec))

	apiKey = testutils.HOPSWORKS_TEST_API_KEY
	db1 = "db001"
	err = ValidateAPIKey(&apiKey, &db1)
	if err != nil {
		t.Fatalf("No error expected; err: %v", err)
	}

	lastUpdated3 := cacheUpdateTime(testutils.HOPSWORKS_TEST_API_KEY)

	lastUpdated2p := lastUpdated2.Add(time.Duration(conf.Security.HopsworksAPIKeysCacheValiditySec))
	if lastUpdated2 != lastUpdated3 && lastUpdated2p.Before(lastUpdated3) {
		t.Fatalf("Cache time is not updated properly")
	}

}

// check that cache is updated every N secs even if the user is not authorized to access a DB
func TestAPIKeyCache2(t *testing.T) {

	if !*testutils.WithRonDB {
		t.Skip("skipping test without RonDB")
	}

	conf := config.GetAll()
	if !conf.Security.UseHopsworksAPIKeys {
		t.Log("tests may fail because Hopsworks API keys are deactivated")
	}

	conString := config.GenerateMgmdConnectString(conf)

	dal.InitRonDBConnection(conString, true)
	defer dal.ShutdownConnection()

	testutils.CreateDatabases(t, []string{"db001", "db002"}...)
	defer testutils.DropDatabases(t, []string{"db001", "db002"}...)

	apiKey := testutils.HOPSWORKS_TEST_API_KEY
	db3 := "db003"
	err := ValidateAPIKey(&apiKey, &db3)
	if err == nil {
		t.Fatalf("Expected it to fail")
	}

	lastUpdated1 := cacheUpdateTime(testutils.HOPSWORKS_TEST_API_KEY)

	apiKey = testutils.HOPSWORKS_TEST_API_KEY
	db1 := "db001"
	err = ValidateAPIKey(&apiKey, &db1)
	if err != nil {
		t.Fatalf("No error expected; err: %v", err)
	}

	lastUpdated2 := cacheUpdateTime(testutils.HOPSWORKS_TEST_API_KEY)

	if lastUpdated1 != lastUpdated2 {
		t.Fatalf("Cache update time is expected to be the same")
	}
}
