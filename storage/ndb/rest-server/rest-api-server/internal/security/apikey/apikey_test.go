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

	"hopsworks.ai/rdrs/internal/common"
	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/dal"
	"hopsworks.ai/rdrs/internal/security/authcache"
)

func TestAPIKey(t *testing.T) {
	conString := fmt.Sprintf("%s:%d", config.Configuration().RonDBConfig.IP,
		config.Configuration().RonDBConfig.Port)

	dal.InitRonDBConnection(conString, true)
	defer dal.ShutdownConnection()

	common.CreateDatabases(t, []string{"DB001", "DB002"}...)
	defer common.DropDatabases(t, []string{"DB001", "DB002"}...)

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
	apiKey = common.HOPSWORKS_TEST_API_KEY
	db1 := "test3"
	err = ValidateAPIKey(&apiKey, &db1)
	if err == nil {
		t.Fatalf("This should have failed")
	}

	// correct api key
	apiKey = common.HOPSWORKS_TEST_API_KEY
	db1 = "DB001"
	err = ValidateAPIKey(&apiKey, &db1)
	if err != nil {
		t.Fatalf("No error expected")
	}

	// valid api key but no db
	apiKey = common.HOPSWORKS_TEST_API_KEY
	err = ValidateAPIKey(&apiKey, nil)
	if err == nil {
		t.Fatalf("This should have failed")
	}

	// no errors
	apiKey = common.HOPSWORKS_TEST_API_KEY
	db1 = "DB001"
	db2 := "DB002"
	err = ValidateAPIKey(&apiKey, &db1, &db2)
	if err != nil {
		t.Fatalf("No error expected; err: %v", err)
	}
}

// check that cache is updated every N secs
func TestAPIKeyCache1(t *testing.T) {

	conString := fmt.Sprintf("%s:%d", config.Configuration().RonDBConfig.IP,
		config.Configuration().RonDBConfig.Port)

	dal.InitRonDBConnection(conString, true)
	defer dal.ShutdownConnection()

	common.CreateDatabases(t, []string{"DB001", "DB002"}...)
	defer common.DropDatabases(t, []string{"DB001", "DB002"}...)

	apiKey := common.HOPSWORKS_TEST_API_KEY
	db1 := "DB001"
	err := ValidateAPIKey(&apiKey, &db1)
	if err != nil {
		t.Fatalf("No error expected")
	}

	lastUpdated1 := authcache.RefreshExpiration(common.HOPSWORKS_TEST_API_KEY)

	apiKey = common.HOPSWORKS_TEST_API_KEY
	db1 = "DB001"
	err = ValidateAPIKey(&apiKey, &db1)
	if err != nil {
		t.Fatalf("No error expected")
	}

	lastUpdated2 := authcache.RefreshExpiration(common.HOPSWORKS_TEST_API_KEY)

	if lastUpdated1 != lastUpdated2 {
		t.Fatalf("Cache update time is expected to be the same")
	}

	time.Sleep(time.Duration(config.Configuration().Security.HopsWorksAPIKeysCacheValiditySec))

	apiKey = common.HOPSWORKS_TEST_API_KEY
	db1 = "DB001"
	err = ValidateAPIKey(&apiKey, &db1)
	if err != nil {
		t.Fatalf("No error expected")
	}

	lastUpdated3 := authcache.RefreshExpiration(common.HOPSWORKS_TEST_API_KEY)

	lastUpdated2p := lastUpdated2.Add(time.Duration(config.Configuration().Security.HopsWorksAPIKeysCacheValiditySec))
	if lastUpdated2 != lastUpdated3 && lastUpdated2p.Before(lastUpdated3) {
		t.Fatalf("Cache time is not updated properly")
	}
}

// check that cache is updated every N secs even if the user is not authorized to access a DB
func TestAPIKeyCache2(t *testing.T) {

	conString := fmt.Sprintf("%s:%d", config.Configuration().RonDBConfig.IP,
		config.Configuration().RonDBConfig.Port)

	dal.InitRonDBConnection(conString, true)
	defer dal.ShutdownConnection()

	common.CreateDatabases(t, []string{"DB001", "DB002"}...)
	defer common.DropDatabases(t, []string{"DB001", "DB002"}...)

	apiKey := common.HOPSWORKS_TEST_API_KEY
	db3 := "DB003"
	err := ValidateAPIKey(&apiKey, &db3)
	if err == nil {
		t.Fatalf("Expected it to fail")
	}

	lastUpdated1 := authcache.RefreshExpiration(common.HOPSWORKS_TEST_API_KEY)

	apiKey = common.HOPSWORKS_TEST_API_KEY
	db1 := "DB001"
	err = ValidateAPIKey(&apiKey, &db1)
	if err != nil {
		t.Fatalf("No error expected")
	}

	lastUpdated2 := authcache.RefreshExpiration(common.HOPSWORKS_TEST_API_KEY)

	if lastUpdated1 != lastUpdated2 {
		t.Fatalf("Cache update time is expected to be the same")
	}
}
