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

package authcache

import (
	"sync"
	"time"

	"hopsworks.ai/rdrs/internal/config"
)

type UserDBs struct {
	uDBs    map[string]bool
	expires time.Time
}

// Thread-safe cache to save users' database permissions
var key2UserDBs = make(map[string]UserDBs)
var key2UserDBsMutex sync.Mutex

// TODO: Check if this is needed
func Reset() {
	key2UserDBsMutex.Lock()
	key2UserDBs = make(map[string]UserDBs)
	key2UserDBsMutex.Unlock()
}

func FindAndValidateCache(apiKey *string, dbs ...*string) (keyFoundInCache, allowedAccess bool) {
	keyFoundInCache = false
	allowedAccess = false

	key2UserDBsMutex.Lock()
	userDBs, ok := key2UserDBs[*apiKey]
	key2UserDBsMutex.Unlock()

	if !ok && time.Now().After(userDBs.expires) {
		return
	}

	keyFoundInCache = true
	for _, db := range dbs {
		// TODO: How would this ever be nil?
		if db == nil {
			allowedAccess = false
			return
		}

		if _, found := userDBs.uDBs[*db]; !found {
			allowedAccess = false
			return
		}
	}
	allowedAccess = true
	return
}

func Set(apikey string, dbs []string) {
	dbsMap := make(map[string]bool)
	for _, db := range dbs {
		dbsMap[db] = true
	}

	conf := config.GetAll()
	userDBs := UserDBs{uDBs: dbsMap,
		expires: time.Now().Add(time.Duration(conf.Security.HopsworksAPIKeysCacheValiditySec) * time.Second)}

	key2UserDBsMutex.Lock()
	key2UserDBs[apikey] = userDBs
	key2UserDBsMutex.Unlock()
}

// Just for testing..
func RefreshExpiration(apiKey string) time.Time {
	key2UserDBsMutex.Lock()
	defer key2UserDBsMutex.Unlock()
	val, ok := key2UserDBs[apiKey]
	if ok {
		conf := config.GetAll()
		return val.expires.Add(time.Duration(-conf.Security.HopsworksAPIKeysCacheValiditySec) * time.Second)
	} else {
		return time.Unix(0, 0)
	}
}
