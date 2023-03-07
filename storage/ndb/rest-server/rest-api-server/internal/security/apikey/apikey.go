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
	"crypto/sha256"
	"fmt"
	"strings"
	"sync"
	"time"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/dal"
)

type UserDBs struct {
	uDBs    map[string]bool
	expires time.Time
}

var key2UserDBs = make(map[string]UserDBs)
var key2UserDBsMutex sync.Mutex

func ValidateAPIKey(apiKey *string, dbs ...*string) error {

	if len(dbs) == 0 {
		return fmt.Errorf("Unauthorized")
	}

	keyFoundInCache, allowedAccess := findAndValidateCache(apiKey, dbs...)

	if keyFoundInCache {
		if !allowedAccess {
			return fmt.Errorf("Unauthorized")
		} else {
			return nil
		}
	} else {

		_, err := GetUserDatabases(apiKey) // this fetches the updates from DB and updates the cache
		if err != nil {
			return err
		}

		keyFoundInCache, allowedAccess = findAndValidateCache(apiKey, dbs...)
		if keyFoundInCache && allowedAccess {
			return nil
		} else {
			return fmt.Errorf("Unauthorized")
		}
	}
}

func findAndValidateCache(apiKey *string, dbs ...*string) (keyFoundInCache, allowedAccess bool) {

	keyFoundInCache = false
	allowedAccess = false

	key2UserDBsMutex.Lock()
	userDBs, ok := key2UserDBs[*apiKey]
	key2UserDBsMutex.Unlock()

	// found in cache
	if ok && time.Now().Before(userDBs.expires) {
		keyFoundInCache = true

		for _, db := range dbs {
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
	}
	return
}

func GetUserDatabases(apiKey *string) ([]string, error) {

	splits := strings.Split(*apiKey, ".")
	if len(splits) != 2 || len(splits[0]) != 16 {
		return []string{}, fmt.Errorf("Wrong API Key")
	}

	prefix := splits[0]
	secret := splits[1]

	key, err := dal.GetAPIKey(prefix)
	if err != nil {
		return []string{}, err
	}

	//sha256(client.secret + db.salt) = db.secret
	newSecret := sha256.Sum256([]byte(secret + key.Salt))
	newSecretHex := fmt.Sprintf("%x", newSecret)
	if strings.Compare(string(newSecretHex), key.Secret) != 0 {
		return []string{}, fmt.Errorf("Wrong API Key")
	}

	dbs, err := dal.GetUserProjects(key.UserID)
	if err != nil {
		return dbs, err
	}

	dbsMap := make(map[string]bool)
	for _, db := range dbs {
		dbsMap[db] = true
	}
	conf := config.GetAll()

	userDBs := UserDBs{uDBs: dbsMap,
		expires: time.Now().Add(time.Duration(conf.Security.HopsworksAPIKeysCacheValiditySec) * time.Second)}

	key2UserDBsMutex.Lock()
	key2UserDBs[*apiKey] = userDBs
	key2UserDBsMutex.Unlock()

	return dbs, nil
}

func Reset() {
	key2UserDBsMutex.Lock()
	key2UserDBs = make(map[string]UserDBs)
	key2UserDBsMutex.Unlock()
}

func cacheUpdateTime(apiKey string) time.Time {
	key2UserDBsMutex.Lock()
	defer key2UserDBsMutex.Unlock()
	conf := config.GetAll()
	val, ok := key2UserDBs[apiKey]
	if ok {
		return val.expires.Add(time.Duration(-conf.Security.HopsworksAPIKeysCacheValiditySec) * time.Second)
	} else {
		return time.Unix(0, 0)
	}
}
