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

package hopsworkscache

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/dal"
	"hopsworks.ai/rdrs2/internal/log"
	"hopsworks.ai/rdrs2/internal/security/apikey/basecache"
)

func New() *Cache {
	someRand := rand.New(rand.NewSource(time.Now().Unix()))
	return &Cache{
		key2UserDBsCache: make(map[string]*UserDBs),
		randomGenerator:  someRand,
	}
}

type Cache struct {
	key2UserDBsCache     map[string]*UserDBs // API Key -> User Databases
	key2UserDBsCacheLock sync.RWMutex
	randomGenerator      *rand.Rand
}

// Cache Entry
type UserDBs struct {
	userDBs         map[string]bool // DBs
	lastUsed        time.Time       // for removing unused entries
	lastUpdated     time.Time       // last updated TS
	rowLock         sync.RWMutex    // this is used to prevent concurrent updates
	ticker          *time.Ticker    // ticker is used to keep the cache entry updated
	evicted         bool            // is evicted or deleted
	refreshInterval time.Duration   // Cache refresh interval
}

func (hwc *Cache) Cleanup() error {
	hwc.key2UserDBsCacheLock.Lock()
	defer hwc.key2UserDBsCacheLock.Unlock()

	if log.IsInfo() {
		log.Info("Shutting down API Key Cache")
	}

	for _, udbs := range hwc.key2UserDBsCache {
		udbs.ticker.Stop()
		udbs.evicted = true
	}

	hwc.key2UserDBsCache = make(map[string]*UserDBs)

	return nil
}

// update the cache entry by fetching the API key from backend
func (hwc *Cache) UpdateCache(apiKey *string) error {

	// if the entry does not already exist in the
	// cache then multiple clients will try to read and
	// update the API key from the backend simultaneously.
	// Trying to prevent multiple writers here

	// first check using read lock
	hwc.key2UserDBsCacheLock.RLock()
	_, ok := hwc.key2UserDBsCache[*apiKey]
	hwc.key2UserDBsCacheLock.RUnlock()

	if !ok {
		// Continue with write lock
		hwc.key2UserDBsCacheLock.Lock()

		_, ok := hwc.key2UserDBsCache[*apiKey]
		if !ok { // the entry still does not exists. insert a new row
			udbs := &UserDBs{}
			udbs.refreshInterval = hwc.refreshIntervalWithJitter()
			hwc.key2UserDBsCache[*apiKey] = udbs
			hwc.startUpdateTicker(apiKey, udbs)
		}
		hwc.key2UserDBsCacheLock.Unlock()
	}

	// the entry already exists. Ticker will update it
	return nil
}

func (hwc *Cache) startUpdateTicker(apiKey *string, udbs *UserDBs) error {
	started := false
	go hwc.cacheEntryUpdater(apiKey, &started)

	//wait for go routine to start
	for {
		if started {
			if log.IsDebug() {
				log.Debugf("API Key cache updater is started for %s. Refresh Interval: %v ", *apiKey, udbs.refreshInterval)
			}
			return nil
		} else {
			time.Sleep(50 * time.Microsecond)
		}
	}
}

func (hwc *Cache) cacheEntryUpdater(apiKey *string, started *bool) {

	udbs, ok := hwc.key2UserDBsCache[*apiKey] // not need for read lock here as the caller holds write lock
	if !ok {
		log.Errorf("Cache updater failed. Report programming error. API Key %s", *apiKey)
		return
	}

	udbs.ticker = time.NewTicker(udbs.refreshInterval)

	cleaner := func() {
		//clean up on eviction
		if log.IsDebug() {
			log.Debugf("API Key %s is evicted", *apiKey)
		}
		udbs.ticker.Stop()
		udbs.evicted = true
		hwc.key2UserDBsCacheLock.Lock()
		delete(hwc.key2UserDBsCache, *apiKey)
		hwc.key2UserDBsCacheLock.Unlock()
	}

	for {
		udbs.rowLock.Lock()
		*started = true
		fail := false

		var hopsKey *dal.HopsworksAPIKey
		var err error
		var dbs []string
		if !udbs.evicted {
			hopsKey, err = hwc.authenticateUser(apiKey)
			if err != nil {
				log.Debugf("Cache updater failed to read API Key. API Key: %s, Error: %v", *apiKey, err)
				fail = true
			}
		}

		if !fail && !udbs.evicted {
			dbs, err = hwc.getUserDatabases(apiKey, hopsKey)
			if err != nil {
				log.Debugf("Cache updater failed to reads user projects.  API Key: %s, Error: %v", *apiKey, err)
			}

			setErr := hwc.updateRecord(apiKey, dbs, udbs)
			if setErr != nil {
				log.Debugf("Cache updater failed to update projects.  API Key: %s, Error: %v", *apiKey, err)
			}
		}

		udbs.rowLock.Unlock()

		<-udbs.ticker.C

		if udbs.evicted {
			//no need for cleanup as evicter cleans up
			return
		}

		// if the entry has not been used for some time the evict this
		udbs.rowLock.RLock()
		lastUsed := udbs.lastUsed
		udbs.rowLock.RUnlock()

		evictTime := time.Duration(config.GetAll().Security.APIKey.CacheUnusedEntriesEvictionMS) * time.Duration(time.Millisecond)
		if lastUsed.Add(evictTime).Before(time.Now()) {
			cleaner()
			return
		}
	}
}

// Authenticates only using the the cache. No request sent to backend
func (hwc *Cache) FindAndValidate(apiKey *string, dbs ...*string) (keyFoundInCache, allowedAccess bool) {
	keyFoundInCache = false
	allowedAccess = false

	hwc.key2UserDBsCacheLock.RLock()
	userDBs, ok := hwc.key2UserDBsCache[*apiKey]
	hwc.key2UserDBsCacheLock.RUnlock()

	if !ok {
		return
	}

	userDBs.rowLock.RLock()
	defer userDBs.rowLock.RUnlock()

	// update TS
	userDBs.lastUsed = time.Now()

	keyFoundInCache = true
	for _, db := range dbs {
		if db == nil {
			allowedAccess = false
			return
		}

		if _, found := userDBs.userDBs[*db]; !found {
			allowedAccess = false
			return
		}
	}

	allowedAccess = true
	return
}

/*
Checking whether the API key can access the given databases
*/
func (hwc *Cache) ValidateAPIKey(apiKey *string, dbs ...*string) error {
	err := basecache.ValidateApiKeyFormat(apiKey)
	if err != nil {
		return err
	}

	if len(dbs) == 0 {
		return nil
	}

	// Authenticates only using the the cache. No request sent to backend
	keyFoundInCache, allowedAccess := hwc.FindAndValidate(apiKey, dbs...)
	if keyFoundInCache {
		if !allowedAccess {
			return fmt.Errorf("unauthorized. Found in cache: %v, allowed access %v",
				keyFoundInCache, allowedAccess)
		}
		return nil
	}

	// Update the cache by fetching the API key from backend
	if err = hwc.UpdateCache(apiKey); err != nil {
		return err
	}

	// Authenticates only using the the cache. No request sent to backend
	keyFoundInCache, allowedAccess = hwc.FindAndValidate(apiKey, dbs...)
	if !keyFoundInCache || !allowedAccess {
		return fmt.Errorf("api key is unauthorized; updated cache - found in cache: %v, allowed access %v",
			keyFoundInCache, allowedAccess)
	}
	return nil
}

func (hwc *Cache) updateRecord(apikey *string, dbs []string, udbs *UserDBs) error {
	// caller holds the lock
	if udbs.evicted {
		return nil
	}

	dbsMap := make(map[string]bool)
	for _, db := range dbs {
		dbsMap[db] = true
	}

	udbs.userDBs = dbsMap
	udbs.lastUpdated = time.Now()

	return nil
}

// Just for testing..
func (hwc *Cache) LastUsed(apiKey *string) time.Time {
	hwc.key2UserDBsCacheLock.RLock()
	defer hwc.key2UserDBsCacheLock.RUnlock()
	entry, ok := hwc.key2UserDBsCache[*apiKey]
	if ok {
		return entry.lastUsed
	} else {
		return time.Unix(0, 0)
	}
}

func (hwc *Cache) LastUpdated(apiKey *string) time.Time {
	hwc.key2UserDBsCacheLock.RLock()
	defer hwc.key2UserDBsCacheLock.RUnlock()
	entry, ok := hwc.key2UserDBsCache[*apiKey]
	if ok {
		return entry.lastUpdated
	} else {
		return time.Unix(0, 0)
	}
}

func (hwc *Cache) authenticateUser(apiKey *string) (*dal.HopsworksAPIKey, error) {
	// caller holds the lock
	splits := strings.Split(*apiKey, ".")
	prefix := splits[0]
	clientSecret := splits[1]

	key, err := dal.GetAPIKey(prefix)
	if err != nil {
		return nil, err
	}

	//sha256(client.secret + db.salt) = db.secret
	newSecret := sha256.Sum256([]byte(clientSecret + key.Salt))
	newSecretHex := fmt.Sprintf("%x", newSecret)
	if strings.Compare(string(newSecretHex), key.Secret) != 0 {
		return nil, errors.New("bad API Key")
	}
	return key, nil
}

// This fetches the databases from the DB and updates the cache
func (hwc *Cache) getUserDatabases(apikey *string, hopsworksKey *dal.HopsworksAPIKey) ([]string, error) {

	// caller holds the lock
	dbs, err := dal.GetUserProjects(hopsworksKey.UserID)
	if err != nil {
		return nil, err
	}

	return dbs, nil
}

func (hwc *Cache) Size() int {
	hwc.key2UserDBsCacheLock.RLock()
	defer hwc.key2UserDBsCacheLock.RUnlock()
	return len(hwc.key2UserDBsCache)
}

func (hwc *Cache) refreshIntervalWithJitter() time.Duration {
	refreshInterval := config.GetAll().Security.APIKey.CacheRefreshIntervalMS
	jitter := int32(config.GetAll().Security.APIKey.CacheRefreshIntervalJitterMS)
	jitter = hwc.randomGenerator.Int31n(jitter)
	if jitter%2 == 0 {
		jitter = -jitter
	}
	refreshInterval = refreshInterval + uint32(jitter)
	return time.Duration(refreshInterval) * time.Millisecond
}
