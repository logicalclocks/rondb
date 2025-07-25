/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2025 Hopsworks AB
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

package feature_store

import (
	"math/rand"
	"sync"
	"time"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/log"
)

// Cache Entry
type CachedEntry struct {
	fvMetadata         *FeatureViewMetadata // Metadata
	featureStoreName   string
	featureViewName    string
	featureViewVersion int
	lastUsed           time.Time     // for removing unused entries
	lastUpdated        time.Time     // last updated TS
	rowLock            sync.RWMutex  // this is used to prevent concurrent updates
	ticker             *time.Ticker  // ticker is used to keep the cache entry updated
	evicted            bool          // is evicted or deleted
	refreshInterval    time.Duration // Cache refresh interval
	err                *RestErrorCode
}

type FeatureViewMetaDataCache struct {
	cachedEntries   map[string]*CachedEntry
	fvMeataDataLock sync.RWMutex
	randomGenerator *rand.Rand
}

func NewFeatureViewMetaDataCache() *FeatureViewMetaDataCache {
	if log.IsInfo() {
		log.Info("Started Medatadata Cache")
	}
	someRand := rand.New(rand.NewSource(time.Now().Unix()))
	return &FeatureViewMetaDataCache{
		cachedEntries:   make(map[string]*CachedEntry),
		randomGenerator: someRand,
	}
}

func (hwc *FeatureViewMetaDataCache) Cleanup() error {
	hwc.fvMeataDataLock.Lock()
	defer hwc.fvMeataDataLock.Unlock()

	if log.IsInfo() {
		log.Info("Shutting down metadata cache")
	}

	for _, udbs := range hwc.cachedEntries {
		udbs.ticker.Stop()
		udbs.evicted = true
	}

	hwc.cachedEntries = make(map[string]*CachedEntry)

	return nil
}

// update the cache entry by fetching the metadata from backend
func (fvmdc *FeatureViewMetaDataCache) UpdateCache(featureStoreName, featureViewName string, featureViewVersion int) error {

	// if the entry does not already exist in the
	// cache then multiple clients will try to read and
	// update the key from the backend simultaneously.
	// Trying to prevent multiple writers here

	// first check using read lock
	var fvKey = getFeatureViewCacheKey(featureStoreName, featureViewName, featureViewVersion)
	fvmdc.fvMeataDataLock.RLock()
	_, ok := fvmdc.cachedEntries[fvKey]
	fvmdc.fvMeataDataLock.RUnlock()

	if !ok {
		// Continue with write lock
		fvmdc.fvMeataDataLock.Lock()

		_, ok := fvmdc.cachedEntries[fvKey]
		if !ok { // the entry still does not exists. insert a new row
			entry := &CachedEntry{}
			entry.featureStoreName = featureStoreName
			entry.featureViewName = featureViewName
			entry.featureViewVersion = featureViewVersion
			entry.refreshInterval = fvmdc.refreshIntervalWithJitter()
			fvmdc.cachedEntries[fvKey] = entry
			fvmdc.startUpdateTicker(fvKey, entry)
		}
		fvmdc.fvMeataDataLock.Unlock()
	}

	// the entry already exists. Ticker will update it
	return nil
}

func (fvmdc *FeatureViewMetaDataCache) startUpdateTicker(fvKey string, entry *CachedEntry) error {
	started := false
	go fvmdc.cacheEntryUpdater(fvKey, &started)

	//wait for go routine to start
	for {
		if started {
			if log.IsDebug() {
				log.Debugf("Feature view cache updater is started for %s. Refresh Interval: %v ", fvKey, entry.refreshInterval)
			}
			return nil
		} else {
			time.Sleep(50 * time.Microsecond)
		}
	}
}

func (fvmdc *FeatureViewMetaDataCache) cacheEntryUpdater(fvKey string, started *bool) {

	entry, ok := fvmdc.cachedEntries[fvKey] // no need for read lock here as the caller holds write lock
	if !ok {
		log.Errorf("Cache updater failed. Report programming error. Key %s", fvKey)
		return
	}

	entry.ticker = time.NewTicker(entry.refreshInterval)

	cleaner := func() {
		//clean up on eviction
		if log.IsDebug() {
			log.Debugf("Metadata evicted. Key %s", fvKey)
		}
		entry.ticker.Stop()
		entry.evicted = true
		fvmdc.fvMeataDataLock.Lock()
		delete(fvmdc.cachedEntries, fvKey)
		fvmdc.fvMeataDataLock.Unlock()
	}

	for {

		// read the out of the lock block
		fvmd, beErr := GetFeatureViewMetadata(entry.featureStoreName,
			entry.featureViewName, entry.featureViewVersion)

		entry.rowLock.Lock()
		*started = true

		if !entry.evicted {
			if beErr != nil {
				log.Infof("Cache updater failed to read FS Metadata.  Key: %s, Error: %v", fvKey, beErr)
				entry.err = beErr
			} else {
				entry.fvMetadata = fvmd
				entry.lastUpdated = time.Now()
			}
		}

		entry.rowLock.Unlock()

		<-entry.ticker.C

		if entry.evicted {
			//no need for cleanup as evicter cleans up
			return
		}

		// if the entry has not been used for some time the evict this
		entry.rowLock.RLock()
		lastUsed := entry.lastUsed
		entry.rowLock.RUnlock()

		evictTime := time.Duration(config.GetAll().FeatureStore.FeatureStoreMetadataCache.CacheUnusedEntriesEvictionMS) * time.Duration(time.Millisecond)
		if lastUsed.Add(evictTime).Before(time.Now()) {
			cleaner()
			return
		}
	}
}

func (fvmdc *FeatureViewMetaDataCache) getInt(featureStoreName, featureViewName string, featureViewVersion int) (*FeatureViewMetadata, *RestErrorCode) {

	var fvKey = getFeatureViewCacheKey(featureStoreName, featureViewName, featureViewVersion)
	fvmdc.fvMeataDataLock.RLock()
	fvmde, ok := fvmdc.cachedEntries[fvKey]
	fvmdc.fvMeataDataLock.RUnlock()

	if !ok {
		return nil, FS_NOT_EXIST
	}

	fvmde.rowLock.RLock()
	// update TS
	fvmde.lastUsed = time.Now()
	fvmde.rowLock.RUnlock()

	if fvmde.fvMetadata == nil {
		if fvmde.err != nil {
			return nil, fvmde.err
		} else {
			return nil, FETCH_METADATA_FROM_CACHE_FAIL
		}
	} else {
		return fvmde.fvMetadata, nil
	}
}

func (fvmdc *FeatureViewMetaDataCache) Get(featureStoreName, featureViewName string, featureViewVersion int) (*FeatureViewMetadata, *RestErrorCode) {

	fvmd, err := fvmdc.getInt(featureStoreName, featureViewName, featureViewVersion)
	if err == nil {
		return fvmd, nil
	}

	if err != nil && (err != FS_NOT_EXIST && err != FV_NOT_EXIST) {
		return nil, err
	}

	if err != nil && (err == FS_NOT_EXIST || err == FV_NOT_EXIST) {
		fvmdc.UpdateCache(featureStoreName, featureViewName, featureViewVersion)
	}

	fvmd, err = fvmdc.getInt(featureStoreName, featureViewName, featureViewVersion)
	return fvmd, err
}

// Just for testing..
func (fvmdc *FeatureViewMetaDataCache) LastUsed(fvKey string) time.Time {
	fvmdc.fvMeataDataLock.RLock()
	defer fvmdc.fvMeataDataLock.RUnlock()
	entry, ok := fvmdc.cachedEntries[fvKey]
	if ok {
		return entry.lastUsed
	} else {
		return time.Unix(0, 0)
	}
}

func (fvmdc *FeatureViewMetaDataCache) LastUpdated(fvKey string) time.Time {
	fvmdc.fvMeataDataLock.RLock()
	defer fvmdc.fvMeataDataLock.RUnlock()
	entry, ok := fvmdc.cachedEntries[fvKey]
	if ok {
		return entry.lastUpdated
	} else {
		return time.Unix(0, 0)
	}
}

func (fvmdc *FeatureViewMetaDataCache) Size() int {
	fvmdc.fvMeataDataLock.RLock()
	defer fvmdc.fvMeataDataLock.RUnlock()
	return len(fvmdc.cachedEntries)
}

func (hwc *FeatureViewMetaDataCache) refreshIntervalWithJitter() time.Duration {
	refreshInterval := config.GetAll().FeatureStore.FeatureStoreMetadataCache.CacheRefreshIntervalMS
	jitter := int32(config.GetAll().FeatureStore.FeatureStoreMetadataCache.CacheRefreshIntervalJitterMS)
	jitter = hwc.randomGenerator.Int31n(jitter)
	if jitter%2 == 0 {
		jitter = -jitter
	}
	refreshInterval = refreshInterval + uint32(jitter)
	return time.Duration(refreshInterval) * time.Millisecond
}
