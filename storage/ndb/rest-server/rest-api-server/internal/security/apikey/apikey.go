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
	"errors"
	"fmt"
	"strings"
	"time"

	"hopsworks.ai/rdrs/internal/security/apikey/apikey_cache"
	"hopsworks.ai/rdrs/internal/security/apikey/apikey_cache/hw_apikey_cache"
)

type APIKeyCacher interface {
	ValidateAPIKey(apiKey *string, dbs ...*string) error
	Cleanup() error
	LastUpdated(apiKey *string) time.Time
	LastUsed(apiKey *string) time.Time
	Size() int
}

type APIKeyCache struct {
	HWAPIKeyCache apikey_cache.HWAPIKeyCache
}

var _ APIKeyCacher = (*APIKeyCache)(nil)

func NewAPIKeyCache() (APIKeyCacher, error) {
	cache := APIKeyCache{HWAPIKeyCache: hw_apikey_cache.NewAPIKeyCache()}
	return &cache, nil
}

/*
Checking whether the API key can access the given databases
*/
func (apic *APIKeyCache) ValidateAPIKey(apiKey *string, dbs ...*string) error {
	err := apic.validateApiKeyFormat(apiKey)
	if err != nil {
		return err
	}

	if dbs == nil || len(dbs) == 0 {
		return nil
	}

	// Authenticates only using the the cache. No request sent to backend
	keyFoundInCache, allowedAccess := apic.HWAPIKeyCache.FindAndValidate(apiKey, dbs...)
	if keyFoundInCache {
		if !allowedAccess {
			return errors.New(fmt.Sprintf("unauthorized. Found in cache: %v, allowed access %v",
				keyFoundInCache, allowedAccess))
		}
		return nil
	}

	// update the cache by fetching the API key from backend
	if err = apic.HWAPIKeyCache.UpdateCache(apiKey); err != nil {
		return err
	}

	// Authenticates only using the the cache. No request sent to backend
	keyFoundInCache, allowedAccess = apic.HWAPIKeyCache.FindAndValidate(apiKey, dbs...)
	if !keyFoundInCache || !allowedAccess {
		return errors.New(fmt.Sprintf("unauthorized. After cache update. Found in cache: %v, allowed access %v",
			keyFoundInCache, allowedAccess))
	}
	return nil
}

func (apic *APIKeyCache) validateApiKeyFormat(apiKey *string) error {
	if apiKey == nil {
		return errors.New("the apikey is nil")
	}
	splits := strings.Split(*apiKey, ".")
	if len(splits) != 2 || len(splits[0]) != 16 || len(splits[1]) < 1 {
		return errors.New("the apikey has an incorrect format")
	}
	return nil
}

func (apic *APIKeyCache) Cleanup() error {
	return apic.HWAPIKeyCache.Cleanup()
}

// only for testing
func (apic *APIKeyCache) LastUpdated(apiKey *string) time.Time {
	return apic.HWAPIKeyCache.LastUpdated(apiKey)
}

func (apic *APIKeyCache) LastUsed(apiKey *string) time.Time {
	return apic.HWAPIKeyCache.LastUsed(apiKey)
}

func (apic *APIKeyCache) Size() int {
	return apic.HWAPIKeyCache.Size()
}
