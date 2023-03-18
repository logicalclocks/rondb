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
	"errors"
	"fmt"
	"strings"

	"hopsworks.ai/rdrs/internal/dal"
	"hopsworks.ai/rdrs/internal/security/apikey/authcache"
)

/*
	Checking whether the API key can access the given databases
*/
func ValidateAPIKey(apiKey *string, dbs ...*string) error {
	err := validateApiKeyFormat(apiKey)
	if err != nil {
		return err
	}

	if len(dbs) < 1 || dbs == nil {
		return nil
	}

	keyFoundInCache, allowedAccess := authcache.FindAndValidateCache(apiKey, dbs...)
	if keyFoundInCache {
		if !allowedAccess {
			return errors.New("unauthorized: no access to db registered in cache")
		}
		return nil
	}

	hopsKey, err := authenticateUserRemote(apiKey)
	if err != nil {
		return err
	}

	_, err = getUserDatabasesRemote(apiKey, hopsKey)
	if err != nil {
		return err
	}

	keyFoundInCache, allowedAccess = authcache.FindAndValidateCache(apiKey, dbs...)
	if !keyFoundInCache || !allowedAccess {
		return errors.New("unauthorized: no access to db registered")
	}
	return nil
}

func validateApiKeyFormat(apiKey *string) error {
	if apiKey == nil {
		return errors.New("the apikey is nil")
	}
	splits := strings.Split(*apiKey, ".")
	if len(splits) != 2 || len(splits[0]) != 16 || len(splits[1]) < 1 {
		return errors.New("the apikey has an incorrect format")
	}
	return nil
}

func authenticateUserRemote(apiKey *string) (*dal.HopsworksAPIKey, error) {
	splits := strings.Split(*apiKey, ".")
	prefix := splits[0]
	secret := splits[1]

	key, err := dal.GetAPIKey(prefix)
	if err != nil {
		return nil, err
	}

	//sha256(client.secret + db.salt) = db.secret
	newSecret := sha256.Sum256([]byte(secret + key.Salt))
	newSecretHex := fmt.Sprintf("%x", newSecret)
	if strings.Compare(string(newSecretHex), key.Secret) != 0 {
		return nil, errors.New("wrong API Key")
	}
	return key, nil
}

// This fetches the databases from the DB and updates the cache
func getUserDatabasesRemote(apikey *string, hopsworksKey *dal.HopsworksAPIKey) ([]string, error) {
	dbs, err := dal.GetUserProjects(hopsworksKey.UserID)
	if err != nil {
		return nil, err
	}
	authcache.Set(*apikey, dbs)
	return dbs, nil
}
