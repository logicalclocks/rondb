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
// TODO: Check if new naming is correct
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
