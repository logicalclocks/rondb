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

package config

import (
	"os"
	"sync"

	"hopsworks.ai/rdrs/internal/log"
)

var globalConfig AllConfigs
var mutex sync.Mutex

/*
Order:
 1. Read from ENV
    if no ENV:
 2. Set to defaults
 3. Read CLI arguments
    if no CLI:
 4. Set to defaults
*/
func init() {
	configFile := os.Getenv(CONFIG_FILE_PATH)
	err := SetFromFileIfExists(configFile)
	if err != nil {
		panic(err)
	}
}

func newWithDefaults() AllConfigs {
	return AllConfigs{
		Internal: Internal{
			BufferSize:          320 * 1024,
			GOMAXPROCS:          -1,
			PreAllocatedBuffers: 1024,
		},
		GRPC: GRPC{
			ServerIP:   "localhost",
			ServerPort: 4406,
		},
		REST: REST{
			ServerIP:   "localhost",
			ServerPort: 5406,
		},
		RonDB: RonDB{
			Mgmds: []Mgmd{
				{
					IP:   "localhost",
					Port: 1186,
				},
			},
			ConnectionPoolSize:            1,
			NodeIDs:                       []uint32{0},
			ConnectionRetries:             5,
			ConnectionRetryDelayInSec:     5,
			OpRetryOnTransientErrorsCount: 3,
			OpRetryInitialDelayInMS:       400,
		},
		Security: Security{
			EnableTLS:                        false,
			RequireAndVerifyClientCert:       false,
			CertificateFile:                  "",
			PrivateKeyFile:                   "",
			RootCACertFile:                   "",
			UseHopsworksAPIKeys:              true,
			HopsworksAPIKeysCacheValiditySec: 3,
		},
		Log: log.LogConfig{
			Level:      "warn",
			FilePath:   "",
			MaxSizeMB:  100,
			MaxBackups: 10,
			MaxAge:     30,
		},
		Testing: Testing{
			MySQL: MySQL{
				Servers: []MySQLServer{
					{
						IP:   "localhost",
						Port: 3306,
					},
				},
				User:     "rondb",
				Password: "rondb",
			},
		},
	}
}
