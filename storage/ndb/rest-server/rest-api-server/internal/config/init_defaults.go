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

package config

import (
	"os"
	"sync"

	"hopsworks.ai/rdrs/internal/log"
)

var globalConfig AllConfigs
var mutex sync.Mutex

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
		},
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
		Security: Security{
			EnableTLS:                        true,
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
	}
}
