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
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"

	"hopsworks.ai/rdrs/internal/log"
)

const CONFIG_FILE_NAME = "config.json"

var _config RSConfiguration

type RSConfiguration struct {
	RestServer  RestServer
	RonDBConfig RonDB
	MySQLServer MySQLServer
	Security    Security
	Log         log.LogConfig
}

func (config RSConfiguration) String() string {
	b, _ := json.MarshalIndent(_config, "", "\t")
	return fmt.Sprintf("%s", string(b))
}

type RestServer struct {
	RESTServerIP        string
	RESTServerPort      uint16
	GRPCServerIP        string
	GRPCServerPort      uint16
	BufferSize          int
	PreAllocatedBuffers uint32
	GOMAXPROCS          int
}

type MySQLServer struct {
	IP       string
	Port     uint16
	User     string
	Password string
}

type RonDB struct {
	IP   string
	Port uint16
}

type Security struct {
	EnableTLS                        bool
	RequireAndVerifyClientCert       bool
	CertificateFile                  string
	PrivateKeyFile                   string
	RootCACertFile                   string
	UseHopsWorksAPIKeys              bool
	HopsWorksAPIKeysCacheValiditySec int
}

func init() {
	restServer := RestServer{
		RESTServerIP:        "localhost",
		RESTServerPort:      4406,
		GRPCServerIP:        "localhost",
		GRPCServerPort:      5406,
		BufferSize:          320 * 1024,
		GOMAXPROCS:          -1,
		PreAllocatedBuffers: 1024,
	}

	ronDBConfig := RonDB{
		IP:   "localhost",
		Port: 1186,
	}

	mySQLServer := MySQLServer{
		IP:       "localhost",
		Port:     3306,
		User:     "rondb",
		Password: "rondb",
	}

	log := log.LogConfig{
		Level:      "warn",
		FilePath:   "",
		MaxSizeMB:  100,
		MaxBackups: 10,
		MaxAge:     30,
	}

	security := Security{
		EnableTLS:                        true,
		RequireAndVerifyClientCert:       true,
		CertificateFile:                  "",
		PrivateKeyFile:                   "",
		RootCACertFile:                   "",
		UseHopsWorksAPIKeys:              true,
		HopsWorksAPIKeysCacheValiditySec: 3,
	}

	_config = RSConfiguration{
		RestServer:  restServer,
		MySQLServer: mySQLServer,
		RonDBConfig: ronDBConfig,
		Security:    security,
		Log:         log,
	}

	configFile := os.Getenv("RDRS_CONFIG_FILE")
	if configFile != "" {
		err := LoadConfig(configFile)
		if err != nil {
			panic(err)
		}
	}
}

func LoadConfig(path string) error {
	jsonFile, err := os.Open(path)
	if err != nil {
		return err
	}
	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)

	err = json.Unmarshal([]byte(byteValue), &_config)
	if err != nil {
		return err
	}
	return validate()
}

func validate() error {
	if _config.Security.EnableTLS {
		if _config.Security.CertificateFile == "" ||
			_config.Security.PrivateKeyFile == "" {
			return errors.New("Server Certificate/Key not set")
		}
	}
	return nil
}

func PrintConfig() {
	b, _ := json.MarshalIndent(_config, "", "\t")
	log.Infof("Configuration loaded from file: %s\n", string(b))
}

func Configuration() *RSConfiguration {
	return &_config
}
