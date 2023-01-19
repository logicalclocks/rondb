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

package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/log"
	"hopsworks.ai/rdrs/internal/servers"
	"hopsworks.ai/rdrs/version"
)

func main() {
	configFile := flag.String("config", "", "Configuration file path")
	ver := flag.Bool("version", false, "Print API and application version")
	flag.Parse()

	if configFile != nil && *configFile != "" {
		config.LoadConfig(*configFile)
	}

	if *ver == true {
		fmt.Printf("App version %s, API version %s\n", version.VERSION, version.API_VERSION)
		return
	}

	// TODO: Not sure if sharing a logger like this is best practice
	log.InitLogger(config.Configuration().Log)
	log.Infof("Current configuration: %s", config.Configuration())
	log.Infof("Starting Version : %s, Git Branch: %s (%s). Built on %s at %s",
		version.VERSION, version.BRANCH, version.GITCOMMIT, version.BUILDTIME, version.HOSTNAME)
	log.Infof("Starting API Version : %s", version.API_VERSION)

	runtime.GOMAXPROCS(config.Configuration().RestServer.GOMAXPROCS)

	// Wait for interrupt signal to gracefully shutdown the server
	quit := make(chan os.Signal)

	err, cleanupServers := servers.CreateAndStartDefaultServers(quit)
	defer cleanupServers()
	if err != nil {
		panic(err.Error())
	}

	/*
		kill (no param) default send syscall.SIGTERM
		kill -2 is syscall.SIGINT
		kill -9 is syscall.SIGKILL but can't be caught, so don't need to add it
	*/
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Info("Shutting down server...")
}
