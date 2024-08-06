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

package feature_store

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/integrationtests"
	"hopsworks.ai/rdrs2/internal/log"
	"hopsworks.ai/rdrs2/internal/testutils"
)

/*
Wraps all unit tests in this package
*/
func TestMain(m *testing.M) {

	// We do this so that we can exit with error code 1 without discarding defer() functions
	retcode := 0
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("caught a panic in main(); making sure we are not returning with exit code 0;\nrecovery: %s\nstacktrace:\n%s", r, debug.Stack())
			retcode = 1
		}
		os.Exit(retcode)
	}()

	conf := config.GetAll()

	if !conf.REST.Enable {
		retcode = 0
		fmt.Println("--- SKIP: Unable to run tests for featurestore as REST interface is not enabled")
		return
	}

	log.InitLogger(conf.Log)

	cleanup, err := integrationtests.InitialiseTesting(conf)
	if err != nil {
		retcode = 1
		log.Fatalf(err.Error())
		return
	}
	defer cleanup()

	executable := testutils.GetExecutablePath()
	args := []string{
		"--root-ca-cert", config.GetAll().Security.TLS.RootCACertFile,
		"--cert-file", config.GetAll().Security.TLS.CertificateFile,
		"--key-file", config.GetAll().Security.TLS.PrivateKeyFile,
	}

	cmd := exec.Command(executable, args...)

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatalf("Failed to capture stdout: %v", err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		log.Fatalf("Failed to capture stderr: %v", err)
	}

	err = cmd.Start()
	if err != nil {
		log.Fatalf("Failed to start rdrs2 server: %v", err)
		os.Exit(1)
	}

	log.Infof("rdrs2 server started with PID %d", cmd.Process.Pid)

	stdoutScanner := bufio.NewScanner(stdoutPipe)
	stderrScanner := bufio.NewScanner(stderrPipe)

	serverReady := make(chan bool)

	go func() {
		for stdoutScanner.Scan() {
			line := stdoutScanner.Text()
			log.Info(line)
			if strings.Contains(line, "Server running") {
				serverReady <- true
			}
		}
	}()

	go func() {
		for stderrScanner.Scan() {
			line := stderrScanner.Text()
			log.Error(line)
		}
	}()

	select {
	case <-serverReady:
		log.Info("Server is ready to accept connections")
	case <-time.After(30 * time.Second):
		log.Fatal("Server did not start within 30 seconds")
		os.Exit(1)
	}

	defer func() {
		if err := cmd.Process.Kill(); err != nil {
			log.Errorf("Failed to kill rdrs2 server process: %v", err)
		} else {
			log.Infof("rdrs2 server process killed successfully")
		}
	}()

	retcode = m.Run()
}
