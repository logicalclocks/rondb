/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2024 Hopsworks AB
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

package testutils

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/log"
)

func GetExecutablePath() string {
	workingDir, err := os.Getwd()
	if err != nil {
		log.Fatalf("failed to get working directory: %v", err)
	}

	executablePath := filepath.Join(workingDir, "..", "..", "..", "..", "..", "..", "..", "..", "build", "storage", "ndb", "rest-server2", "server", "src", "rdrs2")

	return executablePath
}

// StartServer starts the rdrs2 server and returns a cleanup function.
func StartServer() (cleanup func(), err error) {
	conf := config.GetAll()

	executable := GetExecutablePath()
	args := []string{
		"--root-ca-cert", conf.Security.TLS.RootCACertFile,
		"--cert-file", conf.Security.TLS.CertificateFile,
		"--key-file", conf.Security.TLS.PrivateKeyFile,
	}

	cmd := exec.Command(executable, args...)

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to capture stdout: %v", err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to capture stderr: %v", err)
	}

	err = cmd.Start()
	if err != nil {
		return nil, fmt.Errorf("failed to start rdrs2 server: %v", err)
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
		return nil, fmt.Errorf("server did not start within 30 seconds")
	}

	cleanup = func() {
		if err := cmd.Process.Kill(); err != nil {
			log.Errorf("failed to kill rdrs2 server process: %v", err)
		} else {
			log.Infof("rdrs2 server process killed successfully")
		}
	}

	return cleanup, nil
}
