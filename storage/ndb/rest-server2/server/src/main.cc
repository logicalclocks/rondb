/*
 * Copyright (C) 2023, 2024 Hopsworks AB, All rights reserved
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301,
 * USA.
 */

#include "connection.hpp"
#include "config_structs.hpp"
#include "rdrs_dal.h"
#include "json_parser.hpp"
#include "pk_read_ctrl.hpp"
#include "src/api_key.hpp"
#include "tls_util.hpp"

#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <sys/errno.h>
#include <thread>
#include <sstream>

int main(int argc, char *argv[]) {
  std::cout << "Starting server" << std::endl;

  jsonParser = JSONParser();
  apiKeyCache = std::make_shared<APIKeyCache>();
  
  /*
    Order:
    1. Read from ENV
        if no ENV:
    2. Set to defaults
    3. Read CLI arguments
        if no CLI:
    4. Set to defaults
  */
  RS_Status status = AllConfigs::init();
  if (status.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    errno = status.http_code;
    exit(errno);
  }
  
  for (int i = 1; i < argc; ++i) {
    if (strcmp(argv[i], "--root-ca-cert") == 0) {
      if (i + 1 < argc) {
        globalConfigs.security.tls.rootCACertFile = argv[++i];
      } else {
        std::cerr << "Error: --root-ca-cert option requires one argument." << std::endl;
        return 1;
      }
    } else if (strcmp(argv[i], "--cert-file") == 0) {
      if (i + 1 < argc) {
        globalConfigs.security.tls.certificateFile = argv[++i];
      } else {
        std::cerr << "Error: --cert-file option requires one argument." << std::endl;
        return 1;
      }
    } else if (strcmp(argv[i], "--key-file") == 0) {
      if (i + 1 < argc) {
        globalConfigs.security.tls.privateKeyFile = argv[++i];
      } else {
        std::cerr << "Error: --key-file option requires one argument." << std::endl;
        return 1;
      }
    }
  }

  // connect to rondb
  RonDBConnection rondbConnection(globalConfigs.ronDB,
                                  globalConfigs.ronDbMetaDataCluster);
  if (globalConfigs.security.tls.enableTLS) {
    status = GenerateTLSConfig(globalConfigs.security.tls.requireAndVerifyClientCert,
                               globalConfigs.security.tls.rootCACertFile,
                               globalConfigs.security.tls.certificateFile,
                               globalConfigs.security.tls.privateKeyFile);
    if (status.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
      errno = status.http_code;
      exit(errno);
    }
  }

  if (globalConfigs.grpc.enable) {
    errno = ENOSYS;
    exit(errno);
  }

  if (globalConfigs.rest.enable) {
    drogon::app().addListener(globalConfigs.rest.serverIP, globalConfigs.rest.serverPort, globalConfigs.security.tls.enableTLS, globalConfigs.security.tls.certificateFile, globalConfigs.security.tls.privateKeyFile);
    printf("Server running on %s:%d\n", globalConfigs.rest.serverIP.c_str(), globalConfigs.rest.serverPort);
    drogon::app().setThreadNum(globalConfigs.rest.numThreads);
    drogon::app().disableSession();
    drogon::app().run();
  }

  return 0;
}
