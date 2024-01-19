/*
 * Copyright (C) 2023 Hopsworks AB
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

#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <sys/errno.h>
#include <thread>
#include <sstream>

int main() {
  // connect to rondb
  AllConfigs::setToDefaults();
  RonDBConnection rondbConnection(globalConfig.ronDB,
                                  globalConfig.ronDbMetaDataCluster);

  if (globalConfig.security.tls.enableTLS) {}

  if (globalConfig.grpc.enable) {
    errno = ENOSYS;
    exit(errno);
  }

  if (globalConfig.rest.enable) {
    drogon::app().addListener(globalConfig.rest.serverIP, globalConfig.rest.serverPort);
    printf("Server running on %s:%d\n", globalConfig.rest.serverIP.c_str(), globalConfig.rest.serverPort);
    drogon::app().setThreadNum(globalConfig.rest.numThreads);
    drogon::app().disableSession();
    drogon::app().run();
  }

  return 0;
}
