/*
 * Copyright (C) 2024 Hopsworks AB
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

#include "tls_util.hpp"
#include "rdrs_dal.hpp"

#include <drogon/HttpAppFramework.h>
#include <drogon/HttpTypes.h>
#include <fstream>
#include <iostream>
#include <openssl/ssl.h>
#include <vector>
#include <memory>
#include <drogon/drogon.h>

RS_Status GenerateTLSConfig(bool requireClientCert, const std::string &rootCACertFile,
                            const std::string &certFile, const std::string &privateKeyFile) {
  auto &httpApp = drogon::app();

  httpApp.setSSLFiles(certFile, privateKeyFile);

  if (requireClientCert) {
    httpApp.setSSLConfigCommands({{"VerifyMode", "Require"}});
  }

  if (!rootCACertFile.empty()) {
    httpApp.setSSLConfigCommands({{"ClientCAFile", rootCACertFile}});
  }
  return CRS_Status::SUCCESS.status;
}
