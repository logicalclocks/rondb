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
  drogon::HttpAppFramework &httpApp = drogon::app();

  httpApp.setSSLFiles(certFile, privateKeyFile);

  if (!requireClientCert) {
    return CRS_Status::SUCCESS.status;
  }

  /*
   * Fail here with a readable message if the CA file is missing or holds no
   * certificate. Otherwise the OpenSSL configuration commands below fail
   * when Drogon creates the listener, and that surfaces as an uncaught
   * exception out of drogon::app().run().
   */
  STACK_OF(X509_NAME) *caNames =
    SSL_load_client_CA_file(rootCACertFile.c_str());
  if (caNames == nullptr) {
    return CRS_Status(HTTP_CODE::SERVER_ERROR,
                      "cannot require client certificates: no CA certificate"
                      " could be read from `RootCACertFile` '" +
                      rootCACertFile + "'").status;
  }
  sk_X509_NAME_pop_free(caNames, X509_NAME_free);

  /*
   * Drogon gives no access to the TLS policy of its listeners, so client
   * certificates are set up with OpenSSL configuration commands. All three
   * are needed:
   * - VerifyMode Require makes the server ask for a client certificate and
   *   fail the handshake when the client presents none.
   * - VerifyCAFile is the CA that client certificates are verified against.
   *   Without it OpenSSL trusts no CA and refuses every client certificate.
   * - ClientCAFile only puts the CA name into the certificate request, so
   *   that the client can pick a matching certificate.
   * They must go in one call: setSSLConfigCommands() replaces the commands
   * of an earlier call.
   */
  std::vector<std::pair<std::string, std::string>> sslConfCmds;
  sslConfCmds.emplace_back("VerifyMode", "Require");
  sslConfCmds.emplace_back("VerifyCAFile", rootCACertFile);
  sslConfCmds.emplace_back("ClientCAFile", rootCACertFile);
  httpApp.setSSLConfigCommands(sslConfCmds);
  return CRS_Status::SUCCESS.status;
}
