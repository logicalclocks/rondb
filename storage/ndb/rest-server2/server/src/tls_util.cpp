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

std::tuple<std::shared_ptr<X509_STORE>, RS_Status> appendCertToPool(const std::string &certFile,
                                                                    X509_STORE *store) {
  std::ifstream file(certFile, std::ios::binary | std::ios::ate);
  if (!file.is_open()) {
    return {
        nullptr,
        CRS_Status(HTTP_CODE::SERVER_ERROR, "Failed to open certificate file: " + certFile).status};
  }

  std::streamsize size = file.tellg();
  file.seekg(0, std::ios::beg);

  std::vector<char> buffer(size);
  if (!file.read(buffer.data(), size)) {
    return {
        nullptr,
        CRS_Status(HTTP_CODE::SERVER_ERROR, "Failed to read certificate file: " + certFile).status};
  }

  const unsigned char *certs = reinterpret_cast<const unsigned char *>(buffer.data());
  X509 *cert                 = d2i_X509(nullptr, &certs, size);
  if (cert == nullptr) {
    return {nullptr,
            CRS_Status(HTTP_CODE::SERVER_ERROR, "Failed to parse certificate: " + certFile).status};
  }

  if (X509_STORE_add_cert(store, cert) != 1) {
    X509_free(cert);
    return {nullptr,
            CRS_Status(HTTP_CODE::SERVER_ERROR, "Failed to add certificate to store: " + certFile)
                .status};
  }
  X509_free(cert);

  return {std::shared_ptr<X509_STORE>(store, X509_STORE_free), CRS_Status().status};
}

std::tuple<std::shared_ptr<X509_STORE>, RS_Status> TrustedCAs(const std::string &rootCACertFile) {
  auto *rootCAs = X509_STORE_new();
  if (rootCAs == nullptr) {
    return {nullptr, CRS_Status(HTTP_CODE::SERVER_ERROR, "Failed to create X509 store").status};
  }

  auto result = appendCertToPool(rootCACertFile, rootCAs);
  if (std::get<1>(result).code != HTTP_CODE::SUCCESS) {
    X509_STORE_free(rootCAs);
    return result;
  }

  return {std::shared_ptr<X509_STORE>(rootCAs, X509_STORE_free), CRS_Status().status};
}

RS_Status GenerateTLSConfig(bool requireClientCert, const std::string &rootCACertFile,
                            const std::string &certFile, const std::string &privateKeyFile) {
  std::cout << "Setting up TLS" << std::endl;
  auto &httpApp = drogon::app();
  std::cout << "Before setting SSL files" << std::endl;
  
  httpApp.setSSLFiles(certFile, privateKeyFile);
  std::cout << "Cert File: " << certFile << std::endl;

  if (requireClientCert) {
    httpApp.setSSLConfigCommands({{"VerifyMode", "Require"}});
    std::cout << "Client Cert Required" << std::endl;
  }

  std::cout << "Root CA Cert File: " << rootCACertFile << std::endl;

  if (!rootCACertFile.empty()) {
    auto [rootCAs, status] = TrustedCAs(rootCACertFile);
    std::cout << "Root CAs: " << rootCAs << std::endl;
    if (status.code != drogon::k200OK) {
      return status;
    }
  }
  return CRS_Status().status;
}