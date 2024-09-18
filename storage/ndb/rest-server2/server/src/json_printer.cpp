/*
 * Copyright (c) 2024, 2024, Hopsworks and/or its affiliates.
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

#include "json_printer.hpp"
#include "config_structs.hpp"

#include <iostream>

/*
 * Printing utilities
 */

#define INDENT_INCREASE 2

#define DEFINE_PRINTER(ValueDatatype, ...) \
  void printJson(ValueDatatype& value, std::ostream& out, [[maybe_unused]] uint32_t indent) __VA_ARGS__

#define INDENT() std::string(indent, ' ')
#define INDENT_INC() std::string(indent + INDENT_INCREASE, ' ')

#define DEFINE_ARRAY_PRINTER(ElementType) \
  DEFINE_PRINTER(std::vector<ElementType>, { \
    uint32_t len = value.size(); \
    out << "[\n"; \
    for (uint32_t i = 0; i < len; i++) { \
      out << INDENT_INC(); \
      printJson(value[i], out, indent + INDENT_INCREASE); \
      if (i < len - 1) { \
        out << ","; \
      } \
      out << "\n"; \
    } \
    out << INDENT() << "]"; \
  })

#define DEFINE_STRUCT_PRINTER(Datatype, ...) \
  DEFINE_PRINTER(Datatype, { \
    bool is_first_field = true; \
    out << "{"; \
    __VA_ARGS__ \
    out << '\n' << INDENT() << "}"; \
  })
#define ELEMENT(SourceVar, TargetKey) \
  out << (is_first_field ? "\n" : ",\n") \
      << INDENT_INC() << "\"" << #TargetKey << "\": "; \
  printJson(value.SourceVar, out, indent + INDENT_INCREASE); \
  is_first_field = false;


/*
 * End of printing utilities
 */

/*
 * Printers for simple datatypes
 */

DEFINE_PRINTER(bool, { out << (value ? "true" : "false"); })
DEFINE_PRINTER(uint64_t, { out << value; })
DEFINE_PRINTER(int64_t, { out << value; })
DEFINE_PRINTER(uint16_t, { out << value; })
DEFINE_PRINTER(uint32_t, { out << value; })
DEFINE_PRINTER(int, { out << value; })

DEFINE_PRINTER(std::string, {
  out << '"';
  for (char c : value) {
    switch (c) {
    case '"': out << "\\\""; break;
    case '\\': out << "\\\\"; break;
    case '\b': out << "\\b"; break;
    case '\f': out << "\\f"; break;
    case '\n': out << "\\n"; break;
    case '\r': out << "\\r"; break;
    case '\t': out << "\\t"; break;
    default:
      if (c < 0x20) {
        constexpr const char* hexdigits = "0123456789abcdef";
        out << "\\u00" << hexdigits[(c >> 4) & 0xf] << hexdigits[c & 0xf];
        break;
      }
      out << c;
      break;
    }
  }
  out << '"';
})

/*
 * Printers for the config structs. Make sure these correspond exactly to the
 * DEFINE_STRUCT_PARSER declarations in json_parser.cpp.
 */

DEFINE_STRUCT_PRINTER(Internal,
                      ELEMENT(reqBufferSize,       ReqBufferSize)
                      ELEMENT(respBufferSize,      RespBufferSize)
                      ELEMENT(preAllocatedBuffers, PreAllocatedBuffers)
                      ELEMENT(batchMaxSize,        BatchMaxSize)
                      ELEMENT(operationIdMaxSize,  OperationIDMaxSize)
                     )

DEFINE_STRUCT_PRINTER(REST,
                      ELEMENT(enable,     Enable)
                      ELEMENT(serverIP,   ServerIP)
                      ELEMENT(serverPort, ServerPort)
                     )

DEFINE_STRUCT_PRINTER(GRPC,
                      ELEMENT(enable,     Enable)
                      ELEMENT(serverIP,   ServerIP)
                      ELEMENT(serverPort, ServerPort)
                     )

DEFINE_STRUCT_PRINTER(Mgmd,
                      ELEMENT(IP,   IP)
                      ELEMENT(port, Port)
                     )

DEFINE_ARRAY_PRINTER(Mgmd)

DEFINE_ARRAY_PRINTER(uint32_t)

DEFINE_STRUCT_PRINTER(RonDB,
                      ELEMENT(Mgmds,                         Mgmds)
                      ELEMENT(connectionPoolSize,            ConnectionPoolSize)
                      ELEMENT(nodeIDs,                       NodeIDs)
                      ELEMENT(connectionRetries,             ConnectionRetries)
                      ELEMENT(connectionRetryDelayInSec,     ConnectionRetryDelayInSec)
                      ELEMENT(opRetryOnTransientErrorsCount, OpRetryOnTransientErrorsCount)
                      ELEMENT(opRetryInitialDelayInMS,       OpRetryInitialDelayInMS)
                      ELEMENT(opRetryJitterInMS,             OpRetryJitterInMS)
                     )

DEFINE_STRUCT_PRINTER(TestParameters,
                      ELEMENT(clientCertFile, ClientCertFile)
                      ELEMENT(clientKeyFile,  ClientKeyFile)
                     )

DEFINE_STRUCT_PRINTER(TLS,
                      ELEMENT(enableTLS,                  EnableTLS)
                      ELEMENT(requireAndVerifyClientCert, RequireAndVerifyClientCert)
                      ELEMENT(certificateFile,            CertificateFile)
                      ELEMENT(privateKeyFile,             PrivateKeyFile)
                      ELEMENT(rootCACertFile,             RootCACertFile)
                      ELEMENT(testParameters,             TestParameters)
                     )

DEFINE_STRUCT_PRINTER(APIKey,
                      ELEMENT(useHopsworksAPIKeys,          UseHopsworksAPIKeys)
                      ELEMENT(cacheRefreshIntervalMS,       CacheRefreshIntervalMS)
                      ELEMENT(cacheUnusedEntriesEvictionMS, CacheUnusedEntriesEvictionMS)
                      ELEMENT(cacheRefreshIntervalJitterMS, CacheRefreshIntervalJitterMS)
                     )

DEFINE_STRUCT_PRINTER(Security,
                      ELEMENT(tls,    TLS)
                      ELEMENT(apiKey, APIKey)
                     )

DEFINE_STRUCT_PRINTER(LogConfig,
                      ELEMENT(level,      Level)
                      ELEMENT(filePath,   FilePath)
                      ELEMENT(maxSizeMb,  MaxSizeMB)
                      ELEMENT(maxBackups, MaxBackups)
                      ELEMENT(maxAge,     MaxAge)
                     )

DEFINE_STRUCT_PRINTER(MySQLServer,
                      ELEMENT(IP,   IP)
                      ELEMENT(port, Port)
                     )

DEFINE_ARRAY_PRINTER(MySQLServer)

DEFINE_STRUCT_PRINTER(MySQL,
                      ELEMENT(servers,  Servers)
                      ELEMENT(user,     User)
                      ELEMENT(password, Password)
                     )

DEFINE_STRUCT_PRINTER(Testing,
                      ELEMENT(mySQL,                MySQL)
                      ELEMENT(mySQLMetadataCluster, MySQLMetadataCluster)
                     )

DEFINE_STRUCT_PRINTER(AllConfigs,
                      ELEMENT(internal,             Internal)
                      ELEMENT(rest,                 REST)
                      ELEMENT(grpc,                 GRPC)
                      ELEMENT(pidfile,              PIDFile)
                      ELEMENT(ronDB,                RonDB)
                      ELEMENT(ronDbMetaDataCluster, RonDBMetadataCluster)
                      ELEMENT(security,             Security)
                      ELEMENT(log,                  Log)
                      ELEMENT(testing,              Testing)
                     )
