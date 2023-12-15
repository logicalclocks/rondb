#include <chrono>
#include <cmath>
#include <cstdio>
#include <iostream>
#include <memory>
#include <thread>
#include "connection.hpp"
#include "config_structs.hpp"
#include "src/rdrs-dal.h"
#include <sstream>
#include "error.hpp"
#include "json_parser.hpp"
#include "pk_read_ctrl.hpp"

int main() {
  // connect to rondb
  AllConfigs::setToDefaults();
  RonDBConnection rondbConnection(globalConfig.ronDB,
                                  globalConfig.ronDbMetaDataCluster);

  if (globalConfig.security.tls.enableTLS) {}

  if (globalConfig.grpc.enable) {}

  if (globalConfig.rest.enable) {
    printf("Server running on 0.0.0.0:5406\n");
    app().addListener("0.0.0.0", 5406);
    app().setThreadNum(MAX_THREADS);
    app().disableSession();
    app().run();
  }

  return 0;
}
