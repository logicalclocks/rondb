#!/bin/bash

set -e

RONDB_INSTALL_DIR=$1
CORES=$2

pushd . 
if [[ -z "${RDRS_VERSION}" ]]; then
  MY_SCRIPT_VARIABLE="Error: Env variable RDRS_VERSION not defined"
  exit 1
else
  # build native lib
  rm -rf rondb-rest-api-server
  git clone --branch $RDRS_VERSION https://github.com/logicalclocks/rondb-rest-api-server
  cd rondb-rest-api-server/data-access-rondb   
  mkdir -p build
  cd build
  cmake .. -DRONDB_DIR:STRING=$RONDB_INSTALL_DIR
  make -j$CORES
  cp librdrclient.so $RONDB_INSTALL_DIR/lib

  # build server 
  cd ../../rest-api-server
  make
  cp bin/server/rdrs $RONDB_INSTALL_DIR/bin

fi
popd
