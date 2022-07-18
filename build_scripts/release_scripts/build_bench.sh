#!/bin/bash

set -e

BUILD_DIR=$1
RONDB_INSTALL_DIR=$2
CORES=$3

#_____________ SYSBENCH_VERSION _____________
if [[ -z "${SYSBENCH_VERSION}" ]]; then
  MY_SCRIPT_VARIABLE="Error: Env variable SYSBENCH_VERSION not defined"
  exit 1
else
  rm -rf ${SYSBENCH_VERSION}*
  wget https://repo.hops.works/master/${SYSBENCH_VERSION}.tar.gz
  tar xfz ${SYSBENCH_VERSION}.tar.gz
  cd ${SYSBENCH_VERSION}
  ./configure --with-mysql=$RONDB_INSTALL_DIR
  make -j$CORES
  cd ..
fi

#_____________ DBT2_VERSION _____________
if [[ -z "${DBT2_VERSION}" ]]; then
  MY_SCRIPT_VARIABLE="Error: Env variable DBT2_VERSION not defined"
  exit 1
else
  rm -rf ${DBT2_VERSION}*
  wget https://repo.hops.works/master/${DBT2_VERSION}.tar.gz
  tar xfz ${DBT2_VERSION}.tar.gz
  cd ${DBT2_VERSION}
  ./configure --with-mysql=$RONDB_INSTALL_DIR
  make -j$CORES
  cd ..
fi

#_____________ DBT3_VERSION _____________
if [[ -z "${DBT3_VERSION}" ]]; then
  MY_SCRIPT_VARIABLE="Error: Env variable DBT3_VERSION not defined"
  exit 1
else
  rm -rf ${DBT3_VERSION}*
  wget https://repo.hops.works/master/${DBT3_VERSION}.tar.gz 
  tar xfz ${DBT3_VERSION}.tar.gz
  cd ${DBT3_VERSION}
  ./configure --with-mysql=$RONDB_INSTALL_DIR
  make -j$CORES
  cd ..
fi


#_____________ Copy Files _____________
cd $RONDB_INSTALL_DIR
cd bin

mkdir -p dbt2
mkdir -p sysbench
mkdir -p dbt3
cp /$BUILD_DIR/${SYSBENCH_VERSION}/sysbench/sysbench sysbench/sysbench
cp /$BUILD_DIR/${DBT2_VERSION}/src/client dbt2/client
cp /$BUILD_DIR/${DBT2_VERSION}/src/driver dbt2/driver
cp /$BUILD_DIR/${DBT2_VERSION}/src/datagen dbt2/datagen
cp /$BUILD_DIR/${DBT2_VERSION}/scripts/bench_run.sh bench_run.sh
cp /$BUILD_DIR/${DBT3_VERSION}/src/dbgen/dbgen dbt3/dbgen
cp /$BUILD_DIR/${DBT3_VERSION}/src/dbgen/qgen dbt3/qgen
cd /$BUILD_DIR
rm -rf ${SYSBENCH_VERSION}
rm -rf ${DBT2_VERSION}
rm -rf ${DBT3_VERSION}
tar xfz ${DBT2_VERSION}.tar.gz
mv ${DBT2_VERSION} $RONDB_INSTALL_DIR/dbt2_install
