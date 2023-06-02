#!/bin/bash
set -e

TYPE=$1
SRC_DIR=$2
INSTALL_DIR=$3

if [[ -z "$TYPE" || -z "$SRC_DIR" || -z "$INSTALL_DIR" ]]; then
  echo "Error:  TYPE/ SRC_DIR / INSTALL_DIR not defined"
  exit 1
fi

if [[ "$TYPE" == "TRAIN" ]]; then

  echo "_____________ TRAIN BUILD _____________"

  cmake $SRC_DIR -DFPROFILE_GENERATE=1 \
    -DWITH_NDB=1 -DBUILD_CONFIG=mysql_release \
    -DWITH_SSL=${OPENSSL_ROOT} -DWITH_LDAP=system -DWITH_KERBEROS=system \
    -DWITH_SASL=system -DWITH_BOOST=${BOOST_ROOT} \
    -DWITH_NDB_TEST=1 -DWITH_UNIT_TESTS=1 -DWITH_RDRS=1 \
    -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR \
    -DCMAKE_BUILD_TYPE=Release

elif [[ "$TYPE" == "RELEASE" ]]; then

  echo "_____________ RELEASE BUILD _____________"

  cmake $SRC_DIR -DFPROFILE_USE=1 \
    -DWITH_NDB=1 -DBUILD_CONFIG=mysql_release \
    -DWITH_SSL=${OPENSSL_ROOT} -DWITH_SASL=system -DWITH_LDAP=system \
    -DWITH_KERBEROS=system -DWITH_BOOST=${BOOST_ROOT} \
    -DWITH_NDB_TEST=1 -DWITH_UNIT_TESTS=1 -DWITH_NDB_JAVA=1 \
    -DCPACK_MONOLITHIC_INSTALL=true -DWITH_RDRS=1 \
    -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR \
    -DCMAKE_BUILD_TYPE=Release

elif [[ "$TYPE" == "SIMPLE" ]]; then

  echo "_____________ SIMPLE BUILD _____________"

  cmake $SRC_DIR -DWITH_NDB=1 \
    -DBUILD_CONFIG=mysql_release \
    -DWITH_SSL=${OPENSSL_ROOT} -DWITH_SASL=system -DWITH_KERBEROS=system \
    -DWITH_LDAP=system -DWITH_BOOST=${BOOST_ROOT} \
    -DWITH_NDB_TEST=1 -DWITH_UNIT_TESTS=1 -DWITH_NDB_JAVA=1 -DWITH_RDRS=1 \
    -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR \
    -DCMAKE_BUILD_TYPE=Release

else
  echo "Error: Wrong build type"
  exit 1
fi
