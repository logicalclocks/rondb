#!/bin/bash
set -e

RONDB_VERSION=$1
TEMP_BUILD_DIR_ABS=$2
OUTPUT_DIR_ABS=$3

cd $TEMP_BUILD_DIR_ABS


rm -rf rondb-$RONDB_VERSION-linux-glibc2.17-x86_64
mv rondb_bin_use rondb-$RONDB_VERSION-linux-glibc2.17-x86_64

TAR_FILE="$TEMP_BUILD_DIR_ABS/rondb-$RONDB_VERSION-linux-glibc2.17-x86_64.tar.gz"
TAR_SRC_FOLDER="rondb-$RONDB_VERSION-linux-glibc2.17-x86_64"

set +e
which pigz > /dev/null
if [[ "$?" -ne "0" ]]; then
  tar cfzv  $TAR_FILE $TAR_SRC_FOLDER 
else
  tar -v -c --use-compress-program=pigz -f $TAR_FILE $TAR_SRC_FOLDER 
fi
set -e

rm -rf rondb-$RONDB_VERSION-linux-glibc2.17-x86_64
mv -f rondb-$RONDB_VERSION-linux-glibc2.17-x86_64.tar.gz $OUTPUT_DIR_ABS
