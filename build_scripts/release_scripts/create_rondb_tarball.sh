#!/bin/bash
set -e

TARBALL_NAME=$1
TEMP_BUILD_DIR_ABS=$2
OUTPUT_DIR_ABS=$3

TAR_FILE="$TEMP_BUILD_DIR_ABS/$TARBALL_NAME.tar.gz"

cd $TEMP_BUILD_DIR_ABS

rm -rf $TARBALL_NAME
mv rondb_bin_use $TARBALL_NAME

set +e
which pigz >/dev/null
if [[ "$?" -ne "0" ]]; then
  # use gzip
  tar czvf $TAR_FILE $TARBALL_NAME
else
  # pigz uses multi-threading; has compatible compression to gzip, but
  # potentially a lot faster
  tar --use-compress-program=pigz -cvf $TAR_FILE $TARBALL_NAME
fi
set -e

rm -rf $TARBALL_NAME
mv -f $TAR_FILE $OUTPUT_DIR_ABS
