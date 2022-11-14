#!/bin/bash
set -e

RONDB_VERSION=$1
TEMP_BUILD_DIR_ABS=$2
OUTPUT_DIR_ABS=$3

cd $TEMP_BUILD_DIR_ABS

# The following code of how to determine OS and CPU architecture is not optimal.
# Essentially, we are trying to keep the naming conventions on repo.hops.works
#   Examples:
#   rondb-21.04.2-linux-glibc2.17-x86_64.tar.gz
#   rondb-21.04.2-macos-xcode-13.1-arm64_v8.tar.gz

# from https://stackoverflow.com/a/17072017/9068781
if [ "$(uname)" == "Darwin" ]; then
  OS="macos"
  # from https://stackoverflow.com/a/42144725/9068781
  XCODE_VERSION=$(pkgutil --pkg-info=com.apple.pkg.CLTools_Executables | grep version | sed -n "s/version\: *\([[:digit:]]*\.[[:digit:]]*\).*$/\1/p")
  STD_LIBRARY="xcode-${XCODE_VERSION}"
elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]; then
  OS="linux"
  GLIBC_VERSION=$(ldd --version | sed -n "s/.*Ubuntu GLIBC *\([[:digit:]]\.[[:digit:]][[:digit:]]\).*$/\1/p")
  STD_LIBRARY="glibc${GLIBC_VERSION}"
else
  echo "OS is neither Linux nor MacOS"
  STD_LIBRARY="unknown-os"
fi

# x86_64    on Ubuntu, Oraclelinux7, Mac
# aarch64   on Ubuntu, Oraclelinux7
# amd64     on Mac
CPU_ARCH=$(uname -m)

if [ "$CPU_ARCH" == "amd64" -o "$CPU_ARCH" == "x86_64" ]; then
  CPU_ARCH_FULL="x86_64"
elif [ "$CPU_ARCH" == "arm64" -o "$CPU_ARCH" == "aarch64" ]; then
  # ARMv8 introduced 64-bit
  CPU_ARCH_FULL="arm64_v8"
else
  echo "Cannot figure out the exact CPU architecture"
  exit 1
fi

TARBALL_NAME="rondb-${RONDB_VERSION}-${OS}-${STD_LIBRARY}-${CPU_ARCH_FULL}"

rm -rf $TARBALL_NAME
mv rondb_bin_use $TARBALL_NAME

TAR_FILE="$TEMP_BUILD_DIR_ABS/$TARBALL_NAME.tar.gz"
TAR_SRC_FOLDER=$TARBALL_NAME

set +e
which pigz >/dev/null
if [[ "$?" -ne "0" ]]; then
  # use gzip
  tar czvf $TAR_FILE $TAR_SRC_FOLDER
else
  # pigz uses multi-threading; has compatible compression to gzip, but
  # potentially a lot faster
  tar --use-compress-program=pigz -cvf $TAR_FILE $TAR_SRC_FOLDER
fi
set -e

rm -rf $TARBALL_NAME
mv -f $TARBALL_NAME.tar.gz $OUTPUT_DIR_ABS
