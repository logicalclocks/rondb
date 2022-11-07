#!/bin/bash
set -e

RONDB_VERSION=$1
TEMP_BUILD_DIR_ABS=$2
OUTPUT_DIR_ABS=$3

cd $TEMP_BUILD_DIR_ABS

# from https://stackoverflow.com/a/17072017/9068781
if [ "$(uname)" == "Darwin" ]; then
  OS="macos"
  # from https://stackoverflow.com/a/42144725/9068781
  XCODE_VERSION=$(pkgutil --pkg-info=com.apple.pkg.CLTools_Executables | grep version | sed -n "s/version\: *\([[:digit:]]*\.[[:digit:]]*\).*$/\1/p")
  COMPILER="xcode-${XCODE_VERSION}"
  CPU_ARCH=$(uname -m)  # amd64 / arm64
elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]; then
  OS="linux"
  GLIBC_VERSION=$(ldd --version | sed -n "s/.*Ubuntu GLIBC *\([[:digit:]]\.[[:digit:]][[:digit:]]\).*$/\1/p")
  COMPILER="glibc${GLIBC_VERSION}"
  CPU_ARCH=$(dpkg --print-architecture)  # amd64 / arm64
else
  echo "OS is neither Linux nor MacOS"
  exit 1
fi

# We are converting here to keep the naming convention on repo.hops.works
#   Examples:
#   rondb-21.04.2-linux-glibc2.17-x86_64.tar.gz
#   rondb-21.04.2-macos-xcode-13.1-arm64_v8.tar.gz
echo "WARNING: No solution figure out how to figure out OS variant"
if [ "$CPU_ARCH" == "amd64" ]; then
  CPU_ARCH_FULL="x86_64"
elif [ "$CPU_ARCH" == "arm64" ]; then
  echo "WARNING: Guessing that we are running with arm64_v8"
  CPU_ARCH_FULL="arm64_v8"
else
  echo "Cannot figure out the exact CPU architecture"
  exit 1
fi

TARBALL_NAME="rondb-${RONDB_VERSION}-${OS}-${COMPILER}-${CPU_ARCH_FULL}"

rm -rf $TARBALL_NAME
mv rondb_bin_use $TARBALL_NAME

TAR_FILE="$TEMP_BUILD_DIR_ABS/$TARBALL_NAME.tar.gz"
TAR_SRC_FOLDER=$TARBALL_NAME

set +e
which pigz >/dev/null
if [[ "$?" -ne "0" ]]; then
  tar cfzv $TAR_FILE $TAR_SRC_FOLDER
else
  tar -v -c --use-compress-program=pigz -f $TAR_FILE $TAR_SRC_FOLDER
fi
set -e

rm -rf $TARBALL_NAME
mv -f $TARBALL_NAME.tar.gz $OUTPUT_DIR_ABS
