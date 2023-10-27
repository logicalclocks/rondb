#!/bin/bash
set -e

TYPE=$1
SRC_DIR=$2
INSTALL_DIR=$3

if [[ -z "$TYPE" || -z "$SRC_DIR" || -z "$INSTALL_DIR" ]]; then
  echo "Error:  TYPE/ SRC_DIR / INSTALL_DIR not defined"
  exit 1
fi

ADDITIONAL_FLAGS=""

# Enabling additional security checks in standard library functions to protect against buffer 
# overflows and other vulnerabilities.
ADDITIONAL_FLAGS="${ADDITIONAL_FLAGS} -Wp,-D_FORTIFY_SOURCE=2"

# -pipe. This flag tells the compiler to use pipes rather than temporary files for communication 
# between the various stages of compilation, potentially speeding up the compilation process.
ADDITIONAL_FLAGS="${ADDITIONAL_FLAGS} -pipe"

# -fexceptions: This flag enables the use of exceptions in C++ code. It allows the code to 
# throw and catch exceptions.
ADDITIONAL_FLAGS="${ADDITIONAL_FLAGS} -fexceptions"

# -fstack-protector-strong: This is a security feature that adds protection against stack buffer
# overflows. It inserts checks to prevent stack buffer overflows by detecting corruption of 
# function return addresses.
ADDITIONAL_FLAGS="${ADDITIONAL_FLAGS} -fstack-protector-strong"

# -grecord-gcc-switches: This flag instructs the compiler to record the command-line 
# options used to compile the source code in the produced binary.
ADDITIONAL_FLAGS="${ADDITIONAL_FLAGS} -grecord-gcc-switches"

# -specs=/usr/lib/rpm/redhat/redhat-hardened-cc1: This specifies a spec file used in the 
# Red Hat Linux distribution, which contains predefined settings for the compiler,
# including security hardening options.
HARDENED_CC1="/usr/lib/rpm/redhat/redhat-hardened-cc1"
if test -f "$HARDENED_CC1"; then
  ADDITIONAL_FLAGS="${ADDITIONAL_FLAGS} -specs=${HARDENED_CC1}"
fi

#-m64: This flag specifies that the code should be compiled for a 64-bit target platform.
ADDITIONAL_FLAGS="${ADDITIONAL_FLAGS} -m64"

# -mtune=generic: This flag tells the compiler to optimize code for a generic (i.e., 
# not architecture-specific) target.
ADDITIONAL_FLAGS="${ADDITIONAL_FLAGS} -mtune=generic"

# -fasynchronous-unwind-tables: This flag is related to exception handling and is used 
# for generating asynchronous unwind tables. It is used for unwinding the call stack 
# during exceptions.
ADDITIONAL_FLAGS="${ADDITIONAL_FLAGS} -fasynchronous-unwind-tables"

# -fstack-clash-protection: This flag adds protection against stack clash attacks,
# which attempt to abuse the stack space to execute malicious code.
ADDITIONAL_FLAGS="${ADDITIONAL_FLAGS} -fstack-clash-protection"

# -fcf-protection: This flag adds control flow protection to protect against various
# control flow attacks.
ADDITIONAL_FLAGS="${ADDITIONAL_FLAGS} -fcf-protection"

COMMON_OPTIONS=(
"-DCMAKE_C_FLAGS='${CMAKE_C_FLAGS} ${ADDITIONAL_FLAGS}'" 
"-DCMAKE_CXX_FLAGS='${CMAKE_CXX_FLAGS} ${ADDITIONAL_FLAGS}'" 
"-DWITH_NDB=1"
"-DBUILD_CONFIG=mysql_release"
"-DWITH_SSL=${OPENSSL_ROOT}"
"-DWITH_LDAP=system"
"-DWITH_KERBEROS=system"
"-DWITH_SASL=system"
"-DWITH_BOOST=${BOOST_ROOT}"
"-DWITH_NDB_TEST=1"
"-DWITH_UNIT_TESTS=1"
"-DWITH_RDRS=1"
"-DCMAKE_INSTALL_PREFIX=$INSTALL_DIR"
"-DCMAKE_BUILD_TYPE=Release"
)

if [[ "$TYPE" == "TRAIN" ]]; then

  echo "_____________ TRAIN BUILD _____________"

  cmake "$SRC_DIR" \
    -DFPROFILE_GENERATE=1 \
    "${COMMON_OPTIONS[@]}"

elif [[ "$TYPE" == "RELEASE" ]]; then

  echo "_____________ RELEASE BUILD _____________"

  cmake "$SRC_DIR" \
    -DFPROFILE_USE=1 \
    -DCPACK_MONOLITHIC_INSTALL=true \
    "${COMMON_OPTIONS[@]}"

elif [[ "$TYPE" == "SIMPLE" ]]; then

  echo "_____________ SIMPLE BUILD _____________"
  cmake "$SRC_DIR" \
  "${COMMON_OPTIONS[@]}"

else
  echo "Error: Wrong build type"
  exit 1
fi
