#!/bin/bash

set -e

export SYSBENCH_VERSION="sysbench-0.4.12.20"
export DBT2_VERSION="dbt2-0.37.50.22"
export DBT3_VERSION="dbt3-1.10"
export RDRS_VERSION="0.1.0-22.10" 

if [[ -z "${OPENSSL_ROOT}" ]]; then
  echo "OPENSSL_ROOT environment variable is not defined. To use open ssl installed on the system use export OPENSSL_ROOT=system "
  exit 1
fi

help() {
  cat <<EOF
build-all.sh {-s path} {-b path} {-o path} {-j build_threads} {-r} {-d} {-f} {-p} {-i}

USAGE
=====
Example: ./build-all.sh -s ../.. -o /tmp/output/ -b /tmp/build/ -r -j 20

build-all.sh {-s path} [-b path] {-o path} {-r} 
-s=path
        Path to RonDB source code
-b=path
        Path to temp build directory
-o=path
        Path to output directory where the build process will copy the final tarball
-j=build_threads
        No of build threads. This is passed to $(make -j$build_threads)
-d      Deploy to remote repo.hops.works
-r      Make release tarballs. Takes longer
-f      Make final realse for clusterj artifacts with out -SNAPSHOT in the artifact name.
        By default clusterj artifacts are uploaded as a SNAPSHOT version
-p      The release is public and it will be copied to /master on the repo server. 
        Otherwise the tarball will be copied to /dev on the repo server.
-i      Ignore MTR test failures. A release build (-r) runs the MTR suites on
        the PGO training build and on the final build, and by default fails
        before creating or deploying the tarball if any test fails. Use this
        only to knowingly release over a known failure.
EOF
}

# Defaults
RELEASE_BUILD=false
RELEASE_FINAL_CLUSTERJ=false
DEPLOY=false
IGNORE_TEST_FAILURES=false

# A POSIX variable
OPTIND=1 # Reset in case getopts has been used previously in the shell.
while getopts ":n:s:b:o:j:rdfpi" opt; do
  case "$opt" in
  s)
    SRC_DIR=$OPTARG
    ;;
  b)
    TEMP_BUILD_DIR=$OPTARG
    ;;
  o)
    OUTPUT_DIR=$OPTARG
    ;;
  j)
    CORES=$OPTARG
    ;;
  r)
    RELEASE_BUILD=true
    ;;
  f)
    RELEASE_FINAL_CLUSTERJ=true
    ;;
  p)
    IS_PUBLIC_RELEASE=true
    ;;
  d)
    DEPLOY=true
    ;;
  i)
    IGNORE_TEST_FAILURES=true
    ;;
  *)
    help
    exit 0
    ;;
  esac
done
shift $((OPTIND - 1))
[ "$1" = "--" ] && shift

if [[ "$SRC_DIR" == "" ]]; then
  echo "Source directory not specified"
  exit 1
fi

SRC_DIR_ABS=$(readlink -f $SRC_DIR)
if [[ ! -d $SRC_DIR_ABS ]]; then
  echo "Invalid source directory"
  exit 1
fi

if [[ ! -f $SRC_DIR_ABS/MYSQL_VERSION ]]; then
  echo "Invalid source directory. MYSQL_VERSION file not found"
  exit 1
fi

source $SRC_DIR_ABS/MYSQL_VERSION
RONDB_VERSION="$MYSQL_VERSION_MAJOR.$MYSQL_VERSION_MINOR.$MYSQL_VERSION_PATCH"
RONDB_VERSION_EXTRA="$MYSQL_VERSION_EXTRA"

if [[ "$OUTPUT_DIR" == "" ]]; then
  echo "Output directory not specified"
  exit 1
fi

OUTPUT_DIR_ABS=$(readlink -f $OUTPUT_DIR)
if [[ ! -d $OUTPUT_DIR_ABS ]]; then
  echo "Invalid output directory"
  exit 1
fi

if [[ "$TEMP_BUILD_DIR" == "" ]]; then
  echo "Temp build directory is not specified"
  exit 1
fi

TEMP_BUILD_DIR_ABS=$(readlink -f $TEMP_BUILD_DIR)
if [[ ! -d $TEMP_BUILD_DIR_ABS ]]; then
  echo "Invalid temp build directory"
  exit 1
fi

if [[ "$CORES" == "" ]]; then
  echo "The number of build threads is not specified is not specified"
  exit 1
fi

echo "Build Params:
  Src dir:                  $SRC_DIR_ABS
  Build dir:                $TEMP_BUILD_DIR_ABS
  Output dir:               $OUTPUT_DIR_ABS
  Release:                  $RELEASE_BUILD
  Deploy:                   $DEPLOY
  Release final clusterj    $RELEASE_FINAL_CLUSTERJ
  Is public release         $IS_PUBLIC_RELEASE
  Ignore test failures      $IGNORE_TEST_FAILURES
  Number of build threads:  $CORES
  RonDB version:            $RONDB_VERSION
  RonDB version Extra:      $RONDB_VERSION_EXTRA" 
   
# Called when run_training.sh reports failing tests on build $1. Fails the
# build, before any tarball is created or deployed, unless -i was given.
test_failures() {
  if [ "$IGNORE_TEST_FAILURES" = true ]; then
    echo "################################################################"
    echo "## WARNING: MTR tests FAILED on the $1 build."
    echo "## Continuing because of -i (ignore test failures)."
    echo "################################################################"
  else
    echo "ERROR: MTR tests failed on the $1 build; see the mtr output above."
    echo "Not creating or deploying a tarball. Fix the tests, or rerun with"
    echo "-i to knowingly release over the failures."
    exit 1
  fi
}

if [ "$RELEASE_BUILD" = true ]; then
  echo "_____________ BUILDING RONDB. RELEASE: TRUE _____________"
  cd $TEMP_BUILD_DIR_ABS
  mkdir -p feedback_build
  cd feedback_build
  $SRC_DIR_ABS/build_scripts/release_scripts/build_gen.sh "TRAIN" $SRC_DIR_ABS $TEMP_BUILD_DIR_ABS/feedback_build/install_dir
  make -j$CORES
  cd mysql-test
  echo "_____________ TESTING PGO TRAINING BUILD _____________"
  $SRC_DIR_ABS/build_scripts/release_scripts/run_training.sh ||
    test_failures "PGO training"

  cd $TEMP_BUILD_DIR_ABS
  mkdir -p use_build
  cd use_build
  $SRC_DIR_ABS/build_scripts/release_scripts/build_gen.sh "RELEASE" $SRC_DIR_ABS $TEMP_BUILD_DIR_ABS/rondb_bin_use
  make -j$CORES
  make install

  echo "_____________ TESTING RELEASE BUILD _____________"
  cd $TEMP_BUILD_DIR_ABS/use_build/mysql-test
  MTR_PARALLEL=$((CORES / 8))
  if [ $MTR_PARALLEL -lt 1 ]; then
    MTR_PARALLEL=1
  fi
  MTR_EXTRA_ARGS="--parallel=$MTR_PARALLEL" \
    $SRC_DIR_ABS/build_scripts/release_scripts/run_training.sh ||
    test_failures "release"
else
  echo "_____________ BUILDING RONDB. RELEASE: FALSE _____________"
  cd $TEMP_BUILD_DIR_ABS
  $SRC_DIR_ABS/build_scripts/release_scripts/build_gen.sh "SIMPLE" $SRC_DIR_ABS $TEMP_BUILD_DIR_ABS/rondb_bin_use
  make -j$CORES
  make install
fi

echo "_____________ BUILDING BENCHMARKS _____________"
cd $TEMP_BUILD_DIR_ABS
$SRC_DIR_ABS/build_scripts/release_scripts/build_bench.sh $TEMP_BUILD_DIR_ABS $TEMP_BUILD_DIR_ABS/rondb_bin_use $CORES

source $SRC_DIR_ABS/build_scripts/release_scripts/get_tarball_name.sh
set +e
TAR_OUTPUT=$(get_tarball_name $RONDB_VERSION)
if [ $? -ne 0 ]; then
  echo $TAR_OUTPUT
  exit 1
fi
set -e
TARBALL_NAME=$(echo "$TAR_OUTPUT" | tail -1)

echo "_____________ BUILDING TARBALL _____________"
cd $TEMP_BUILD_DIR_ABS
$SRC_DIR_ABS/build_scripts/release_scripts/create_rondb_tarball.sh $TARBALL_NAME $TEMP_BUILD_DIR_ABS $OUTPUT_DIR_ABS

if [ "$DEPLOY" = true ]; then
  echo "_____________ DEPLOYING TARBALL _____________"
  cd $TEMP_BUILD_DIR_ABS

  TARBALL_COPY_LOCATION="/opt/repository/dev/rondb"
  if [ "$IS_PUBLIC_RELEASE" = true ]; then
    TARBALL_COPY_LOCATION="/opt/repository/master"

    if [ "$RELEASE_FINAL_CLUSTERJ" = true ]; then
      CLUSTERJ_VERSION="$RONDB_VERSION"
    else
      CLUSTERJ_VERSION="$RONDB_VERSION-SNAPSHOT"
    fi

  else
    echo "Not a public release. Skip deploying clusterj."
    CLUSTERJ_VERSION="DO_NOT_DEPLOY"
  fi
  

  $SRC_DIR_ABS/build_scripts/release_scripts/deploy.sh $RONDB_VERSION $TARBALL_NAME $OUTPUT_DIR_ABS $SRC_DIR_ABS/id_rsa "$TARBALL_COPY_LOCATION" "$CLUSTERJ_VERSION" "$RONDB_VERSION_EXTRA"
fi
