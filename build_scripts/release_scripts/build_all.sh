#!/bin/bash

set -e

export SYSBENCH_VERSION="sysbench-0.4.12.18"
export DBT2_VERSION="dbt2-0.37.50.19"
export DBT3_VERSION="dbt3-1.10"
export RDRS_VERSION="0.1.0"

if [[ -z "${OPENSSL_ROOT}" ]]; then
  echo "OPENSSL_ROOT environment variable is not defined. To use open ssl installed on the system use export OPENSSL_ROOT=system "
  exit 1
fi

if [[ -z "${BOOST_ROOT}" ]]; then
  echo "BOOST_ROOT environment variable is not defined. To use boost installed on the system use export BOOST_ROOT=system"
  exit 1
fi

help() {
  cat <<EOF
build-all.sh {-s path} {-b path} {-o path} {-j build_threads} {-r} {-d} {-f}

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
EOF
}

# Defaults
RELEASE_BUILD=false
RELEASE_FINAL_CLUSTERJ=false
DEPLOY=false

# A POSIX variable
OPTIND=1 # Reset in case getopts has been used previously in the shell.
while getopts ":n:s:b:o:j:rdf" opt; do
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
  d)
    DEPLOY=true
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
  Number of build threads:  $CORES
  RonDB version:            $RONDB_VERSION"

if [ "$RELEASE_BUILD" = true ]; then
  echo "_____________ BUILDING RONDB. RELEASE: TRUE _____________"
  cd $TEMP_BUILD_DIR_ABS
  mkdir -p feedback_build
  cd feedback_build
  $SRC_DIR_ABS/build_scripts/release_scripts/build_gen.sh "TRAIN" $SRC_DIR_ABS $TEMP_BUILD_DIR_ABS/feedback_build/install_dir
  make -j$CORES
  cd mysql-test
  $SRC_DIR_ABS/build_scripts/release_scripts/run_training.sh

  cd $TEMP_BUILD_DIR_ABS
  mkdir -p use_build
  cd use_build
  $SRC_DIR_ABS/build_scripts/release_scripts/build_gen.sh "RELEASE" $SRC_DIR_ABS $TEMP_BUILD_DIR_ABS/rondb_bin_use
  make -j$CORES
  make install
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

  CLUSTERJ_ARTIFACT_POSTFIX="-SNAPSHOT"
  if [ "$RELEASE_FINAL_CLUSTERJ" = true ]; then
    CLUSTERJ_ARTIFACT_POSTFIX=""
  fi

  $SRC_DIR_ABS/build_scripts/release_scripts/deploy.sh $RONDB_VERSION $TARBALL_NAME $OUTPUT_DIR_ABS $SRC_DIR_ABS/id_rsa "$CLUSTERJ_ARTIFACT_POSTFIX"
fi
