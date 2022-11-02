#!/bin/bash

set -e

export SYSBENCH_VERSION="sysbench-0.4.12.17"
export DBT2_VERSION="dbt2-0.37.50.18"
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

CORES=$(( $(nproc) / 2 + 1 ))

help(){
  echo "build-all.sh {-s path} [-b path] {-o path} {-j build_threads} {-r} "
  echo "USAGE" 
  echo "=====" 
  echo "      ./build-all.sh -s ../.. -o /tmp/output/ -b /tmp/build/ -r -j 20"
  echo ""
  echo "build-all.sh {-s path} [-b path] {-o path} {-r} "
  echo "-s=path" 
  echo "       path to rondb source code" 
  echo "-b=path"
  echo "       path to temp build direcotry" 
  echo "-o=path"
  echo "       path to output direcoty where the build process will copy the final tar ball" 
  echo "-j=build_threads"
  echo "       No of build threads. This is passed to make -j\$build_threads" 
  echo "-d deploy to remote repo"
  echo "-r make release tar balls. Takes longer"
}

RELEASE_BUILD=false
DEPLOY=false
# A POSIX variable
OPTIND=1         # Reset in case getopts has been used previously in the shell.
while getopts ":n:s:b:o:j:rd" opt; do
    case "$opt" in
    s)  SRC_DIR=$OPTARG
        ;;
    b)  TEMP_BUILD_DIR=$OPTARG
        ;;
    o)  OUTPUT_DIR=$OPTARG
        ;;
    j)  CORES=$OPTARG
        ;;
    r)  RELEASE_BUILD=true
        ;;
    d)  DEPLOY=true
        ;;
    *)
        help
        exit 0
        ;;
    esac
done
shift $((OPTIND-1))
[ "$1" = "--" ] && shift


if [[ "$SRC_DIR" == "" ]]; then
  echo "Source directory not specified"
  exit 1
else 
  SRC_DIR_ABS=$(readlink -f $SRC_DIR)
  if [[ ! -d $SRC_DIR_ABS ]]; then
    echo "Invalid source directory"
    exit 1
  fi

  if [[ ! -f $SRC_DIR_ABS/MYSQL_VERSION ]]; then
    echo "Invalid source directory. MYSQL_VERSION file not found"
    exit 1
  fi
fi

if [[ "$OUTPUT_DIR" == "" ]]; then
  echo "Output directory not specified"
  exit 1
else
  OUTPUT_DIR_ABS=$(readlink -f $OUTPUT_DIR)
  if [[ ! -d $OUTPUT_DIR_ABS ]]; then
    echo "Invalid output directory"
    exit 1
  fi
fi

if [[ "$TEMP_BUILD_DIR" == "" ]]; then
  echo "Temp build directory is not specified"
else
  TEMP_BUILD_DIR_ABS=$(readlink -f $TEMP_BUILD_DIR)
  if [[ ! -d $TEMP_BUILD_DIR_ABS ]]; then
    echo "Invalid temp build directory"
    exit 1
  fi
fi

echo "Build Params. Src: $SRC_DIR_ABS, Build dir: $TEMP_BUILD_DIR_ABS, \
Output dir: $OUTPUT_DIR_ABS, Release: $RELEASE_BUILD. Deploy: $DEPLOY"
source $SRC_DIR_ABS/MYSQL_VERSION
RONDB_VERSION="$MYSQL_VERSION_MAJOR.$MYSQL_VERSION_MINOR.$MYSQL_VERSION_PATCH"
if [ "$RELEASE_BUILD" = true ] ; then
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

echo "_____________ Building Tarball _____________"
cd $TEMP_BUILD_DIR_ABS
$SRC_DIR_ABS/build_scripts/release_scripts/create_rondb_tarball.sh $RONDB_VERSION $TEMP_BUILD_DIR_ABS $OUTPUT_DIR_ABS

if [ "$DEPLOY" = true ]; then
  echo "_____________ Deploying Tarball _____________"
  cp $SRC_DIR_ABS/build_scripts/release_scripts/id_rsa $TEMP_BUILD_DIR_ABS
  cd $TEMP_BUILD_DIR_ABS
  chmod 600 id_rsa
  $SRC_DIR_ABS/build_scripts/release_scripts/deploy.sh $OUTPUT_DIR_ABS/*
fi


