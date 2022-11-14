#!/bin/bash

set -e

help() {
  cat <<EOF
docker-build.sh {-s path} {-o path} [-t tag] [-b path] [-j build_threads] [-r] [-d]

This script builds RonDB using Docker and returns tarballs containing the binaries.

USAGE
=====
Example: ./docker-build.sh -s ../.. -o /tmp/output/ -r -j 20

-s=path
      Path to RonDB source code
-o=path
      Path to output directory where the build process will copy the final tarball
-t=tag
      Optional Docker image tag
      Default is rondb_build:<rondb-version>
      The RonDB version is extracted from the MYSQL_VERSION file
-b=path
      Optional path to directory of resulting RonDB binaries
      If this is omitted then the binaries reside in the container
-j=build_thread
      Optional number of build threads
      Defaults to 1
      See $(docker info) for more information

-r    Create release builds. This takes longer
-d    Deploy to remote repo.hops.works. Hopsworks AB specific
EOF
}

command -v "docker"
if [[ "${?}" -ne 0 ]]; then
  echo "Make sure that you have Docker installed"
  exit 1
fi

# Defaults
RELEASE_BUILD=false
DEPLOY=false
CORES=1

# A POSIX variable
OPTIND=1 # Reset in case getopts has been used previously in the shell.
while getopts ":t:s:b:o:j:rd" opt; do
  case "$opt" in
  t)
    DOCKER_IMAGE_TAG=$OPTARG
    ;;
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

MAX_DOCKER_CORES=$(docker info | sed -n "s/.*CPUs\: *\(\S*\)/\1/p") # cores are limited to the Docker configs
echo "The Docker config allows the usage of $MAX_DOCKER_CORES CPUs"
if [ $CORES -gt $MAX_DOCKER_CORES ]; then
  echo "The amount of cores specified in the Docker configs is $MAX_DOCKER_CORES; cannot build with more threads"
  exit 1
fi

if [ -z $SRC_DIR ]; then
  echo "Source directory not specified"
  exit 1
fi

# TODO: Differentiate between mac/linux (greadlink/readlink)
SRC_DIR_ABS=$(greadlink -f $SRC_DIR)
if [[ ! -d $SRC_DIR_ABS ]]; then
  echo "Invalid source directory"
  exit 1
fi

# Basic source code check
if [[ ! -f $SRC_DIR_ABS/MYSQL_VERSION ]]; then
  echo "Invalid source directory. MYSQL_VERSION file not found"
  exit 1
fi

if [ -z $OUTPUT_DIR ]; then
  echo "Output directory not specified"
  exit 1
fi

OUTPUT_DIR_ABS=$(greadlink -f $OUTPUT_DIR)
if [[ ! -d $OUTPUT_DIR_ABS ]]; then
  echo "Invalid output directory"
  exit 1
fi

if [ -z $TEMP_BUILD_DIR ]; then
  echo "Temp build directory is not specified"
else
  TEMP_BUILD_DIR_ABS=$(greadlink -f $TEMP_BUILD_DIR)
  if [[ ! -d $TEMP_BUILD_DIR_ABS ]]; then
    echo "Invalid temp build directory"
    exit 1
  fi
fi

source $SRC_DIR_ABS/MYSQL_VERSION
RONDB_VERSION="$MYSQL_VERSION_MAJOR.$MYSQL_VERSION_MINOR.$MYSQL_VERSION_PATCH"

if [ -z $DOCKER_IMAGE_TAG ]; then
  DOCKER_IMAGE_TAG="rondb_build:${RONDB_VERSION}"
fi

echo "Build Params:
  Src dir: $SRC_DIR_ABS
  Build dir: $TEMP_BUILD_DIR_ABS
  Output dir: $OUTPUT_DIR_ABS
  Docker image tag: $DOCKER_IMAGE_TAG
  No of build threads: $CORES
  Release: $RELEASE_BUILD
  Deploy: $DEPLOY"

echo "Creating Docker image ${DOCKER_IMAGE_TAG}"

docker build -t $DOCKER_IMAGE_TAG .

echo "Building RonDB using $DOCKER_IMAGE_TAG"
mount="-v $SRC_DIR_ABS:/src -v $OUTPUT_DIR_ABS:/output "
if [ -z $TEMP_BUILD_DIR_ABS ]; then
  mount="$mount -v $TEMP_BUILD_DIR_ABS:/build "
fi

build_args="-j $CORES -s /src -b /build -o /output "

if [ "$RELEASE_BUILD" = true ]; then
  build_args="$build_args -r"
fi

if [ "$DEPLOY" = true ]; then
  build_args="$build_args -d"
fi

docker run --rm $mount -w /src --user mysql "$DOCKER_IMAGE_TAG" /bin/bash -l ./build_scripts/release_scripts/build_all.sh $build_args
#docker run --rm $mount -w /src --user mysql -it "$DOCKER_IMAGE_TAG" /bin/bash -l
