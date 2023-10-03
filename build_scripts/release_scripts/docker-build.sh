#!/bin/bash

set -e

help() {
  cat <<EOF
docker-build.sh {-s path} {-o path} [-n name] [-b path] [-j build_threads] [-r] [-d]

This script builds RonDB using Docker and returns tarballs containing the binaries.
Use this file if access to the RonDB build directory is required. When only using
the Dockerfile for building RonDB, the build directory will be stored inside a Docker
cache.

USAGE
=====
Example: ./docker-build.sh -s ../.. -o /tmp/output/ -b /tmp/build/ -r -j 20

-s=path
      Path to RonDB source code
-o=path
      Path to output directory where the build process will copy the final tarball
-b=path
      Path to directory of resulting RonDB binaries
      If this is omitted then the binaries reside in the container
-f=Dockerfile
      Name of the Dockerfile; Default: Dockerfile.oraclelinux7 (amd64 builds)
-n=name
      Optional Docker image name
      Default is "rondb-build-dependencies"
      The tag will be <rondb-version>
      The RonDB version is extracted from the MYSQL_VERSION file
-j=build_thread
      Optional number of build threads
      Defaults to 1
      See "docker info" for how many CPUs Docker has access to

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
DOCKERFILE="Dockerfile.oraclelinux7"
CORES=1
RELEASE_BUILD=
DEPLOY=

# A POSIX variable
OPTIND=1 # Reset in case getopts has been used previously in the shell.
while getopts ":n:s:b:o:f:j:rd" opt; do
  case "$opt" in
  t)
    DOCKER_IMAGE_NAME=$OPTARG
    ;;
  s)
    SRC_DIR=$OPTARG
    ;;
  b)
    BUILD_DIR=$OPTARG
    ;;
  o)
    OUTPUT_DIR=$OPTARG
    ;;
  f)
    DOCKERFILE=$OPTARG
    ;;
  j)
    CORES=$OPTARG
    ;;
  r)
    RELEASE_BUILD="-r"
    ;;
  d)
    DEPLOY="-d"
    ;;
  *)
    help
    exit 0
    ;;
  esac
done
shift $((OPTIND - 1))
[ "$1" = "--" ] && shift

# Differentiate between Mac/Linux (greadlink/readlink)
readlinkcmd=
case "$(uname -sr)" in
Darwin*)
  readlinkcmd="greadlink"
  ;;

Linux*Microsoft*)
  readlinkcmd="readlink"
  ;;

Linux*)
  readlinkcmd="readlink"
  ;;

*)
  echo 'Other OS'
  exit 1
  ;;
esac

if [ -z $SRC_DIR ]; then
  echo "Source directory not specified"
  exit 1
fi

SRC_DIR_ABS=$($readlinkcmd -f $SRC_DIR)
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

OUTPUT_DIR_ABS=$($readlinkcmd -f $OUTPUT_DIR)
if [[ ! -d $OUTPUT_DIR_ABS ]]; then
  echo "Invalid output directory"
  exit 1
fi

if [ -z $BUILD_DIR ]; then
  echo "Build directory is not specified"
  exit 1
fi

BUILD_DIR_ABS=$($readlinkcmd -f $BUILD_DIR)
if [[ ! -d $BUILD_DIR_ABS ]]; then
  echo "Invalid build directory"
  exit 1
fi

source $SRC_DIR_ABS/MYSQL_VERSION
RONDB_VERSION="$MYSQL_VERSION_MAJOR.$MYSQL_VERSION_MINOR.$MYSQL_VERSION_PATCH"

if [ -z $DOCKER_IMAGE_NAME ]; then
  DOCKER_IMAGE_NAME="rondb-build-dependencies:${RONDB_VERSION}"
else
  DOCKER_IMAGE_NAME="$DOCKER_IMAGE_NAME:${RONDB_VERSION}"
fi

echo "Build Params:
  Src dir: $SRC_DIR_ABS
  Build dir: $BUILD_DIR_ABS
  Output dir: $OUTPUT_DIR_ABS
  Dockerfile: $DOCKERFILE
  Docker image: $DOCKER_IMAGE_NAME
  No of build threads: $CORES
  Release: $RELEASE_BUILD
  Deploy: $DEPLOY"

echo "Creating Docker image ${DOCKER_IMAGE_NAME}"

docker buildx build . \
  -f $SRC_DIR_ABS/$DOCKERFILE \
  --tag $DOCKER_IMAGE_NAME \
  --target rondb-build-dependencies \
  --build-arg BUILD_THREADS=$CORES

echo "Building RonDB using $DOCKER_IMAGE_NAME"

# Using ´docker run` lets us mount the build directory into a local directory
docker run --rm \
  --mount type=bind,src=$SRC_DIR_ABS,dst=/rondb-src \
  --mount type=bind,src=$OUTPUT_DIR_ABS,dst=/rondb-tarball \
  --mount type=bind,src=$BUILD_DIR_ABS,dst=/rondb-bin \
  $DOCKER_IMAGE_NAME \
  /bin/bash -c "source /root/.bashrc && /rondb-src/build_scripts/release_scripts/build_all.sh -s /rondb-src -b /rondb-bin -o /rondb-tarball -j $CORES $RELEASE_BUILD $DEPLOY"
