#!/bin/bash

set -e

help() {
  cat <<EOF
docker-build.sh {-n name} {-s path} [-b path] {-o path} {-j build_threads} {-r}
USAGE
=====
Example: ./docker-build.sh -s ../.. -o /tmp/output/ -r -j 20

-n=name
      Docker image name. Default is rondb_build
-s=path
      Path to RonDB source code
-b=path
      Optional path to temp build directory. If this is omitted then the build files reside in the container
-o=path
      Path to output directory where the build process will copy the final tar ball
-j=build_thread
      Number of build threads; this cannot be more than CPUs permitted to use by Docker; see $(docker info) for more information

-d    Deploy to remote repo. Hopsworks AB specific
-r    Make release tar balls. This takes longer
EOF
}

command -v "docker"
if [[ "${?}" -ne 0 ]]; then
  echo "Make sure that you have Docker installed"
  exit 1
fi

RELEASE_BUILD=false
DEPLOY=false
# A POSIX variable
OPTIND=1 # Reset in case getopts has been used previously in the shell.
while getopts ":n:s:b:o:j:rd" opt; do
  case "$opt" in
  n)
    PREFIX=$OPTARG
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
if [ -z $CORES ]; then
  CORES=$MAX_DOCKER_CORES
elif [ $CORES -gt $MAX_DOCKER_CORES ]; then
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

DOCKER_IMAGE="rondb_build:${RONDB_VERSION}"
PREFIX=$1
if [ "$PREFIX" != "" ]; then
  DOCKER_IMAGE="${PREFIX}:${RONDB_VERSION}"
fi

echo "Build Params:
  Src dir: $SRC_DIR_ABS
  Build dir: $TEMP_BUILD_DIR_ABS
  Output dir: $OUTPUT_DIR_ABS
  Docker image name: $DOCKER_IMAGE
  No of build threads: $CORES
  Release: $RELEASE_BUILD
  Deploy: $DEPLOY"

echo "Creating Docker image ${DOCKER_IMAGE}"

docker build -t $DOCKER_IMAGE .

echo "Building RonDB using $DOCKER_IMAGE"
mount="-v $SRC_DIR_ABS:/src -v $OUTPUT_DIR_ABS:/output "
if [[ -d $TEMP_BUILD_DIR_ABS ]]; then
  mount="$mount -v $TEMP_BUILD_DIR_ABS:/build "
fi

build_args="-j $CORES -s /src -b /build -o /output "

if [ "$RELEASE_BUILD" = true ]; then
  build_args="$build_args -r"
fi

if [ "$DEPLOY" = true ]; then
  build_args="$build_args -d"
fi

docker run --rm $mount -w /src --user mysql "$DOCKER_IMAGE" /bin/bash -l ./build_scripts/release_scripts/build_all.sh $build_args
#docker run --rm $mount -w /src --user mysql -it "$DOCKER_IMAGE" /bin/bash -l
