#!/bin/bash

set -e

help() {
  cat <<EOF
docker-create-builder.sh {-s path} [-f dockerfile] [-n name] [-j build_threads] [-j build_volume_name]

This script first builds an image with all dependencies installed that RonDB
requires to be built. It then runs this image and opens a shell in the container.
It also mounts the source code into the container, such that the source code can
be edited on the host machine and then be compiled inside the machine. To persist
build files, the container directory /root/rondb-bin is mounted into a volume.
The name of this volume can be altered via the CLI.

USAGE
=====
Example: ./docker-create-builder.sh -s ../.. -f ../../Dockerfile.ubuntu22 -j 20

-s    <string>
      Path to RonDB source code
-f    <string> (optional)
      Name (not path) of the Dockerfile
      Default: Dockerfile.oraclelinux7 (amd64 builds)
-n    <string> (optional)
      Docker image name
      Default: "rondb-build-dependencies"
      The tag will be the Rondb version, which is extracted from the MYSQL_VERSION file
-j    <int> (optional)
      Number of threads to build the Dockerfile
      Default: 1
      See "docker info" for how many CPUs Docker has access to
-v    <string> (optional)
      Name of Docker build volume
      Default: "rondb-bin"
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
BUILD_VOLUME_NAME="rondb-bin"

POSITIONAL=()
while [[ $# -gt 0 ]]; do
  key="$1"

  case $key in
  -h | --help)
    help
    exit 0
    ;;
  -s)
    SRC_DIR="$2"
    shift # past argument
    shift # past value
    ;;
  -f)
    DOCKERFILE="$2"
    shift # past argument
    shift # past value
    ;;
  -n)
    DOCKER_IMAGE_NAME="$2"
    shift # past argument
    shift # past value
    ;;
  -j)
    CORES="$2"
    shift # past argument
    shift # past value
    ;;
  -v)
    BUILD_VOLUME_NAME="$2"
    shift # past argument
    shift # past value
    ;;

  *)                   # unknown option
    POSITIONAL+=("$1") # save it in an array for later
    shift              # past argument
    ;;
  esac
done

set -- "${POSITIONAL[@]}" # restore unknown options
if [[ -n $1 ]]; then
  echo "##################" >&2
  echo "Illegal arguments: $*" >&2
  echo "##################" >&2
  echo
  help
  exit 1
fi

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
  echo 'Other OS' >&2
  help
  exit 1
  ;;
esac

if [ -z $SRC_DIR ]; then
  echo "Source directory not specified" >&2
  help
  exit 1
fi

SRC_DIR_ABS=$($readlinkcmd -f $SRC_DIR)
if [[ ! -d $SRC_DIR_ABS ]]; then
  echo "Invalid source directory" >&2
  help
  exit 1
fi

# Basic source code check
if [[ ! -f $SRC_DIR_ABS/MYSQL_VERSION ]]; then
  echo "Invalid source directory. MYSQL_VERSION file not found" >&2
  help
  exit 1
fi

source $SRC_DIR_ABS/MYSQL_VERSION
RONDB_VERSION="$MYSQL_VERSION_MAJOR.$MYSQL_VERSION_MINOR.$MYSQL_VERSION_PATCH"

if [ -z $DOCKER_IMAGE_NAME ]; then
  DOCKER_IMAGE_NAME="rondb-build-dependencies:${RONDB_VERSION}"
else
  DOCKER_IMAGE_NAME="$DOCKER_IMAGE_NAME:${RONDB_VERSION}"
fi

cat <<EOF
Build Params:
  Src dir: $SRC_DIR_ABS
  Dockerfile: $DOCKERFILE
  Docker image: $DOCKER_IMAGE_NAME
  No of build threads: $CORES
EOF

echo "Creating Docker image ${DOCKER_IMAGE_NAME}"

docker buildx build . \
  -f $SRC_DIR_ABS/$DOCKERFILE \
  --platform linux/arm64 \
  --tag $DOCKER_IMAGE_NAME \
  --target rondb-build-dependencies \
  --build-arg BUILD_THREADS=$CORES

echo "Building RonDB using $DOCKER_IMAGE_NAME"

# Using ´docker run` lets us mount the build directory into a local directory
docker run --rm -it \
  --name rondb-builder \
  --entrypoint=/bin/bash \
  --mount type=bind,src=$SRC_DIR_ABS,dst=/root/rondb-src \
  --mount type=volume,src=$BUILD_VOLUME_NAME,dst=/root/rondb-bin \
  $DOCKER_IMAGE_NAME
