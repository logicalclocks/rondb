#!/bin/bash

set -e

help(){
  echo "docker-build.sh {-n name} {-s path} [-b path] {-o path} {-j build_threads} {-r} "
  echo "USAGE" 
  echo "=====" 
  echo "      ./docker-build.sh -s ../.. -o /tmp/output/ -r -j 20"
  echo ""
  echo "-n=name" 
  echo "       docker image name. Default iname name is rondb_build" 
  echo "-s=path" 
  echo "       path to rondb source code" 
  echo "-b=path"
  echo "       path to temp build direcotry. This is optional. if this is omitted then the build files reside in the container" 
  echo "-o=path"
  echo "       path to output direcoty where the build process will copy the final tar ball" 
  echo "-j=build_thread"
  echo "       number of build threads" 
  echo "-d deploy to remote repo. Hopsworks AB specific"
  echo "-r make release tar balls. Takes longer"
}

command -v "docker"
if [[ "${?}" -ne 0 ]]; then
  echo "Make sure that you have docker installed to be able to build ePipe."
  exit 1
fi

CORES=$(( $(nproc) / 2 + 1 ))
USERID=`id -u`
GROUPID=`id -g`
RELEASE_BUILD=false
DEPLOY=false
# A POSIX variable
OPTIND=1         # Reset in case getopts has been used previously in the shell.
while getopts ":n:s:b:o:j:rd" opt; do
    case "$opt" in
    n)  PREFIX=$OPTARG
        ;;
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

source ../../MYSQL_VERSION
RONDB_VERSION="$MYSQL_VERSION_MAJOR.$MYSQL_VERSION_MINOR.$MYSQL_VERSION_PATCH"

DOCKER_IMAGE="rondb_build:${RONDB_VERSION}"
PREFIX=$1
if [ "$PREFIX" != "" ]; then
  DOCKER_IMAGE="${PREFIX}:${RONDB_VERSION}"
fi

echo "Build Params. Src: $SRC_DIR_ABS, Build dir: $TEMP_BUILD_DIR_ABS, Output dir: $OUTPUT_DIR_ABS, \ 
  Release: $RELEASE_BUILD. Docker image name: $DOCKER_IMAGE. No of build threads: $CORES. Deploy: $DEPLOY"

echo "Creating docker image ${DOCKER_IMAGE}"

docker build --build-arg userid=${USERID} --build-arg groupid=${GROUPID}  -t $DOCKER_IMAGE . 

echo "Building RonDB using $DOCKER_IMAGE"
mount="-v $SRC_DIR_ABS:/src -v $OUTPUT_DIR_ABS:/output "
if [[ -d $TEMP_BUILD_DIR_ABS ]]; then
   mount="$mount -v $TEMP_BUILD_DIR_ABS:/build " 
fi

build_args="-j $CORES -s /src -b /build -o /output "

if [ "$RELEASE_BUILD" = true ] ; then
build_args="$build_args -r"
fi

if [ "$DEPLOY" = true ] ; then
build_args="$build_args -d"
fi

docker run --rm $mount -w /src --user mysql "$DOCKER_IMAGE" /bin/bash -l ./build_scripts/release_scripts/build_all.sh $build_args
#docker run --rm $mount -w /src --user mysql -it "$DOCKER_IMAGE" /bin/bash -l 
