#!/bin/bash

set -e

RONDB_VERSION=$1
TARBALL_NAME=$2
OUTPUT_DIR_ABS=$3
ABS_PATH_RSA_KEY=$4
TARBALL_COPY_LOCATION=$5
CLUSTERJ_VERSION=$6
RONDB_VERSION_EXTRA=$7

TAR_FILE="$TARBALL_NAME.tar.gz"

CE_USER=___CE_USER___
CE_PASS=___CE_PASS___
EE_USER=___EE_USER___
EE_PASS=___EE_PASS___

TAR_FILE_ABS=$(readlink -f $OUTPUT_DIR_ABS/$TAR_FILE)
if [[ ! -f "$TAR_FILE_ABS" ]]; then
  echo "Error: Unable to find tar ball $TAR_FILE_ABS"
  exit 1
fi

DST="repo@repo.hops.works:$TARBALL_COPY_LOCATION/$TAR_FILE"
echo "Copying: $TAR_FILE_ABS to $DST"
scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i $ABS_PATH_RSA_KEY $TAR_FILE_ABS $DST

# The x86_64 and aarch64 builds run as separate build jobs, concurrently and in no
# particular order. Each one deploys the libndbclient jar for its own platform. The
# platform-independent ClusterJ jar is deployed once, from the x86_64 job only.
CPU_ARCH=$(uname -m)
case "$CPU_ARCH" in
  x86_64)        NATIVE_CLASSIFIER=linux-x86_64 ;;
  aarch64|arm64) NATIVE_CLASSIFIER=linux-aarch64 ;;
  *)
    echo "Error: unsupported CPU architecture '$CPU_ARCH'"
    exit 1
    ;;
esac

if [ "$CLUSTERJ_VERSION" = "DO_NOT_DEPLOY" ]; then
  echo "Skip deploying clusterj"
  exit 0
fi

if [ "$CPU_ARCH" = "x86_64" ]; then
  set +e

  # First attempt. clusterj-$RONDB_VERSION.jar. e.g. 22.10.6.jar
  echo "Extracting ClusterJ JAR file from tarball"
  JAR_FILE="$TARBALL_NAME/share/java/clusterj-$RONDB_VERSION.jar"
  tar xf $TAR_FILE_ABS $JAR_FILE
  if [[ ! -f "$JAR_FILE" ]]; then
    echo "Error: Unable to find cluster file '$JAR_FILE'. Retrying ..."

    # Second attempt. e.g. 22.10.6LTS.jar
    JAR_FILE="$TARBALL_NAME/share/java/clusterj-$RONDB_VERSION$RONDB_VERSION_EXTRA.jar"
    tar xf $TAR_FILE_ABS $JAR_FILE
    if [[ ! -f "$JAR_FILE" ]]; then
      echo "Error: Unable to find cluster file '$JAR_FILE'"
      exit 1
    fi
  fi

  set -e

  mvn deploy:deploy-file -Dfile=$JAR_FILE -DgroupId=com.mysql.ndb -DartifactId=clusterj-rondb \
    -Dversion=$CLUSTERJ_VERSION -Dpackaging=jar -DrepositoryId=Hops \
    -Durl=https://archiva.hops.works/repository/Hops \
    -DJenkinsHops.RepoID=Hops \
    -DJenkinsHops.User=$CE_USER \
    -DJenkinsHops.Password=$CE_PASS

  mvn deploy:deploy-file -Dfile=$JAR_FILE -DgroupId=com.mysql.ndb -DartifactId=clusterj-rondb \
    -Dversion=$CLUSTERJ_VERSION -Dpackaging=jar -DrepositoryId=HopsEE \
    -Durl=https://nexus.hops.works/repository/hops-artifacts \
    -DJenkinsHops.RepoID=HopsEE \
    -DJenkinsHops.User=$EE_USER \
    -DJenkinsHops.Password=$EE_PASS
else
  echo "Not on x86_64; the platform independent ClusterJ jar is deployed by the x86_64 job"
fi

echo "Extracting libndbclient from tarball"
# The tarball has lib/libndbclient.so as a symlink to the versioned real file, e.g.
# libndbclient.so.6.1.0. Extract both and resolve the symlink, so this does not depend on
# the soversion and the file we hand over is the real library, not a link.
tar xf $TAR_FILE_ABS --wildcards "$TARBALL_NAME/lib/libndbclient.so*"
LIBNDB_FILE=$(readlink -f "$TARBALL_NAME/lib/libndbclient.so")
if [[ ! -f "$LIBNDB_FILE" || -L "$LIBNDB_FILE" ]]; then
  echo "Error: Unable to find the libndbclient shared library in '$TARBALL_NAME/lib'"
  exit 1
fi
echo "Using $LIBNDB_FILE"

# Deploys com.mysql.ndb:libndbclient-multiarch:$RONDB_VERSION with classifier
# $NATIVE_CLASSIFIER to Nexus. The script uploads the shared pom only if no other
# platform's job has done so yet, so the two jobs may finish in either order.
git clone --branch multiarch https://github.com/hopshadoop/clusterj-native
./clusterj-native/deploy-native.sh "$RONDB_VERSION" "$NATIVE_CLASSIFIER" "$LIBNDB_FILE" \
  -DJenkinsHops.RepoID=HopsEE \
  -DJenkinsHops.User=$EE_USER \
  -DJenkinsHops.Password=$EE_PASS

rm -rf "$TARBALL_NAME"
