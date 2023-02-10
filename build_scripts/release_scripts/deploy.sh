#!/bin/bash

set -e

RONDB_VERSION=$1
TARBALL_NAME=$2
OUTPUT_DIR_ABS=$3
ABS_PATH_RSA_KEY=$4
CLUSTERJ_ARTIFACT_POSTFIX=$5

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

DST="repo@repo.hops.works:/opt/repository/master/$TAR_FILE"
echo "Copying: $TAR_FILE_ABS to $DST"
scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i $ABS_PATH_RSA_KEY $TAR_FILE_ABS $DST

# will return x86_64 on Ubuntu, Oraclelinux7 & Mac
CPU_ARCH=$(uname -m)
if [ "$CPU_ARCH" != "x86_64" ]; then
  echo "We're not on x86_64 here; Only Java files left to deploy and they are platform independent; Skipping them"
  exit 0
fi

echo "Extracting ClusterJ JAR file from tarball again"
JAR_FILE="$TARBALL_NAME/share/java/clusterj-$RONDB_VERSION.jar"
tar xf $TAR_FILE_ABS $JAR_FILE
if [[ ! -f "$JAR_FILE" ]]; then
  echo "Error: Unable to find cluster file '$JAR_FILE'"
  exit 1
fi

mvn deploy:deploy-file -Dfile=$JAR_FILE -DgroupId=com.mysql.ndb -DartifactId=clusterj-rondb \
  -Dversion=$RONDB_VERSION$CLUSTERJ_ARTIFACT_POSTFIX -Dpackaging=jar -DrepositoryId=Hops \
  -Durl=https://archiva.hops.works/repository/Hops \
  -DJenkinsHops.RepoID=Hops \
  -DJenkinsHops.User=$CE_USER \
  -DJenkinsHops.Password=$CE_PASS

mvn deploy:deploy-file -Dfile=$JAR_FILE -DgroupId=com.mysql.ndb -DartifactId=clusterj-rondb \
  -Dversion=$RONDB_VERSION$CLUSTERJ_ARTIFACT_POSTFIX -Dpackaging=jar -DrepositoryId=HopsEE \
  -Durl=https://nexus.hops.works/repository/hops-artifacts \
  -DJenkinsHops.RepoID=HopsEE \
  -DJenkinsHops.User=$EE_USER \
  -DJenkinsHops.Password=$EE_PASS

echo "Extracting libndbclient.so.6.1.0 file from tarball again"
LIBNDB_FILE="$TARBALL_NAME/lib/libndbclient.so.6.1.0"
tar xf $TAR_FILE_ABS $LIBNDB_FILE
if [[ ! -f "$LIBNDB_FILE" ]]; then
  echo "Error: Unable to find libndbclient.so.6.1.0 file '$LIBNDB_FILE'"
  exit 1
fi

git clone --branch jenkins https://github.com/hopshadoop/clusterj-native
cp $LIBNDB_FILE clusterj-native/src/main/resources/libndbclient.so

cd clusterj-native
sed -i "s/___RONDBVERSION___/$RONDB_VERSION/g" pom.xml
mvn deploy \
  -DJenkinsHops.RepoID=Hops \
  -DJenkinsHops.User=$CE_USER \
  -DJenkinsHops.Password=$CE_PASS

rm -rf "$TARBALL_NAME"
