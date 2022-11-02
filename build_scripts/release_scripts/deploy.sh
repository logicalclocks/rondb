#!/bin/bash

set -e

GLIBC=2.17

TAR_FILE=$1
CE_USER=___CE_USER___
CE_PASS=___CE_PASS___
EE_USER=___EE_USER___
EE_PASS=___EE_PASS___

RONDB_VERSION=$(echo $TAR_FILE | grep -oh "rondb-[0-9][0-9]\.[0-9][0-9]\.[0-9]-linux-glibc" | grep -oh "[0-9][0-9]\.[0-9][0-9]\.[0-9]")

TAR_FILE_ABS=$(readlink -f $TAR_FILE)
if [[ ! -f "$TAR_FILE_ABS" ]]; then
  echo "Error: Unable to find tar ball $TAR_FILE_ABS"
  exit 1
fi

DST="repo@repo.hops.works:/opt/repository/master/rondb-$RONDB_VERSION-linux-glibc$GLIBC-x86_64.tar.gz"
echo "Copying: $TAR_FILE_ABS to $DST"
scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i ./id_rsa $TAR_FILE_ABS $DST

JAR_FILE="rondb-$RONDB_VERSION-linux-glibc$GLIBC-x86_64/share/java/clusterj-$RONDB_VERSION.jar"
tar xf $TAR_FILE_ABS $JAR_FILE

if [[ ! -f "$JAR_FILE" ]]; then
  echo "Error: Unable to find cluster file. $JAR_FILE"
  exit 1
fi

mvn deploy:deploy-file -Dfile=$JAR_FILE -DgroupId=com.mysql.ndb -DartifactId=clusterj-rondb \
-Dversion=$RONDB_VERSION -Dpackaging=jar -DrepositoryId=Hops \
-Durl=https://archiva.hops.works/repository/Hops \
-DJenkinsHops.RepoID=Hops \
-DJenkinsHops.User=$CE_USER \
-DJenkinsHops.Password=$CE_PASS


mvn deploy:deploy-file -Dfile=$JAR_FILE -DgroupId=com.mysql.ndb -DartifactId=clusterj-rondb \
-Dversion=$RONDB_VERSION -Dpackaging=jar -DrepositoryId=HopsEE \
-Durl=https://nexus.hops.works/repository/hops-artifacts \
-DJenkinsHops.RepoID=HopsEE \
-DJenkinsHops.User=$EE_USER \
-DJenkinsHops.Password=$EE_PASS


LIBNDB_FILE="rondb-$RONDB_VERSION-linux-glibc$GLIBC-x86_64/lib/libndbclient.so.6.1.0"
tar xf $TAR_FILE_ABS $LIBNDB_FILE

if [[ ! -f "$LIBNDB_FILE" ]]; then
  echo "Error: Unable to find libndbclient.so.6.1.0 file. $JAR_FILE"
  exit 1
fi

git clone --branch jenkins https://github.com/hopshadoop/clusterj-native
cp "rondb-$RONDB_VERSION-linux-glibc$GLIBC-x86_64/lib/libndbclient.so.6.1.0"  clusterj-native/src/main/resources/libndbclient.so

cd clusterj-native
sed -i "s/___RONDBVERSION___/$RONDB_VERSION/g" pom.xml
mvn deploy \
-DJenkinsHops.RepoID=Hops \
-DJenkinsHops.User=$CE_USER \
-DJenkinsHops.Password=$CE_PASS

rm -rf "rondb-$RONDB_VERSION-linux-glibc$GLIBC-x86_64"
