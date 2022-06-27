#
# This Dockerfile is used to build RonDB 21.04.8.
# We are building the release version using GCC 9.3.1 currently.
#
# Create a directory where you want to create the docker image from.
# Next copy 5 tarballs there
# 1) rondb-gpl-21.04.8.tar.gz
# 2) build_scripts.tar.gz
# 3) sysbench-0.4.12.17.tar.gz
# 4) dbt2-0.37.50.18.tar.gz
# 5) dbt3-1.10.tar.gz
#
# Next build the Docker image using the command:
# docker build -t rondb_ol7_build .
# 
# Now your Docker image is ready to use, start the
# Docker instance and execute bash into it using the
# command:
# docker run -v $HOME/data:/data -v $HOME/build:/build --name build --rm -i -t rondb_ol7_build bash
#
# This starts a bash terminal in the Docker container created.
# When entering the Docker image the first step is to copy the tarballs to the
# build directory using the command:
# mv *.tar.gz build/.
#
# Now if you want to build a release version you execute the following command
# first:
# export BUILD_RELEASE="yes"
#
# Next you build the RonDB binary tarball using the command:
# build_scripts/build_all.sh
#
# After some time this build has completed.
# Now copy the RonDB binary tarball to the /data directory
# cp /rondb-21.04.8-linux-glibc2.17-x86_64.tar.gz /data/.
# Exit the Docker container
# exit
#
# cd $HOME/data
# The binary tarball are owned by root. Change this to your user
# chown $USER rondb-21.04.8-linux-glibc2.17-x86_64.tar.gz
# chgrp $USER rondb-21.04.8-linux-glibc2.17-x86_64.tar.gz
#
# Now your RonDB binary tarball is created and ready to use
#

FROM oraclelinux:7-slim

ARG cmake_version=3.23.2
ARG boost_version=1.73.0
ARG rondb_version=21.04.8
ARG lib_ssl_version=1.1.1o
ARG userid=1000
ARG groupid=1000

COPY epel.txt /epel.txt

RUN cd / ; \
mkdir /etc/yum/repos.d; \
cp epel.txt /etc/yum/repos.d/epel-yum-ol7.repo; \
yum repolist; \
yum -y update;

RUN yum -y install wget make git which perl-core openldap-devel

RUN yum -y install bison krb5-server krb5-workstation krb5-devel

RUN yum -y install numactl numactl-libs numactl-devel; \
yum -y install patchelf ncurses-devel java-1.8.0-openjdk-devel automake; \
yum -y install zlib-devel; \
yum -y install scl-utils; \
yum -y install oracle-softwarecollection-release-el7; \
yum-config-manager -y --enable "ol7_optional_latest"; \
yum -y install devtoolset-9;

RUN source scl_source enable devtoolset-9; \
wget --progress=bar:force https://www.openssl.org/source/openssl-${lib_ssl_version}.tar.gz; \
tar xf openssl-${lib_ssl_version}.tar.gz; \
cd openssl-${lib_ssl_version}; \
./config --prefix=/usr/local/ssl --openssldir=/usr/local/ssl shared zlib; \
make -j$(nproc); \
make install; \
echo "/usr/local/ssl/lib" >> /etc/ld.so.conf.d/openssl-${lib_ssl_version}.conf; \
ldconfig -v; \
cd ..; \
rm -rf openssl-${lib_ssl_version}; \
echo Installing cmake; \
source scl_source enable devtoolset-9; \
wget --progress=bar:force https://github.com/Kitware/CMake/releases/download/v${cmake_version}/cmake-${cmake_version}.tar.gz; \
tar xzf cmake-${cmake_version}.tar.gz; \
cd cmake-${cmake_version}; \
export OPENSSL_ROOT_DIR=/usr/local/ssl/; \
./bootstrap --prefix=/usr/local; \
make -j$(nproc); \
make install; \
cd ..; \
rm -r cmake-${cmake_version}*; \
groupadd mikael --gid ${groupid}; \
useradd -ms /bin/bash mikael --uid ${userid} --gid ${groupid}; \
echo Installing Boost; \
BOOST_V=`echo ${boost_version} | sed s/'\.'/'_'/g`; \
source scl_source enable devtoolset-9; \
wget --progress=bar:force https://boostorg.jfrog.io/artifactory/main/release/${boost_version}/source/boost_${BOOST_V}.tar.gz ; \
tar xzf boost_${BOOST_V}.tar.gz; \
rm boost_${BOOST_V}.tar.gz;

RUN echo "source scl_source enable devtoolset-9" >> /root/.bashrc; \
BOOST_V=`echo ${boost_version} | sed s/'\.'/'_'/g`; \
echo "export BOOST_ROOT=/boost_${BOOST_V}" >> /root/.bashrc; \
echo "export BOOST_VERSION=73" >> /root/.bashrc; \
echo "export RONDB_VERSION=${rondb_version}" >> /root/.bashrc; \
echo "export OPENSSL_ROOT=/usr/local/ssl" >> /root/.bashrc

COPY build_scripts.tar.gz /build_scripts.tar.gz

VOLUME ["/data"]
VOLUME ["/build"]

COPY rondb-gpl-${rondb_version}.tar.gz /rondb-gpl-${rondb_version}.tar.gz

COPY sysbench-0.4.12.17.tar.gz /sysbench-0.4.12.17.tar.gz

COPY dbt2-0.37.50.18.tar.gz /dbt2-0.37.50.18.tar.gz

COPY dbt3-1.10.tar.gz /dbt3-1.10.tar.gz

RUN cd / ; \
tar xfz build_scripts.tar.gz; \
rm build_scripts.tar.gz

USER root

CMD ["/usr/bin/scl", "enable", "devtoolset-9", "bash"]
