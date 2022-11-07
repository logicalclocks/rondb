# syntax=docker/dockerfile:1.3-labs

FROM --platform=$TARGETPLATFORM ubuntu:22.04 as rondb-build-dependencies
ARG BUILDPLATFORM
ARG TARGETPLATFORM

RUN echo "Running on $BUILDPLATFORM, building for $TARGETPLATFORM"

ARG USER=mysql
ARG BOOST_VERSION_MAJOR=1
ARG BOOST_VERSION_MINOR=73
ARG BOOST_VERSION_PATCH=0

# Default build threads to 1; max is defined in Docker config (run `nproc` in Docker container)
ARG BUILD_THREADS
ENV THREADS_ARG=${BUILD_THREADS:-1}

RUN apt-get update && apt-get -y install wget pkg-config patchelf \
    libncurses5-dev default-jdk bison golang-go

# Ubuntu 22.04 comes with gcc/g++ 11.3 by default; we need version 9
# We will not remove the default installation, but change the default
# one using `update-alternatives`
# See https://askubuntu.com/a/26518/1646182 for reference
RUN apt-get update && apt-get install -y gcc-9 g++-9 \
    && update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-9 10 \
    && update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-9 10

# ENV CXX=/usr/bin/gcc-9
# ENV CPP=/usr/bin/g++-9

# Installing Boost
ENV BOOST_VERSION=${BOOST_VERSION_MAJOR}.${BOOST_VERSION_MINOR}.${BOOST_VERSION_PATCH}
ENV BOOST_V_UNDERSCORE=${BOOST_VERSION_MAJOR}_${BOOST_VERSION_MINOR}_${BOOST_VERSION_PATCH}
ENV BOOST_LABEL="boost_${BOOST_V_UNDERSCORE}"
ENV BOOST_TARBALL="${BOOST_LABEL}.tar.gz"
RUN wget --progress=bar:force https://boostorg.jfrog.io/artifactory/main/release/${BOOST_VERSION}/source/${BOOST_TARBALL} \
    && tar xzf ${BOOST_TARBALL} \
    && rm ${BOOST_TARBALL}
# Important env variable for RonDB compilation
ENV BOOST_ROOT="/${BOOST_LABEL}"

# Ubuntu 22.04 contains openssl-3.x
# RonDB requires openssl-1.1.1m and will look for it in $OPENSSL_ROOT
# We will not overwrite the default OpenSSL, but just install version 1.1.1m
# Commands are from https://linuxpip.org/install-openssl-linux/
ENV OPENSSL_ROOT=/usr/local/ssl
RUN apt-get update -y \
    && apt-get install -y build-essential checkinstall zlib1g-dev \
    && cd /usr/local/src/ \
    && wget --progress=bar:force https://www.openssl.org/source/openssl-1.1.1m.tar.gz \
    && tar -xf openssl-1.1.1m.tar.gz \
    && cd openssl-1.1.1m \
    && ./config --prefix=$OPENSSL_ROOT --openssldir=$OPENSSL_ROOT shared zlib \
    && make -j$THREADS_ARG \
    && make install \
    && echo "$OPENSSL_ROOT/lib" >> /etc/ld.so.conf.d/openssl-1.1.1m.conf \
    && ldconfig -v \
    && cd .. \
    && rm -r openssl-1.1.1m.*
    # Could also run `make test`
    # `make install` places shared libraries into $OPENSSL_ROOT

# Installing CMake
# CMake will look for $OPENSSL_ROOT_DIR
ENV OPENSSL_ROOT_DIR=$OPENSSL_ROOT
RUN wget --progress=bar:force https://github.com/Kitware/CMake/releases/download/v3.23.2/cmake-3.23.2.tar.gz \
    && tar xzf cmake-3.23.2.tar.gz \
    && cd cmake-3.23.2 \
    && ./bootstrap --prefix=/usr/local \
    && make -j$THREADS_ARG \
    && make install \
    && cd .. \
    && rm -r cmake-3.23.2*

RUN groupadd mysql && adduser mysql --ingroup mysql --shell /bin/bash

WORKDIR /home/${USER}

# For the deployment of Java artifacts to the Maven repository
COPY <<-"EOF" .m2/settings.xml
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0 http://maven.apache.org/xsd/settings-1.0.0.xsd">
    <servers>
        <server>
            <id>${JenkinsHops.RepoID}</id>
            <username>${JenkinsHops.User}</username>
            <filePermissions>664</filePermissions>
            <directoryPermissions>775</directoryPermissions>
            <password>${JenkinsHops.Password}</password>
        </server>
    </servers>
</settings>
EOF

# See https://stackoverflow.com/a/51264575/9068781 for conditional envs
FROM rondb-build-dependencies as build-all
ARG DEPLOY_TO_REPO
ENV DEPLOY_ARG=${DEPLOY_TO_REPO:+-d}

ARG RELEASE_TARBALL
ENV RELEASE_ARG=${RELEASE_TARBALL:+-r}

RUN mkdir rondb-src rondb-bin rondb-tarballs
RUN --mount=type=bind,source=.,target=rondb-src \
    rondb-src/build_scripts/release_scripts/build_all.sh \
    -s rondb-src \
    -b rondb-bin \
    -o rondb-tarballs \
    -j $THREADS_ARG \
    $DEPLOY_ARG $RELEASE_ARG

# TODO: Make mysql a variable/argument
# run with --output <output-folder>
FROM scratch AS get-package-all
COPY --from=build-all /home/mysql/rondb-tarballs .

# TODO: Check owner of all dirs in /home/mysql
# TODO: Make Boost library a build-arg.. it varies between RonDB versions..
