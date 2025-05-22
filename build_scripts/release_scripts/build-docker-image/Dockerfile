# syntax=docker/dockerfile:1

# Explaining order of ARGS in Dockerfiles: https://stackoverflow.com/a/53683625/9068781

ARG RONDB_TARBALL_LOCAL_REMOTE=remote

# Download all required Ubuntu dependencies
FROM ubuntu:22.04 AS rondb_runtime_dependencies

ARG BUILDPLATFORM
ARG TARGETPLATFORM

ARG TARGETARCH
ARG TARGETVARIANT

RUN echo "Running on $BUILDPLATFORM, building for $TARGETPLATFORM"
RUN echo "TARGETARCH: $TARGETARCH; TARGETVARIANT: $TARGETVARIANT"

# Fail the build if TARGETARCH is not arm64 or amd64
RUN if [ "$TARGETARCH" != "arm64" ] && [ "$TARGETARCH" != "amd64" ]; then \
        echo "Unsupported architecture: $TARGETARCH" && exit 1; \
    fi

RUN --mount=type=cache,target=/var/cache/apt,id=ubuntu22-apt-$TARGETPLATFORM \
    --mount=type=cache,target=/var/lib/apt/lists,id=ubuntu22-apt-lists-$TARGETPLATFORM \
    apt-get update -y \
    && apt-get install -y wget tar gzip vim \
    libaio1 libaio-dev \
    libncurses5 libnuma-dev \
    bc default-jdk maven libcurl4-openssl-dev \
    sudo iproute2 dnsutils
    # Java & Maven are required by YCSB
    # bc is required by dbt2
    # libaio is a dynamic library used by RonDB
    # libncurses5 & libnuma-dev are required for x86 only
    # sudo is required in the entrypoint
    # iproute2 is for using the command `ss`
    # dnsutils is for using the command `nslookup` in K8s

# Let PATH survive through sudo
RUN sed -ri '/secure_path/d' /etc/sudoers

# Creating a cache dir for downloads to avoid redownloading
ENV DOWNLOADS_CACHE_DIR=/tmp/downloads
RUN mkdir $DOWNLOADS_CACHE_DIR

# Copying bare minimum of Hopsworks cloud environment for now
FROM rondb_runtime_dependencies AS cloud_preparation
ARG RONDB_VERSION=21.04.16
ENV HOPSWORK_DIR=/srv/hops
ENV RONDB_BIN_DIR=$HOPSWORK_DIR/mysql-$RONDB_VERSION
RUN mkdir -p $RONDB_BIN_DIR

# Get RonDB tarball from local path & unpack it
FROM cloud_preparation AS local_tarball
ARG RONDB_X86_TARBALL_URI
ARG RONDB_ARM_TARBALL_URI

RUN case "$TARGETARCH" in \
        amd64) echo "export TARBALL_PATH=$RONDB_X86_TARBALL_URI" >> /env.sh;; \
        arm64) echo "export TARBALL_PATH=$RONDB_ARM_TARBALL_URI" >> /env.sh;; \
    esac

RUN --mount=type=bind,source=.,target=/context \
    . /env.sh \
    && tar xfz /context/${TARBALL_PATH} -C $RONDB_BIN_DIR --strip-components=1

# Get RonDB tarball from remote url & unpack it
FROM cloud_preparation AS remote_tarball
ARG RONDB_X86_TARBALL_URI
ARG RONDB_ARM_TARBALL_URI

RUN case "$TARGETARCH" in \
        amd64) TARBALL_URL=$RONDB_X86_TARBALL_URI;; \
        arm64) TARBALL_URL=$RONDB_ARM_TARBALL_URI;; \
    esac \
    && wget $TARBALL_URL -O ./temp_tarball.tar.gz \
    && tar xfz ./temp_tarball.tar.gz -C $RONDB_BIN_DIR --strip-components=1 \
    && rm ./temp_tarball.tar.gz

FROM ${RONDB_TARBALL_LOCAL_REMOTE}_tarball

WORKDIR $HOPSWORK_DIR

# We use symlinks in case we want to exchange binaries
ENV RONDB_BIN_DIR_SYMLINK=$HOPSWORK_DIR/mysql
RUN ln -s $RONDB_BIN_DIR $RONDB_BIN_DIR_SYMLINK

ENV PATH=$RONDB_BIN_DIR_SYMLINK/bin:$PATH

# Add RonDB libs to system path (cannot use env variables here)
COPY <<-"EOF" /etc/ld.so.conf.d/rondb.conf
/srv/hops/mysql/lib
/srv/hops/mysql/lib/private
EOF
RUN ldconfig --verbose

ENV RONDB_DATA_DIR=$HOPSWORK_DIR/mysql-cluster
ENV MGMD_DATA_DIR=$RONDB_DATA_DIR/mgmd
ENV MYSQLD_DATA_DIR=$RONDB_DATA_DIR/mysql
ENV NDBD_DATA_DIR=$RONDB_DATA_DIR/ndb_data

RUN mkdir -p $MGMD_DATA_DIR $NDBD_DATA_DIR $MYSQLD_DATA_DIR

ENV MYSQL_FILES_DIR=$RONDB_DATA_DIR/mysql-files
RUN mkdir -p $MYSQL_FILES_DIR

ENV LOG_DIR=$RONDB_DATA_DIR/log
ENV RONDB_SCRIPTS_DIR=$RONDB_DATA_DIR/ndb/scripts
ENV BACKUP_DATA_DIR=$RONDB_DATA_DIR/ndb/backups
ENV DISK_COLUMNS_DIR=$RONDB_DATA_DIR/ndb_disk_columns
ENV MYSQL_UNIX_PORT=$RONDB_DATA_DIR/mysql.sock

RUN mkdir -p $LOG_DIR $RONDB_SCRIPTS_DIR $BACKUP_DATA_DIR $DISK_COLUMNS_DIR

COPY ./resources/rondb_scripts $RONDB_SCRIPTS_DIR
ENV PATH=$RONDB_SCRIPTS_DIR:$PATH

RUN touch $MYSQL_UNIX_PORT

# We expect this image to be used as base image to other
# images with additional files specific to Docker
COPY ./resources/entrypoints ./docker/rondb_standalone/entrypoints
COPY ./resources/healthcheck.sh ./docker/rondb_standalone/healthcheck.sh

# Can be used to mount SQL init scripts
ENV SQL_INIT_SCRIPTS_DIR=$HOPSWORK_DIR/docker/rondb_standalone/sql_init_scripts
RUN mkdir $SQL_INIT_SCRIPTS_DIR

# Creating benchmarking files/directories
# When using load balancers, "sysbench" can be used for both _single and _multi
ENV BENCHMARKS_DIR=$HOPSWORK_DIR/benchmarks
RUN mkdir $BENCHMARKS_DIR && cd $BENCHMARKS_DIR \
    && mkdir -p sysbench sysbench_single sysbench_multi dbt2_single dbt2_multi dbt2_data

# These directories have to have 777 permissions if we want to
# run RonDB containers with arbitrary users
RUN chmod 777 -R $RONDB_DATA_DIR \
    && chmod 777 -R $HOPSWORK_DIR/docker \
    && chmod 777 -R $BENCHMARKS_DIR

# Remove awkward message when using arbitrary user
RUN echo "PS1='${debian_chroot:+(\$debian_chroot)}\h:\w\$ '" >> /etc/bash.bashrc

ENTRYPOINT ["./docker/rondb_standalone/entrypoints/main.sh"]
EXPOSE 3306 33060 11860 1186 4406 5406 6379
CMD ["mysqld"]
