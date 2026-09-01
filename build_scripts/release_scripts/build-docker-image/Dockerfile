# syntax=docker/dockerfile:1

# Explaining order of ARGS in Dockerfiles: https://stackoverflow.com/a/53683625/9068781

ARG RONDB_TARBALL_LOCAL_REMOTE=remote

# ---------------------------------------------------------------------------
# Builder base.
#
# This stage only exists to fetch and unpack the tarball. Nothing from its
# filesystem reaches the runtime image except the pruned tree COPYed out of the
# "pruner" stage below. That is deliberate: the untar is a single ~1.65 GB
# layer, so deleting files in a later layer of the same image would not have
# reclaimed a single byte.
# ---------------------------------------------------------------------------
FROM ubuntu:22.04 AS tarball_base

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

# wget is needed to fetch the tarball in "remote" mode and ca-certificates to
# verify the TLS chain. Both are build-time only and never reach the runtime
# image, which is why the runtime stage no longer installs wget at all.
RUN apt-get update -y \
    && apt-get install -y --no-install-recommends wget ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ARG RONDB_VERSION=21.04.16
ENV HOPSWORK_DIR=/srv/hops
ENV RONDB_BIN_DIR=$HOPSWORK_DIR/mysql-$RONDB_VERSION
RUN mkdir -p $RONDB_BIN_DIR

# Get RonDB tarball from local path & unpack it
FROM tarball_base AS local_tarball
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
FROM tarball_base AS remote_tarball
ARG RONDB_X86_TARBALL_URI
ARG RONDB_ARM_TARBALL_URI

RUN case "$TARGETARCH" in \
        amd64) TARBALL_URL=$RONDB_X86_TARBALL_URI;; \
        arm64) TARBALL_URL=$RONDB_ARM_TARBALL_URI;; \
    esac \
    && wget $TARBALL_URL -O ./temp_tarball.tar.gz \
    && tar xfz ./temp_tarball.tar.gz -C $RONDB_BIN_DIR --strip-components=1 \
    && rm ./temp_tarball.tar.gz

# ---------------------------------------------------------------------------
# Remove test material from the tarball. Anything not named here is kept.
#
#   mysql-test/          the MTR suite. 778 MB.
#   feature_store_test   Go test binary. 30 MB.
#   api_key_test         Go test binary. 17 MB.
#
# The two Go binaries are named individually rather than matched by a pattern
# because they are the only files in the image that a vulnerability scanner
# flags: both embed the Go standard library, and trivy reports 28 findings
# (20 HIGH) against each. Every other test binary in bin/ is C++ and carries
# no findings, so there is no security reason to remove them.
#
# Comments must stay outside the RUN: a "#" line inside a backslash-continued
# RUN is removed by the Dockerfile parser.
# ---------------------------------------------------------------------------
FROM ${RONDB_TARBALL_LOCAL_REMOTE}_tarball AS pruner

ENV REQUIRED_BINS="mysqld ndb_mgmd ndbmtd ndb_mgm ndb_waiter mysql mysqladmin mysqldump mysqlpump ndb_restore rdrs rdrs2 bench_run.sh"

ENV GO_TEST_BINS="feature_store_test api_key_test"

RUN set -eu; \
    cd "$RONDB_BIN_DIR"; \
    before=$(du -sm . | cut -f1); \
    rm -rf mysql-test; \
    for f in $GO_TEST_BINS; do \
        if [ -e "bin/$f" ]; then rm -f "bin/$f"; \
        else echo "NOTE: bin/$f not in this tarball"; fi; \
    done; \
    missing=""; \
    for k in $REQUIRED_BINS; do \
        if [ ! -e "bin/$k" ] && [ ! -L "bin/$k" ]; then missing="$missing $k"; fi; \
    done; \
    if [ -n "$missing" ]; then \
        echo "ERROR: prune removed or tarball lacks required binaries:$missing" >&2; \
        exit 1; \
    fi; \
    after=$(du -sm . | cut -f1); \
    echo "tarball tree: ${before} MB -> ${after} MB"

# ---------------------------------------------------------------------------
# Runtime image.
#
# A clean ubuntu:22.04 that receives only the pruned tree. ubuntu:22.04 is
# already the minimal Ubuntu rootfs (78 MB) and is glibc 2.35; the binaries are
# built against glibc 2.28 and glibc is backward-compatible only, so a newer
# base is fine and Alpine/musl can never work.
# ---------------------------------------------------------------------------
FROM ubuntu:22.04

ARG TARGETPLATFORM
ARG RONDB_VERSION=21.04.16

# Runtime shared-library dependencies only, derived from ldd/DT_NEEDED over the
# kept binaries. Everything else they link is already in ubuntu:22.04
# (libc, libdl, libgcc_s, libm, libpthread, libresolv, librt, libstdc++, libz,
# libncurses.so.6, libtinfo.so.6), and so are the tools the scripts shell out
# to (bash, awk, sed, grep, find, tar, gzip, timeout, seq, mktemp, tr, tee).
#
#   libnuma1    libnuma.so.1, needed by mysqld
#   libcurl4    libcurl.so.4, needed by rdrs2
#   libaio1     in no binary's NEEDED set; ~10 kB kept as insurance against a
#               dlopen. The -dev variant it replaces was build-time only.
#   libncurses6 already present via the base; named explicitly so a future base
#               change cannot silently remove libncurses.so.6 / libtinfo.so.6.
#               Note this replaces libncurses5 - the binaries link ncurses 6.
#   dnsutils    nslookup. Required by the rondb-helm ndbmtd livenessProbe and
#               every init container, and its output is parsed as /^Address: /,
#               so a BusyBox nslookup would not do.
#   iproute2    ss, used by rondb_scripts/mgm-server-start.sh
#   bc          NOT only a dbt2 dependency, despite the old comment here:
#               dbt2_install/scripts/run_oltp.sh line 760 pipes through bc to
#               compute the standard deviation in the *sysbench* analyze step.
#               Without it every benchmark run ends with "bc: command not found"
#               and reports no summary.
#
# Dropped from the previous list: wget (builder-only now), vim, sudo,
# default-jdk and maven (no YCSB ships in this image or the chart), plus the
# -dev variants libaio-dev, libnuma-dev and libcurl4-openssl-dev.
#
# The apt lists are deleted in the same layer that creates them, so they never
# enter the image. No --mount=type=cache: production builds with --no-cache
# anyway, and a cache mount shared between two stages that buildx runs in
# parallel deadlocks on /var/lib/apt/lists/lock.
RUN apt-get update -y \
    && apt-get install -y --no-install-recommends \
    libaio1 libnuma1 libcurl4 libncurses6 \
    bc \
    dnsutils iproute2 \
    && rm -rf /var/lib/apt/lists/*

# Creating a cache dir for downloads to avoid redownloading
ENV DOWNLOADS_CACHE_DIR=/tmp/downloads
RUN mkdir $DOWNLOADS_CACHE_DIR

# Copying bare minimum of Hopsworks cloud environment for now
ENV HOPSWORK_DIR=/srv/hops
ENV RONDB_BIN_DIR=$HOPSWORK_DIR/mysql-$RONDB_VERSION

# COPY preserves symlinks, so bin/rdrs -> rdrs2 survives.
COPY --from=pruner $RONDB_BIN_DIR $RONDB_BIN_DIR

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
