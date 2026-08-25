# Building RonDB on RISC-V (riscv64)

RonDB builds and runs on 64-bit RISC-V (rv64gc). This was validated on
Ubuntu 26.04 LTS (riscv64) with gcc 15 and CMake 4. A full single-node cluster
(ndb_mgmd + ndbmtd + mysqld) starts and serves NDBCLUSTER tables.

## Prerequisites (Ubuntu riscv64)

Packages come from ports.ubuntu.com; the base image's default repos are
sufficient.

    apt install -y git build-essential cmake pkg-config bison flex libudev-dev \
      libssl-dev libncurses-dev zlib1g-dev libbrotli-dev patchelf maven \
      libsasl2-dev libldap-dev libaio-dev protobuf-compiler uuid-dev \
      openjdk-17-jdk doxygen bash-completion libtirpc-dev \
      libnuma-dev rpcsvc-proto

(`libnuma-dev` enables NUMA support; `rpcsvc-proto` provides the `rpcgen` tool,
which `libtirpc-dev` does not.)

## Toolchain notes

- CMake 4 removed compatibility with `cmake_minimum_required(VERSION < 3.5)` and
  hard-errors on some bundled third-party trees. Pass
  `-DCMAKE_POLICY_VERSION_MINIMUM=3.5`.
- gcc 14+ emits `-Wcalloc-transposed-args` on some NDB code. A non-Debug build
  (which does not enable `-Werror`) treats it as a warning; or add
  `-Wno-calloc-transposed-args`.
- Set the target explicitly: `-march=rv64gc -mabi=lp64d` (equivalently
  `rv64imafdc`).

## Build

    mkdir build && cd build
    cmake \
      -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_C_FLAGS="-march=rv64gc -mabi=lp64d -Wno-calloc-transposed-args" \
      -DCMAKE_CXX_FLAGS="-march=rv64gc -mabi=lp64d -Wno-calloc-transposed-args" \
      -DWITH_SSL=system -DWITH_NDB=1 \
      ..
    make -j$(nproc)

A Release build is recommended: it is far smaller on disk than Debug (no DWARF)
and does not enable `-Werror`, so newer-compiler warnings are non-fatal.

## Single-node smoke test

    cmake --install . --prefix /path/to/install
    # config.ini: NoOfReplicas=1, one [ndb_mgmd], one [ndbd], one [mysqld].
    # Cap memory with TotalMemoryConfig (RonDB's AutomaticMemoryConfig otherwise
    # sizes the data node to all system RAM).
    ndb_mgmd --initial -f config.ini --configdir=...
    ndbmtd  -c 127.0.0.1:1186 --initial      # ndbd for a single-hart target
    ndb_mgm -c 127.0.0.1:1186 -e show        # wait for the data node = started
    mysqld  --defaults-file=my.cnf --user=root --initialize-insecure
    mysqld  --defaults-file=my.cnf --user=root &
    mysql   --socket=... -e "CREATE TABLE t(id INT PRIMARY KEY) ENGINE=NDBCLUSTER;"

## RDRS on RISC-V

RDRS (REST + Rondis + RonSQL) builds with `-DWITH_RDRS=1`. It additionally needs
Go (`golang-go`; Go supports linux/riscv64) and prometheus-cpp 1.3.x installed
as a system package (find_package CONFIG).
