#!/usr/bin/env bash
#
# build_and_test_rdma_transporter.sh
#
# Configure, build, and run the RDMA_Transporter wire-format unit test
# (TAPTEST(RDMA_Transporter), defined in
# storage/ndb/src/common/transporter/RDMA_Transporter.cpp under
# #ifdef TEST_RDMA_TRANSPORTER).
#
# Why this script exists
# ----------------------
# The RDMA_Transporter test is registered in CMake via
# NDB_ADD_TEST(RDMA_Transporter-t ...) inside
# storage/ndb/src/common/transporter/CMakeLists.txt. NDB_ADD_TEST is a
# no-op unless WITH_UNIT_TESTS=ON. Enabling WITH_UNIT_TESTS, however,
# also adds unittest/gunit which pulls in MySQL server pieces
# (component_reference_cache, ext::icu, ...) that are not available
# when WITHOUT_SERVER=ON. The result is that a developer who only
# wants to verify the RDMA wire-format logic cannot rely on the
# CMake-driven test target without first installing the full server
# build prerequisites.
#
# This script provides a minimal, fully scripted alternative:
#   1. CMake configure with WITH_NDB_RDMA=ON and WITH_UNIT_TESTS=OFF.
#   2. Build the convenience libraries the test links against.
#   3. Build the existing ndbclient_static_link_test target to prove
#      an RDMA-enabled NDB API client still links.
#   4. Recompile RDMA_Transporter.cpp with -DTEST_RDMA_TRANSPORTER.
#   5. Compile the mytap tap.cc framework.
#   6. Compile a single-file stub object providing the kernel-only
#      symbols pulled in transitively by ndbtransport but never
#      exercised by the wire-format test.
#   7. Link the test binary and run it with TAP output.
#
# When WITH_UNIT_TESTS=ON becomes viable (e.g. after the server-deps
# situation is fixed upstream, or in a build that includes the server)
# this script can be retired in favour of `ctest -R RDMA_Transporter`.
#
# Requirements:
#   - libibverbs-dev installed (Debian/Ubuntu) or rdma-core-devel
#     (RHEL/Fedora).
#   - A C++20-capable g++ in PATH.
#
# Usage:
#   ./build_scripts/build_and_test_rdma_transporter.sh
#
# Exit codes:
#   0  All TAP tests passed.
#   1  Configure, build, link, or test execution failed.

set -euo pipefail

# Resolve the repo root from the script location so the script works
# regardless of the caller's CWD.
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

BUILD_DIR="${RDMA_TEST_BUILD_DIR:-${REPO_ROOT}/build-rdma-test}"
TEST_DIR="${BUILD_DIR}/test_rdma"

JOBS="${JOBS:-$(nproc 2>/dev/null || echo 4)}"

echo "==> Repo root:  ${REPO_ROOT}"
echo "==> Build dir:  ${BUILD_DIR}"
echo "==> Test dir:   ${TEST_DIR}"
echo "==> Jobs:       ${JOBS}"

# Step 1: CMake configure.
mkdir -p "${BUILD_DIR}"
cd "${BUILD_DIR}"
echo "==> CMake configure"
cmake "${REPO_ROOT}" \
  -DWITH_NDB_RDMA=ON \
  -DWITH_UNIT_TESTS=OFF \
  -DWITH_NDB=ON \
  -DWITH_NDB_JAVA=OFF \
  -DWITH_NDB_NODEJS=OFF \
  -DCMAKE_BUILD_TYPE=Debug \
  -DWITHOUT_SERVER=ON \
  >/dev/null

# Step 2: Build the convenience libraries the test links against. The
# RDMA_Transporter.cpp source itself is rebuilt from scratch in step 4
# with -DTEST_RDMA_TRANSPORTER; everything else comes out of the
# regular archives.
echo "==> Building support libraries"
make -j"${JOBS}" \
  ndbtransport \
  ndbmgmapi \
  ndbmgmcommon \
  ndbgeneral \
  ndbportlib \
  ndblogger \
  ndbsignaldata \
  ndbtrace \
  >/dev/null

echo "==> Building NDB API static link check"
# This target is intentionally build-only; it proves linkage for an
# RDMA-enabled NDB API client without exercising client runtime semantics.
make -j"${JOBS}" ndbclient_static_link_test >/dev/null

# Step 4: Recompile RDMA_Transporter.cpp with the test gate defined.
#
# The compile flags are kept aligned with the per-target flags.make
# that CMake generated for ndbtransport_objlib. If you add a new
# define or warning suppression there, mirror it here.
mkdir -p "${TEST_DIR}"
echo "==> Compiling RDMA_Transporter.cpp with -DTEST_RDMA_TRANSPORTER"
g++ -c -std=c++20 -g -DTEST_RDMA_TRANSPORTER \
  -DHAVE_NDB_CONFIG_H -DHAVE_TLSv13 -DLZ4_DISABLE_DEPRECATE_WARNINGS \
  -DNDB_RDMA_TRANSPORTER_SUPPORTED=1 -D_FILE_OFFSET_BITS=64 -D_GNU_SOURCE \
  -D_USE_MATH_DEFINES -D__STDC_FORMAT_MACROS -D__STDC_LIMIT_MACROS \
  -DSAFE_MUTEX -DENABLED_DEBUG_SYNC \
  -DACC_SAFE_QUEUE -DAPI_TRACE -DARRAY_GUARD -DERROR_INSERT -DNDB_DEBUG -DVM_TRACE \
  "-I${BUILD_DIR}" "-I${BUILD_DIR}/include" \
  "-I${REPO_ROOT}" "-I${REPO_ROOT}/include" "-I${REPO_ROOT}/libs" \
  "-I${REPO_ROOT}/storage/ndb/include" \
  "-I${REPO_ROOT}/storage/ndb/include/util" \
  "-I${REPO_ROOT}/storage/ndb/include/portlib" \
  "-I${REPO_ROOT}/storage/ndb/include/debugger" \
  "-I${REPO_ROOT}/storage/ndb/include/transporter" \
  "-I${REPO_ROOT}/storage/ndb/include/kernel" \
  "-I${REPO_ROOT}/storage/ndb/include/mgmapi" \
  "-I${REPO_ROOT}/storage/ndb/include/mgmcommon" \
  "-I${REPO_ROOT}/storage/ndb/include/ndbapi" \
  "-I${REPO_ROOT}/storage/ndb/include/logger" \
  "-I${BUILD_DIR}/storage/ndb/include" \
  "-I${REPO_ROOT}/storage/ndb/src/mgmapi" \
  -fno-omit-frame-pointer -fno-strict-aliasing -Wno-error \
  "${REPO_ROOT}/storage/ndb/src/common/transporter/RDMA_Transporter.cpp" \
  -o "${TEST_DIR}/RDMA_Transporter-t.o"

# Step 5: Compile the mytap TAP framework.
echo "==> Compiling mytap/tap.cc"
g++ -c -std=c++20 -g -DHAVE_NDB_CONFIG_H \
  "-I${BUILD_DIR}/include" \
  "-I${REPO_ROOT}" "-I${REPO_ROOT}/include" \
  "-I${REPO_ROOT}/unittest/mytap" \
  "${REPO_ROOT}/unittest/mytap/tap.cc" \
  -o "${TEST_DIR}/tap.o"

# Step 6: Build a tiny stubs object for kernel-only symbols pulled in
# transitively by Packer.cpp::pack<SegmentedSectionArg> but never
# reached by the wire-format test. The stub aborts on call so any
# accidental dependency in future tests fails loud and fast.
echo "==> Compiling kernel-only stubs"
STUBS_SRC="${TEST_DIR}/rdma_test_stubs.cpp"
cat > "${STUBS_SRC}" <<'CPP_EOF'
// Stubs for kernel-only symbols pulled in by the transporter library
// but never executed by the RDMA wire-format unit test. If anything
// actually reaches one of these at runtime the program aborts.
#include <cstdio>
#include <cstdlib>

struct SectionSegmentPool;
struct SegmentedSectionPtr;

extern "C" void __rdma_test_abort_stub(const char *name) {
  std::fprintf(stderr, "rdma test stub called: %s\n", name);
  std::abort();
}

bool copy(unsigned int *& /*dst*/, SectionSegmentPool & /*pool*/,
          SegmentedSectionPtr const & /*src*/) {
  __rdma_test_abort_stub("copy(SegmentedSectionPtr)");
  return false;
}
CPP_EOF
g++ -c -std=c++20 -g "${STUBS_SRC}" -o "${TEST_DIR}/stubs.o"

# Step 7: Link the test binary. --start-group/--end-group resolves
# the cyclic references between ndbgeneral, ndbmgmcommon, and
# ndbtrace in the convenience-library set.
echo "==> Linking RDMA_Transporter-t"
TEST_BIN="${TEST_DIR}/RDMA_Transporter-t"
g++ -g -o "${TEST_BIN}" \
  "${TEST_DIR}/RDMA_Transporter-t.o" \
  "${TEST_DIR}/tap.o" \
  "${TEST_DIR}/stubs.o" \
  -Wl,--start-group \
  "${BUILD_DIR}/archive_output_directory/libndbtransport.a" \
  "${BUILD_DIR}/archive_output_directory/libndbmgmapi.a" \
  "${BUILD_DIR}/archive_output_directory/libndbmgmcommon.a" \
  "${BUILD_DIR}/archive_output_directory/libndbgeneral.a" \
  "${BUILD_DIR}/archive_output_directory/libndbportlib.a" \
  "${BUILD_DIR}/archive_output_directory/libndblogger.a" \
  "${BUILD_DIR}/archive_output_directory/libndbsignaldata.a" \
  "${BUILD_DIR}/archive_output_directory/libndbtrace.a" \
  "${BUILD_DIR}/archive_output_directory/libmysys.a" \
  "${BUILD_DIR}/archive_output_directory/libmytime.a" \
  "${BUILD_DIR}/archive_output_directory/libstrings.a" \
  "${BUILD_DIR}/archive_output_directory/libzlib.a" \
  "${BUILD_DIR}/archive_output_directory/libzstd.a" \
  "${BUILD_DIR}/archive_output_directory/libbacktrace.a" \
  -Wl,--end-group \
  -libverbs -lssl -lcrypto -lpthread -ldl -lm -lrt

# Step 8: Run the binary and surface the TAP plan/result lines.
echo "==> Running RDMA_Transporter-t"
echo "----------------------------------------"
"${TEST_BIN}"
echo "----------------------------------------"
echo "==> RDMA wire-format unit tests passed."
