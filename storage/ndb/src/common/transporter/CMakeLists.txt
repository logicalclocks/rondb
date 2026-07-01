# Copyright (c) 2008, 2026, Oracle and/or its affiliates.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0,
# as published by the Free Software Foundation.
#
# This program is designed to work with certain software (including
# but not limited to OpenSSL) that is licensed under separate terms,
# as designated in a particular file or component or in included license
# documentation.  The authors of MySQL hereby grant you an additional
# permission to link the program and your derivative works with the
# separately licensed software that they have either included with
# the program or referenced in the documentation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License, version 2.0, for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA


INCLUDE_DIRECTORIES(
  ${CMAKE_SOURCE_DIR}/storage/ndb/include/mgmapi
  ${CMAKE_SOURCE_DIR}/storage/ndb/src/mgmapi
  ${CMAKE_SOURCE_DIR}/storage/ndb/include/debugger
  ${CMAKE_SOURCE_DIR}/storage/ndb/include/kernel
  ${CMAKE_SOURCE_DIR}/storage/ndb/include/transporter)

IF(NOT WIN32)
  SET(EXTRA_SRC SHM_Transporter.unix.cpp SHM_Transporter.cpp)
ENDIF()

# Compile the RDMA transporter only when WITH_NDB_RDMA is enabled and
# libibverbs was located by ndb_configure.cmake.
IF(NDB_RDMA_TRANSPORTER_SUPPORTED)
  LIST(APPEND EXTRA_SRC RDMA_Transporter.cpp)
ENDIF()

ADD_CONVENIENCE_LIBRARY(ndbtransport
  Transporter.cpp
  TCP_Transporter.cpp
  Loopback_Transporter.cpp
  TransporterRegistry.cpp
  Packer.cpp
  Multi_Transporter.cpp
  ${EXTRA_SRC}
  )
IF(NDB_RDMA_TRANSPORTER_SUPPORTED)
  TARGET_LINK_LIBRARIES(ndbtransport PUBLIC ${NDB_RDMA_LIBRARIES})
ENDIF()

# Unit tests for the RDMA wire-format helpers. Only built when the
# WITH_UNIT_TESTS flag is set (NDB_ADD_TEST returns silently otherwise)
# and the RDMA transporter is compiled in. The test binary recompiles
# RDMA_Transporter.cpp with TEST_RDMA_TRANSPORTER defined; NDB_ADD_TEST
# adds -DTEST_RDMA_TRANSPORTER automatically. The encode_msg_header /
# validate_msg_header logic exercised by the tests does not touch any
# verbs or socket APIs, but the rest of the translation unit still does,
# so we link against ${NDB_RDMA_LIBRARIES} to resolve those references.
#
# RDMA_Transporter_test_stubs.cpp is a tiny companion source that
# provides an abort-stub for the kernel-side `copy(SectionSegmentPool&,
# SegmentedSectionPtr const&)` symbol that Packer.cpp's explicit
# `Packer::pack<SegmentedSectionArg>` instantiation references. None
# of the wire-format helpers exercised by the TAP cases actually call
# into that code path, but without the stub the test executable fails
# to link because none of the libraries we depend on define the
# kernel-side symbol. The same workaround is applied at runtime by
# build_scripts/build_and_test_rdma_transporter.sh; this baked-in
# stub keeps the WITH_UNIT_TESTS=ON CI build working too. See
# RDMA_Transporter_test_stubs.cpp for the rationale.
IF(NDB_RDMA_TRANSPORTER_SUPPORTED)
  NDB_ADD_TEST(RDMA_Transporter-t
               RDMA_Transporter.cpp
               RDMA_Transporter_test_stubs.cpp
               LIBS ndbtransport ndbmgmapi ndbgeneral ndbportlib
                    ${NDB_RDMA_LIBRARIES})
ENDIF()
