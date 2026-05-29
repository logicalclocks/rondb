/*
   Copyright (c) 2026, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

/*
 * Kernel-only symbol stubs for the RDMA_Transporter-t test binary.
 *
 * Why this file exists
 * --------------------
 * Packer.cpp (compiled into the ndbtransport convenience library that
 * the test executable links against) emits an unconditional explicit
 * template instantiation:
 *
 *     template void Packer::pack<Packer::SegmentedSectionArg>(
 *         Uint32 *theData, SegmentedSectionArg section) const;
 *
 * That instantiation references the free function
 *     bool copy(unsigned int *&, SectionSegmentPool &,
 *               SegmentedSectionPtr const &);
 * which is defined on the kernel side of the build (alongside the
 * SegmentSubPool / SegmentedSectionPtr implementations) and is NOT
 * part of any of the libraries the RDMA_Transporter-t target links
 * against (ndbtransport, ndbmgmapi, ndbgeneral, ndbportlib,
 * ${NDB_RDMA_LIBRARIES}, mytap). The result is an unresolved-symbol
 * link error at the test binary even though the wire-format helpers
 * exercised by the unit tests never invoke this code path.
 *
 * `build_scripts/build_and_test_rdma_transporter.sh` already works
 * around this for its standalone build by generating an equivalent
 * stub file on the fly. This source file bakes the same stub into
 * the regular CMake build so the WITH_UNIT_TESTS=ON CI build can
 * also link RDMA_Transporter-t without dragging in any kernel-side
 * libraries.
 *
 * Safety
 * ------
 * If anything ever reaches the stubbed function at runtime the
 * program aborts with a diagnostic message identifying which stub
 * fired. The wire-format TAP suite must never reach it: the test
 * exercises only `encode_msg_header` / `validate_msg_header` and
 * pure helpers in the anonymous namespace; none of those paths call
 * into `Packer::pack` at all. The abort is purely a defensive
 * tripwire for a future test addition that would otherwise silently
 * link against a sentinel.
 */
#include <cstdio>
#include <cstdlib>

/*
 * Opaque forward declarations of the kernel-side types so the stub
 * signature matches the symbol expected by Packer.cpp's template
 * instantiation without dragging in the actual kernel headers.
 */
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
