/*
 * Copyright (c) 2026, Hopsworks and/or its affiliates.
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2.0,
 * as published by the Free Software Foundation.
 *
 * This program is also distributed with certain software (including
 * but not limited to OpenSSL) that is licensed under separate terms,
 * as designated in a particular file or component or in included license
 * documentation.  The authors of MySQL hereby grant you an additional
 * permission to link the program and your derivative works with the
 * separately licensed software that they have included with MySQL.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

#ifndef CTE_LINKED_ATTR_HPP
#define CTE_LINKED_ATTR_HPP

#include "ndb_types.h"

/**
 * Inline encoding of CTE virtual-column type info in the per-entry
 * 96-bit header of the linked-attr buffer.
 *
 * Layout per entry today:
 *   [word 0][word 1][AttributeHeader][data...]
 *
 *   Real table:    word 0 = tableId, word 1 = schemaVersion
 *   CTE virt col:  word 0 = 0x80000000 | (typeId << 16) | maxBytes,
 *                  word 1 = (csNumber << 16) | flags
 *
 * Bit 31 of word 0 is the marker.  Real NDB tableIds always fit in
 * 31 bits (max table count is far below 2^31), so the marker is
 * unambiguous; a defensive ndbrequire(tableId < 0x80000000u) at
 * table create guards the invariant.
 *
 * Receivers MUST check the marker bit before treating word 0 as a
 * tableId — see JoinAggInterpreter::initGBTypes.
 */
namespace CteLinkedAttr {
  static constexpr Uint32 MARKER_BIT      = 0x80000000u;
  static constexpr Uint32 TYPEID_SHIFT    = 16;
  static constexpr Uint32 TYPEID_MASK     = 0x7Fu;        // 7 bits
  static constexpr Uint32 MAX_BYTES_MASK  = 0xFFFFu;      // 16 bits
  static constexpr Uint32 CS_SHIFT        = 16;
  static constexpr Uint32 CS_MASK         = 0xFFFFu;      // 16 bits
  static constexpr Uint32 FLAGS_MASK      = 0xFFFFu;      // 16 bits, reserved

  /** Encode word 0 of a CTE-virt-column linked-attr entry. */
  static inline Uint32 encodeWord0(Uint32 typeId, Uint32 maxBytes) {
    return MARKER_BIT
         | ((typeId & TYPEID_MASK) << TYPEID_SHIFT)
         | (maxBytes & MAX_BYTES_MASK);
  }

  /** Encode word 1 of a CTE-virt-column linked-attr entry. */
  static inline Uint32 encodeWord1(Uint32 csNumber, Uint32 flags = 0) {
    return ((csNumber & CS_MASK) << CS_SHIFT)
         | (flags & FLAGS_MASK);
  }

  /** Returns true if word 0 is a CTE-virt-column marker (not a tableId). */
  static inline bool isCteMarker(Uint32 word0) {
    return (word0 & MARKER_BIT) != 0;
  }

  /** Decode typeId from a CTE-marked word 0. */
  static inline Uint32 decodeTypeId(Uint32 word0) {
    return (word0 >> TYPEID_SHIFT) & TYPEID_MASK;
  }

  /** Decode maxBytes from a CTE-marked word 0. */
  static inline Uint32 decodeMaxBytes(Uint32 word0) {
    return word0 & MAX_BYTES_MASK;
  }

  /** Decode csNumber from a CTE-marked word 1. */
  static inline Uint32 decodeCsNumber(Uint32 word1) {
    return (word1 >> CS_SHIFT) & CS_MASK;
  }

  /** Decode flags from a CTE-marked word 1. */
  static inline Uint32 decodeFlags(Uint32 word1) {
    return word1 & FLAGS_MASK;
  }
}

#endif  // CTE_LINKED_ATTR_HPP
