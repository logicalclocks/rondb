/*
   Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#ifndef NDB_GET_COLLATION_INFO_HPP
#define NDB_GET_COLLATION_INFO_HPP

#include <ndb_types.h>

/**
 * GET_COLLATION_INFO_REQ / CONF / REF — any node, normally an API node,
 * to DBDICT on a data node.
 *
 * Asks which character set and collation the data node has under a
 * collation id (the number NDB keeps in the upper half of a column's
 * AttributeExtPrecision), under a collation name, or under a character set
 * name, which names that character set's default collation.  Names are
 * matched without regard to case.  The answer describes the data node's
 * own copy of the collation library, so an API node can check that a
 * collation it uses, in a table or inline in a pushed-down program, exists
 * on the data node and agrees with its own.  Looking a collation up
 * initialises it on the data node, as creating a table that uses it does.
 *
 * With WithWeights the CONF also carries the collation's weights: the
 * weight string of every character of the character set and of every
 * contraction, in section WEIGHTS.  A CONF is one fragmented signal of at
 * most MaxWeightsResponseBytes, so a large collation takes several
 * requests: the first asks from weightsPosition 0, each CONF says in
 * nextWeightsPosition where the next request continues, until it is
 * AllWeightsSent.  Positions below
 * ContractionsPosition are code points, ContractionsPosition + n is the
 * nth contraction.  DBDICT builds each such CONF in slices before sending
 * it; it serves one WithWeights request at a time and refuses another
 * meanwhile with Busy.
 *
 * DBDICT answers with GET_COLLATION_INFO_CONF, or refuses with
 * GET_COLLATION_INFO_REF when the request is malformed or names no
 * collation known there.  The reply goes to senderRef, which must be on
 * the sending node; when it is not, or the signal is too short to carry
 * it, the REF goes to the sending block instead.
 *
 * Version gated: send the request only to data nodes with
 * ndbd_support_get_collation_info(version).  An older data node takes the
 * unknown signal number as a fatal error.
 */
struct GetCollationInfoReq {
  Uint32 senderRef;
  Uint32 senderData;
  Uint32 requestType;      // RequestType
  Uint32 collationId;      // ById: the collation id; ignored otherwise
  Uint32 nameLength;       // ByCollationName, ByCharsetName: bytes of the
                           // name in section NAME, without any terminating
                           // NUL; ignored otherwise
  Uint32 requestFlags;     // RequestFlags
  Uint32 weightsPosition;  // WithWeights: where to start, 0 at first,
                           // then the nextWeightsPosition of the last CONF

  static constexpr Uint32 SignalLength = 7;

  enum RequestType {
    ById = 0,             // no sections
    ByCollationName = 1,  // one section, NAME
    ByCharsetName = 2     // one section, NAME; the default collation
  };

  enum RequestFlags {
    WithWeights = 0x1  // also send weights, in section WEIGHTS
  };

  /* weightsPosition: code points come first, then the contractions */
  static constexpr Uint32 ContractionsPosition = 0x110000;
  static constexpr Uint32 AllWeightsSent = 0xFFFFFFFF;

  /**
   * The most bytes of sections in one WithWeights CONF.  A data node
   * built with VM_TRACE sends at most 4096, so that a collation takes
   * many CONFs there.
   */
  static constexpr Uint32 MaxWeightsResponseBytes = 1024 * 1024;

  /**
   * Section NAME holds nameLength bytes of the name, at most
   * MaxNameLength, padded to whole words (a NUL after the name is
   * allowed but not needed).
   */
  static constexpr Uint32 NAME = 0;
  static constexpr Uint32 MaxNameLength = 64;
  static constexpr Uint32 MaxNameWords = (MaxNameLength + 1 + 3) / 4;
};

/**
 * Section WEIGHTS (WithWeights) holds weightsLength bytes of weight
 * entries, padded with zeros to a whole word, then contractionsLength
 * bytes of contraction entries, padded likewise.  The CONF has the
 * section only when it holds an entry.  Every multi-byte number in it is
 * big-endian.
 *
 * A weight entry covers count consecutive code points first .. first +
 * count - 1:
 *
 *   3 bytes  first code point
 *   3 bytes  count, at least 1
 *   1 byte   stepOffset
 *   1 byte   encodedLength
 *   2 bytes  weightLength
 *   encodedLength bytes  the first code point in the character set
 *   weightLength bytes   the weight string of the first code point
 *
 * The characters of the character set are the Unicode code points it
 * encodes and decodes back to the same code point; each such code point
 * is in exactly one entry, the entries in increasing code point order.
 * The weight string of a character is what the collation's strnxfrm gives
 * for a string of that one character, without padding.  How it combines
 * into the weight string of a longer string depends on the collation: a
 * UCA 9.0.0 collation lists the weights of each level in turn, separated
 * by 0x0000, and contractions take the place of their characters; a few
 * collations (czech, win1250ch, tis620) order characters by their
 * neighbours as well.
 *
 * In an entry with count > 1, code point first + k has:
 * - the weight string of first, but with the 16-bit number at byte
 *   stepOffset of it increased by k, or the very same weight string when
 *   stepOffset is NoStep, and
 * - encodedLength bytes holding the encoded first code point as a
 *   big-endian number increased by k.
 * encodedLength is 0 for a Unicode character set (Flags::Unicode), whose
 * encoding is the standard one, utf8, utf16, utf16le, utf32 or ucs2.
 */
struct GetCollationInfoWeight {
  static constexpr Uint32 HeaderLength = 10;
  static constexpr Uint32 NoStep = 0xFF;
  static constexpr Uint32 MaxCount = 0xFFFFFF;
};

/**
 * A contraction entry, for a UCA collation with contractions:
 *
 *   1 byte   number of code points, 2 to MaxCodePoints
 *   1 byte   flags, PreviousContext
 *   2 bytes  weightLength
 *   3 bytes per code point, in string order
 *   weightLength bytes  the weight string of the whole sequence
 *
 * A contraction is a sequence of code points the collation weighs as one
 * unit, such as "ch" in Czech.  PreviousContext marks a pair whose second
 * code point is weighed by the first, such as the Japanese prolonged
 * sound mark; the first keeps its own weights.  A contraction holding a
 * code point the character set does not encode has no entry, but still
 * has its position.
 */
struct GetCollationInfoContraction {
  static constexpr Uint32 HeaderLength = 4;
  static constexpr Uint32 MaxCodePoints = 6;

  enum Flags {
    PreviousContext = 0x1
  };
};

struct GetCollationInfoConf {
  Uint32 senderData;
  Uint32 collationId;          // the collation found
  Uint32 primaryCollationId;   // default collation of its character set
  Uint32 binaryCollationId;    // binary collation of its character set,
                               // 0 if it has none
  Uint32 flags;                // Flags
  Uint32 mbMinLen;             // fewest bytes in a character
  Uint32 mbMaxLen;             // most bytes in a character
  Uint32 strxfrmMultiply;      // most sort key bytes per string byte
  Uint32 caseMultiply;         // see getCaseUpMultiply/getCaseDownMultiply
  Uint32 padChar;              // the byte a CHAR value is padded with
  Uint32 levelsForCompare;     // weight levels compared (UCA 9.0.0)
  Uint32 minSortChar;          // lowest-sorting code point (LIKE ranges)
  Uint32 maxSortChar;          // highest-sorting code point (LIKE ranges)
  Uint32 nameLengths;          // see getCharsetNameLength and
                               // getCollationNameLength
  Uint32 weightEntries;        // weight entries in WEIGHTS
  Uint32 weightsLength;        // bytes of weight entries in WEIGHTS
  Uint32 contractionEntries;   // contraction entries in WEIGHTS
  Uint32 contractionsLength;   // bytes of contraction entries in WEIGHTS
  Uint32 nextWeightsPosition;  // weightsPosition of the next request, or
                               // AllWeightsSent; AllWeightsSent without
                               // WithWeights

  /**
   * A fragment of a WithWeights CONF carrying pieces of both sections
   * holds this, 2 section numbers and the fragment id, and a signal's
   * length and section count make at most 25 words: so at most 20.
   */
  static constexpr Uint32 SignalLength = 19;

  /* Most bytes upper-casing, and lower-casing, makes of a byte */
  Uint32 getCaseUpMultiply() const { return caseMultiply & 0xFF; }
  Uint32 getCaseDownMultiply() const { return (caseMultiply >> 8) & 0xFF; }
  void setCaseMultiply(Uint32 up, Uint32 down) {
    caseMultiply = (up & 0xFF) | ((down & 0xFF) << 8);
  }

  /* Bytes of the character set name, and of the collation name, in NAMES */
  Uint32 getCharsetNameLength() const { return nameLengths & 0xFFFF; }
  Uint32 getCollationNameLength() const { return nameLengths >> 16; }
  void setNameLengths(Uint32 charsetNameLength, Uint32 collationNameLength) {
    nameLengths = (charsetNameLength & 0xFFFF) | (collationNameLength << 16);
  }

  enum Flags {
    Primary = 0x1,        // the default collation of its character set
    Binary = 0x2,         // binary sort order
    Unicode = 0x4,        // the character set is Unicode
    CaseSensitive = 0x8,  // case-sensitive sort order
    PureAscii = 0x10,     // the character set is pure ASCII
    NoPad = 0x20          // NO PAD: trailing spaces count in comparisons
  };

  /**
   * Section NAMES holds the character set name, a NUL, the collation
   * name and a NUL, padded to whole words with NULs.  Section WEIGHTS is
   * described above.
   */
  static constexpr Uint32 NAMES = 0;
  static constexpr Uint32 WEIGHTS = 1;
};

struct GetCollationInfoRef {
  Uint32 senderData;   // 0 if the request was too short to carry it
  Uint32 errorCode;    // ErrorCode
  Uint32 requestType;  // as in the request, 0 if too short to carry it
  Uint32 collationId;  // as in the request, 0 if too short to carry it

  static constexpr Uint32 SignalLength = 4;

  enum ErrorCode {
    InvalidSignal = 1,       // too short, or the sections do not fit the
                             // requestType
    InvalidSenderRef = 2,    // senderRef on another node than the sender
    InvalidRequestType = 3,  // not a RequestType, or unknown requestFlags
    InvalidCollationId = 4,  // ById: 0, or not below 2048
                             // (MY_ALL_CHARSETS_SIZE)
    InvalidName = 5,         // empty, longer than MaxNameLength or than its
                             // section, or holding a NUL
    UnknownCollation = 6,    // no such collation on this data node
    UnknownCharset = 7,      // ByCharsetName: no such character set on
                             // this data node
    InvalidWeightsRequest = 8,  // WithWeights: weightsPosition is
                                // AllWeightsSent
    Busy = 9,                // WithWeights: another such request is being
                             // served, try again
    OutOfMemory = 10         // WithWeights: no memory to build the CONF in
  };
};

#endif  // NDB_GET_COLLATION_INFO_HPP
