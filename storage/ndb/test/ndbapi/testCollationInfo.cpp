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

/**
 * GET_COLLATION_INFO_REQ / CONF / REF (signaldata/GetCollationInfo.hpp).
 *
 * Asks DBDICT on every data node for every collation id, and for every
 * collation and character set by name, and checks each answer against
 * this program's own collation library, which is the data nodes' library
 * in a cluster of one build.  Fetches the weights of a set of collations
 * of every kind, over as many CONFs as they take, and checks the weight
 * string of every character and contraction the same way.  Then sends
 * malformed requests and checks that DBDICT refuses each with the
 * expected error code, and stays up.
 */

#include <GlobalSignalNumbers.h>
#include <my_sys.h>
#include <ndb_version.h>
#include <NDBT.hpp>
#include <NDBT_Test.hpp>
#include <NdbRestarter.hpp>
#include <cstring>
#include <signaldata/GetCollationInfo.hpp>
#include <vector>
#include "../../src/ndbapi/SignalSender.hpp"
#include "mysql/strings/m_ctype.h"
#include "strings/str_uca_type.h"

namespace {

/* A GET_COLLATION_INFO_CONF or _REF, its fragments put together */
struct CollationInfoReply {
  Uint32 gsn{0};  // GSN_GET_COLLATION_INFO_CONF or _REF, 0 if none came
  Uint32 length{0};
  Uint32 data[25]{};
  std::vector<Uint32> sections[3];

  const GetCollationInfoConf *conf() const {
    return (const GetCollationInfoConf *)data;
  }
  const GetCollationInfoRef *ref() const {
    return (const GetCollationInfoRef *)data;
  }
};

Uint32 readBigEndian(const Uint8 *p, Uint32 bytes) {
  Uint32 value = 0;
  for (Uint32 i = 0; i < bytes; i++) value = (value << 8) | p[i];
  return value;
}

/**
 * The character of the character set at codePoint, encoded, as the data
 * node finds them: a code point it encodes and decodes back.
 */
bool localCharacter(const CHARSET_INFO *cs, Uint32 codePoint, Uint8 *encoded,
                    Uint32 *encodedLength) {
  const int length = cs->cset->wc_mb(cs, codePoint, encoded, encoded + 8);
  if (length <= 0) return false;
  my_wc_t decoded = 0;
  if (cs->cset->mb_wc(cs, &decoded, encoded, encoded + length) != length ||
      decoded != codePoint) {
    return false;
  }
  *encodedLength = Uint32(length);
  return true;
}

/* Whether the character set encodes every code point of a contraction */
bool encodable(const CHARSET_INFO *cs, const my_wc_t *codePoints,
               Uint32 count) {
  for (Uint32 i = 0; i < count; i++) {
    Uint8 encoded[8];
    if (cs->cset->wc_mb(cs, codePoints[i], encoded, encoded + 8) <= 0) {
      return false;
    }
  }
  return true;
}

/* The contractions of a collation that its character set encodes */
Uint32 countLocalContractions(const CHARSET_INFO *cs,
                              const std::vector<MY_CONTRACTION> &nodes,
                              my_wc_t *path, Uint32 depth) {
  Uint32 count = 0;
  for (const MY_CONTRACTION &node : nodes) {
    path[depth] = node.ch;
    if (depth > 0 && node.is_contraction_tail &&
        encodable(cs, path, depth + 1)) {
      count++;
    }
    if (depth + 1 < GetCollationInfoContraction::MaxCodePoints) {
      count += countLocalContractions(cs, node.child_nodes, path, depth + 1);
    }
    if (depth == 0) {
      for (const MY_CONTRACTION &previous : node.child_nodes_context) {
        const my_wc_t pair[2] = {previous.ch, node.ch};
        if (previous.is_contraction_tail && encodable(cs, pair, 2)) count++;
      }
    }
  }
  return count;
}

class CollationInfoChecker {
 public:
  explicit CollationInfoChecker(SignalSender *ss) : m_ss(ss) {}

  int runTest(Uint32 nodeId, bool checkWeights) {
    m_nodeId = nodeId;
    if (!checkAllCollations() || !checkRefusals()) return NDBT_FAILED;
    if (checkWeights && !checkAllWeights()) return NDBT_FAILED;
    return NDBT_OK;
  }

 private:
  SignalSender *const m_ss;
  Uint32 m_nodeId{0};
  Uint32 m_senderData{0};

  GetCollationInfoReq makeReq(Uint32 requestType, Uint32 collationId,
                              Uint32 nameLength) {
    GetCollationInfoReq req;
    memset(&req, 0, sizeof(req));
    req.senderRef = m_ss->getOwnRef();
    req.senderData = ++m_senderData;
    req.requestType = requestType;
    req.collationId = collationId;
    req.nameLength = nameLength;
    return req;
  }

  GetCollationInfoReq makeWeightsReq(Uint32 collationId, Uint32 position) {
    GetCollationInfoReq req = makeReq(GetCollationInfoReq::ById, collationId, 0);
    req.requestFlags = GetCollationInfoReq::WithWeights;
    req.weightsPosition = position;
    return req;
  }

  /**
   * Send the first length words of req to DBDICT, with section NAME when
   * nameWordCount != 0.
   */
  bool sendReq(const GetCollationInfoReq &req, Uint32 length,
               const Uint32 *nameWords = nullptr, Uint32 nameWordCount = 0) {
    SimpleSignal request(false);
    memcpy(request.getDataPtrSend(), &req, sizeof(req));
    if (nameWordCount != 0) {
      request.ptr[0].p = const_cast<Uint32 *>(nameWords);
      request.ptr[0].sz = nameWordCount;
      request.header.m_noOfSections = 1;
    }
    m_ss->lock();
    const SendStatus status = m_ss->sendSignal(
        m_nodeId, request, DBDICT, GSN_GET_COLLATION_INFO_REQ, length);
    m_ss->unlock();
    if (status != SEND_OK) {
      g_err << "Node " << m_nodeId << ": failed to send GET_COLLATION_INFO_REQ"
            << endl;
      return false;
    }
    return true;
  }

  /**
   * Wait for the CONF or REF from the node that carries senderData,
   * putting the fragments of a fragmented CONF together.  Every fragment
   * carries the signal body; after it come the numbers of the sections
   * the fragment carries a part of, and the fragment id.
   */
  CollationInfoReply waitReply(Uint32 senderData) {
    CollationInfoReply reply;
    Uint32 fragmentId = 0;
    m_ss->lock();
    while (true) {
      const SimpleSignal *signal = m_ss->waitFor(30000);
      if (signal == nullptr) {
        g_err << "Node " << m_nodeId
              << ": no reply to GET_COLLATION_INFO_REQ, senderData "
              << senderData << endl;
        reply.gsn = 0;
        break;
      }
      const Uint32 gsn = signal->readSignalNumber();
      const Uint32 *data = signal->getDataPtr();
      const Uint32 length = signal->getLength();
      if ((gsn != GSN_GET_COLLATION_INFO_CONF &&
           gsn != GSN_GET_COLLATION_INFO_REF) ||
          refToNode(signal->header.theSendersBlockRef) != m_nodeId ||
          length == 0 || data[0] != senderData) {
        continue;  // not the reply to this request
      }
      const Uint32 fragmentInfo = signal->header.m_fragmentInfo;
      const Uint32 sections = signal->header.m_noOfSections;
      Uint32 bodyLength = length;
      if (fragmentInfo != 0) {
        require(length >= sections + 2);
        bodyLength = length - sections - 1;
        const Uint32 id = data[length - 1];
        if (fragmentId == 0) {
          fragmentId = id;
        } else if (id != fragmentId) {
          g_err << "Node " << m_nodeId << ": fragment of train " << id
                << " in train " << fragmentId << endl;
          reply.gsn = 0;
          break;
        }
      }
      for (Uint32 i = 0; i < sections; i++) {
        const Uint32 sectionNo = (fragmentInfo != 0) ? data[bodyLength + i] : i;
        require(sectionNo < 3);
        reply.sections[sectionNo].insert(reply.sections[sectionNo].end(),
                                         signal->ptr[i].p,
                                         signal->ptr[i].p + signal->ptr[i].sz);
      }
      if (fragmentInfo == 0 || fragmentInfo == 3) {
        reply.gsn = gsn;
        reply.length = bodyLength;
        memcpy(reply.data, data, 4 * bodyLength);
        break;
      }
    }
    m_ss->unlock();
    return reply;
  }

  CollationInfoReply send(const GetCollationInfoReq &req, Uint32 length,
                          const Uint32 *nameWords = nullptr,
                          Uint32 nameWordCount = 0) {
    if (!sendReq(req, length, nameWords, nameWordCount)) {
      return CollationInfoReply();
    }
    return waitReply(req.senderData);
  }

  /* Send a by-name request, the name in as few words as it fits in */
  CollationInfoReply sendName(Uint32 requestType, const char *name) {
    const Uint32 nameLength = Uint32(strlen(name));
    Uint32 nameWords[GetCollationInfoReq::MaxNameWords + 1] = {0};
    require(nameLength <= sizeof(nameWords));
    memcpy(nameWords, name, nameLength);
    return send(makeReq(requestType, 0, nameLength),
                GetCollationInfoReq::SignalLength, nameWords,
                (nameLength + 3) / 4);
  }

  bool expectRef(const CollationInfoReply &reply, Uint32 errorCode,
                 const char *what) {
    if (reply.gsn != GSN_GET_COLLATION_INFO_REF ||
        reply.length < GetCollationInfoRef::SignalLength) {
      g_err << "Node " << m_nodeId << ", " << what
            << ": expected GET_COLLATION_INFO_REF with error " << errorCode
            << ", got GSN " << reply.gsn << endl;
      return false;
    }
    if (reply.ref()->errorCode != errorCode) {
      g_err << "Node " << m_nodeId << ", " << what << ": expected error "
            << errorCode << ", got " << reply.ref()->errorCode << endl;
      return false;
    }
    return true;
  }

  /**
   * Expect a CONF describing cs as this program's library does.  Without
   * WithWeights it carries no weights; with it, checkWeightsConf looks at
   * them.
   */
  bool expectConf(const CollationInfoReply &reply, const CHARSET_INFO *cs,
                  const char *what, bool withWeights = false) {
    if (reply.gsn != GSN_GET_COLLATION_INFO_CONF ||
        reply.length < GetCollationInfoConf::SignalLength) {
      g_err << "Node " << m_nodeId << ", " << what << ": expected "
            << "GET_COLLATION_INFO_CONF for " << cs->m_coll_name << " ("
            << cs->number << "), got GSN " << reply.gsn;
      if (reply.gsn == GSN_GET_COLLATION_INFO_REF) {
        g_err << " with error " << reply.ref()->errorCode;
      }
      g_err << endl;
      return false;
    }
    const GetCollationInfoConf *conf = reply.conf();

    Uint32 flags = 0;
    if (cs->state & MY_CS_PRIMARY) flags |= GetCollationInfoConf::Primary;
    if (cs->state & MY_CS_BINSORT) flags |= GetCollationInfoConf::Binary;
    if (cs->state & MY_CS_UNICODE) flags |= GetCollationInfoConf::Unicode;
    if (cs->state & MY_CS_CSSORT) flags |= GetCollationInfoConf::CaseSensitive;
    if (cs->state & MY_CS_PUREASCII) flags |= GetCollationInfoConf::PureAscii;
    if (cs->pad_attribute == NO_PAD) flags |= GetCollationInfoConf::NoPad;

    const Uint32 charsetNameLength = Uint32(strlen(cs->csname));
    const Uint32 collationNameLength = Uint32(strlen(cs->m_coll_name));
    const Uint32 namesBytes = charsetNameLength + 1 + collationNameLength + 1;
    const std::vector<Uint32> &namesSection =
        reply.sections[GetCollationInfoConf::NAMES];
    const char *names = (const char *)namesSection.data();

    const bool same =
        conf->collationId == cs->number &&
        conf->primaryCollationId ==
            get_charset_number(cs->csname, MY_CS_PRIMARY) &&
        conf->binaryCollationId ==
            get_charset_number(cs->csname, MY_CS_BINSORT) &&
        conf->flags == flags && conf->mbMinLen == cs->mbminlen &&
        conf->mbMaxLen == cs->mbmaxlen &&
        conf->strxfrmMultiply == cs->strxfrm_multiply &&
        conf->getCaseUpMultiply() == cs->caseup_multiply &&
        conf->getCaseDownMultiply() == cs->casedn_multiply &&
        conf->padChar == cs->pad_char &&
        conf->levelsForCompare == cs->levels_for_compare &&
        conf->minSortChar == Uint32(cs->min_sort_char) &&
        conf->maxSortChar == Uint32(cs->max_sort_char) &&
        conf->getCharsetNameLength() == charsetNameLength &&
        conf->getCollationNameLength() == collationNameLength &&
        namesSection.size() == (namesBytes + 3) / 4 &&
        memcmp(names, cs->csname, charsetNameLength + 1) == 0 &&
        memcmp(names + charsetNameLength + 1, cs->m_coll_name,
               collationNameLength + 1) == 0;
    if (!same) {
      g_err << "Node " << m_nodeId << ", " << what << ": "
            << cs->m_coll_name << " (" << cs->number
            << ") differs, data node has collation " << conf->collationId
            << " primary " << conf->primaryCollationId << " binary "
            << conf->binaryCollationId << " flags " << hex << conf->flags
            << dec << " mbminlen " << conf->mbMinLen << " mbmaxlen "
            << conf->mbMaxLen << " strxfrm_multiply " << conf->strxfrmMultiply
            << " name lengths " << conf->getCharsetNameLength() << "/"
            << conf->getCollationNameLength() << " in " << namesSection.size()
            << " words" << endl;
      return false;
    }
    if (!withWeights &&
        (conf->weightEntries != 0 || conf->weightsLength != 0 ||
         conf->contractionEntries != 0 || conf->contractionsLength != 0 ||
         conf->nextWeightsPosition != GetCollationInfoReq::AllWeightsSent ||
         !reply.sections[GetCollationInfoConf::WEIGHTS].empty())) {
      g_err << "Node " << m_nodeId << ", " << what
            << ": weights without WithWeights" << endl;
      return false;
    }
    return true;
  }

  bool checkAllCollations() {
    Uint32 collations = 0;
    for (Uint32 id = 1; id < MY_ALL_CHARSETS_SIZE; id++) {
      const CHARSET_INFO *cs = get_charset(id, MYF(0));
      const CollationInfoReply reply =
          send(makeReq(GetCollationInfoReq::ById, id, 0),
               GetCollationInfoReq::SignalLength);
      if (cs == nullptr) {
        if (!expectRef(reply, GetCollationInfoRef::UnknownCollation,
                       "unknown collation id")) {
          g_err << "  (collation id " << id << ")" << endl;
          return false;
        }
        continue;
      }
      collations++;
      if (!expectConf(reply, cs, "by id") ||
          !expectConf(
              sendName(GetCollationInfoReq::ByCollationName, cs->m_coll_name),
              cs, "by collation name")) {
        return false;
      }
      const CHARSET_INFO *primary =
          get_charset_by_csname(cs->csname, MY_CS_PRIMARY, MYF(0));
      if (primary == nullptr) {
        g_err << "Character set " << cs->csname
              << " has no default collation here" << endl;
        return false;
      }
      if (!expectConf(sendName(GetCollationInfoReq::ByCharsetName, cs->csname),
                      primary, "by character set name")) {
        return false;
      }
    }
    if (collations == 0) {
      g_err << "No collations known to this program" << endl;
      return false;
    }
    g_info << "Node " << m_nodeId << " agrees on all " << collations
           << " collations" << endl;
    return true;
  }

  /**
   * Check the weight entries of one CONF against this program's
   * library.  codePoint is the first code point the CONF can cover; on
   * return the first one the next CONF covers.
   */
  bool checkWeightEntries(const CHARSET_INFO *cs, const Uint8 *entries,
                          Uint32 length, Uint32 expectedEntries,
                          Uint32 &codePoint) {
    const bool unicode = (cs->state & MY_CS_UNICODE) != 0;
    Uint32 offset = 0;
    Uint32 entryCount = 0;
    while (offset < length) {
      if (offset + GetCollationInfoWeight::HeaderLength > length) {
        g_err << "Weight entry header past the end" << endl;
        return false;
      }
      const Uint8 *entry = entries + offset;
      const Uint32 first = readBigEndian(entry, 3);
      const Uint32 count = readBigEndian(entry + 3, 3);
      const Uint32 stepOffset = entry[6];
      const Uint32 encodedLength = entry[7];
      const Uint32 weightLength = readBigEndian(entry + 8, 2);
      const Uint8 *encoded = entry + GetCollationInfoWeight::HeaderLength;
      const Uint8 *weight = encoded + encodedLength;
      offset += GetCollationInfoWeight::HeaderLength + encodedLength +
                weightLength;
      entryCount++;
      if (offset > length || count == 0 || first < codePoint ||
          first + count > GetCollationInfoReq::ContractionsPosition ||
          (unicode && encodedLength != 0) ||
          (stepOffset != GetCollationInfoWeight::NoStep &&
           (count == 1 || stepOffset + 2 > weightLength))) {
        g_err << "Bad weight entry at U+" << hex << first << dec << " count "
              << count << " step " << stepOffset << " lengths "
              << encodedLength << "/" << weightLength << endl;
        return false;
      }
      /* The code points skipped are not characters of the character set */
      Uint8 localEncoded[8];
      Uint32 localEncodedLength = 0;
      for (; codePoint < first; codePoint++) {
        if (localCharacter(cs, codePoint, localEncoded, &localEncodedLength)) {
          g_err << cs->m_coll_name << ": U+" << hex << codePoint << dec
                << " missing from the weights" << endl;
          return false;
        }
      }
      for (Uint32 k = 0; k < count; k++, codePoint++) {
        if (!localCharacter(cs, codePoint, localEncoded,
                            &localEncodedLength)) {
          g_err << cs->m_coll_name << ": U+" << hex << codePoint << dec
                << " in the weights, but not a character here" << endl;
          return false;
        }
        if (!unicode &&
            (localEncodedLength != encodedLength ||
             readBigEndian(localEncoded, localEncodedLength) !=
                 readBigEndian(encoded, encodedLength) + k)) {
          g_err << cs->m_coll_name << ": U+" << hex << codePoint << dec
                << " encoded differently" << endl;
          return false;
        }
        Uint8 expected[1024];
        require(weightLength <= sizeof(expected));
        memcpy(expected, weight, weightLength);
        if (k != 0 && stepOffset != GetCollationInfoWeight::NoStep) {
          const Uint32 value = readBigEndian(expected + stepOffset, 2) + k;
          if (value > 0xFFFF) {
            g_err << cs->m_coll_name << ": step past 0xFFFF at U+" << hex
                  << codePoint << dec << endl;
            return false;
          }
          expected[stepOffset] = Uint8(value >> 8);
          expected[stepOffset + 1] = Uint8(value);
        }
        Uint8 localWeight[1024];
        const size_t localWeightLength =
            cs->coll->strnxfrm(cs, localWeight, sizeof(localWeight), 1,
                               localEncoded, localEncodedLength, 0);
        if (localWeightLength != weightLength ||
            memcmp(localWeight, expected, weightLength) != 0) {
          g_err << cs->m_coll_name << ": weight of U+" << hex << codePoint
                << dec << " differs (" << weightLength << " bytes there, "
                << Uint32(localWeightLength) << " here)" << endl;
          return false;
        }
      }
    }
    if (offset != length || entryCount != expectedEntries) {
      g_err << cs->m_coll_name << ": " << entryCount << " weight entries in "
            << offset << " bytes, CONF says " << expectedEntries << " in "
            << length << endl;
      return false;
    }
    return true;
  }

  /* Check the contraction entries of one CONF, counting them */
  bool checkContractionEntries(const CHARSET_INFO *cs, const Uint8 *entries,
                               Uint32 length, Uint32 expectedEntries,
                               Uint32 &contractions) {
    Uint32 offset = 0;
    Uint32 entryCount = 0;
    while (offset < length) {
      const Uint8 *entry = entries + offset;
      const Uint32 count = entry[0];
      const Uint32 flags = entry[1];
      const Uint32 weightLength = readBigEndian(entry + 2, 2);
      offset +=
          GetCollationInfoContraction::HeaderLength + 3 * count + weightLength;
      entryCount++;
      if (offset > length || count < 2 ||
          count > GetCollationInfoContraction::MaxCodePoints ||
          (flags & ~Uint32(GetCollationInfoContraction::PreviousContext)) !=
              0 ||
          ((flags & GetCollationInfoContraction::PreviousContext) != 0 &&
           count != 2)) {
        g_err << cs->m_coll_name << ": bad contraction entry" << endl;
        return false;
      }
      Uint8 encoded[GetCollationInfoContraction::MaxCodePoints * 8];
      Uint32 encodedLength = 0;
      const Uint8 *codePoints = entry + GetCollationInfoContraction::HeaderLength;
      for (Uint32 i = 0; i < count; i++) {
        const int n =
            cs->cset->wc_mb(cs, readBigEndian(codePoints + 3 * i, 3),
                            encoded + encodedLength, encoded + sizeof(encoded));
        if (n <= 0) {
          g_err << cs->m_coll_name << ": contraction not encodable here"
                << endl;
          return false;
        }
        encodedLength += n;
      }
      Uint8 localWeight[1024];
      const size_t localWeightLength = cs->coll->strnxfrm(
          cs, localWeight, sizeof(localWeight), count, encoded, encodedLength,
          0);
      if (localWeightLength != weightLength ||
          memcmp(localWeight, codePoints + 3 * count, weightLength) != 0) {
        g_err << cs->m_coll_name << ": weight of a contraction from U+"
              << hex << readBigEndian(codePoints, 3) << dec << " differs"
              << endl;
        return false;
      }
    }
    if (offset != length || entryCount != expectedEntries) {
      g_err << cs->m_coll_name << ": " << entryCount
            << " contraction entries in " << offset << " bytes, CONF says "
            << expectedEntries << " in " << length << endl;
      return false;
    }
    contractions += entryCount;
    return true;
  }

  /**
   * Fetch all weights of a collation, in as many CONFs as the data node
   * sends them in (many when it is built with VM_TRACE), and check each.
   */
  bool checkWeights(const char *collationName) {
    const CHARSET_INFO *cs = get_charset_by_name(collationName, MYF(0));
    if (cs == nullptr) {
      g_info << collationName << " unknown to this program, skipped" << endl;
      return true;
    }
    Uint32 position = 0;
    Uint32 codePoint = 0;
    Uint32 contractions = 0;
    Uint32 confs = 0;
    Uint32 bytes = 0;
    while (position != GetCollationInfoReq::AllWeightsSent) {
      const CollationInfoReply reply =
          send(makeWeightsReq(cs->number, position),
               GetCollationInfoReq::SignalLength);
      if (!expectConf(reply, cs, collationName, true)) return false;
      const GetCollationInfoConf *conf = reply.conf();
      confs++;

      const std::vector<Uint32> &weightsSection =
          reply.sections[GetCollationInfoConf::WEIGHTS];
      const Uint32 weightsBytes = (conf->weightsLength + 3) & ~Uint32(3);
      const Uint32 contractionsBytes =
          (conf->contractionsLength + 3) & ~Uint32(3);
      const Uint32 limit = GetCollationInfoReq::MaxWeightsResponseBytes;
      const Uint32 sectionBytes =
          4 * Uint32(reply.sections[GetCollationInfoConf::NAMES].size() +
                     weightsSection.size());
      if (4 * weightsSection.size() != weightsBytes + contractionsBytes ||
          sectionBytes > limit) {
        g_err << collationName << ": WEIGHTS of " << weightsSection.size()
              << " words for " << conf->weightsLength << " + "
              << conf->contractionsLength << " bytes, sections "
              << sectionBytes << " bytes of at most " << limit << endl;
        return false;
      }
      bytes += sectionBytes;
      const Uint8 *entries = (const Uint8 *)weightsSection.data();

      if (position < GetCollationInfoReq::ContractionsPosition) {
        if (codePoint != position) {
          g_err << collationName << ": asked from U+" << hex << position
                << ", expected U+" << codePoint << dec << endl;
          return false;
        }
        if (!checkWeightEntries(cs, entries, conf->weightsLength,
                                conf->weightEntries, codePoint)) {
          return false;
        }
      } else if (conf->weightEntries != 0) {
        g_err << collationName << ": weight entries after the code points"
              << endl;
        return false;
      }
      if (!checkContractionEntries(cs, entries + weightsBytes,
                                   conf->contractionsLength,
                                   conf->contractionEntries, contractions)) {
        return false;
      }

      const Uint32 next = conf->nextWeightsPosition;
      if (next != GetCollationInfoReq::AllWeightsSent && next <= position) {
        g_err << collationName << ": next position " << next << " after "
              << position << endl;
        return false;
      }
      if (next < GetCollationInfoReq::ContractionsPosition) {
        if (conf->contractionEntries != 0) {
          g_err << collationName << ": contractions before the code points"
                << " end" << endl;
          return false;
        }
        /* The CONF ended with the entries, before the next code point */
        Uint8 encoded[8];
        Uint32 encodedLength = 0;
        for (; codePoint < next; codePoint++) {
          if (localCharacter(cs, codePoint, encoded, &encodedLength)) {
            g_err << cs->m_coll_name << ": U+" << hex << codePoint << dec
                  << " missing before the next position" << endl;
            return false;
          }
        }
      }
      position = next;
    }
    /* The code points after the last entry are not characters */
    Uint8 encoded[8];
    Uint32 encodedLength = 0;
    for (; codePoint < GetCollationInfoReq::ContractionsPosition; codePoint++) {
      if (localCharacter(cs, codePoint, encoded, &encodedLength)) {
        g_err << cs->m_coll_name << ": U+" << hex << codePoint << dec
              << " missing at the end" << endl;
        return false;
      }
    }
    Uint32 localContractions = 0;
    if (cs->uca != nullptr && cs->uca->have_contractions &&
        cs->uca->contraction_nodes != nullptr) {
      my_wc_t path[GetCollationInfoContraction::MaxCodePoints];
      localContractions =
          countLocalContractions(cs, *cs->uca->contraction_nodes, path, 0);
    }
    if (contractions != localContractions) {
      g_err << collationName << ": " << contractions
            << " contractions, expected " << localContractions << endl;
      return false;
    }
    g_info << "Node " << m_nodeId << ": " << collationName << " weights in "
           << confs << " CONF(s), " << bytes << " bytes, " << contractions
           << " contractions" << endl;
    return true;
  }

  bool checkAllWeights() {
    /* Collations of each kind of character set and collation handler */
    static const char *const collations[] = {
        "latin1_swedish_ci",   "latin1_german2_ci",  "latin2_czech_cs",
        "binary",              "ascii_general_ci",   "gbk_chinese_ci",
        "gb18030_chinese_ci",  "utf8mb3_general_ci", "utf8mb4_general_ci",
        "utf8mb4_bin",         "utf8mb4_unicode_ci", "utf16le_general_ci",
        "utf32_unicode_520_ci", "utf8mb4_0900_ai_ci", "utf8mb4_0900_as_cs",
        "utf8mb4_cs_0900_ai_ci", "utf8mb4_ja_0900_as_cs",
        "utf8mb4_zh_0900_as_cs"};
    for (const char *collation : collations) {
      if (!checkWeights(collation)) return false;
    }

    /**
     * One WithWeights request at a time, another is refused meanwhile.
     * latin1 has no character from U+3000 on, so the first request scans
     * all of the rest of Unicode in several slices, with nothing to send,
     * and the second arrives during that.
     */
    const CHARSET_INFO *latin1 =
        get_charset_by_name("latin1_swedish_ci", MYF(0));
    if (latin1 != nullptr) {
      const GetCollationInfoReq first = makeWeightsReq(latin1->number, 0x3000);
      const GetCollationInfoReq second = makeWeightsReq(latin1->number, 0);
      if (!sendReq(first, GetCollationInfoReq::SignalLength) ||
          !sendReq(second, GetCollationInfoReq::SignalLength) ||
          !expectRef(waitReply(second.senderData), GetCollationInfoRef::Busy,
                     "second WithWeights request") ||
          !expectConf(waitReply(first.senderData), latin1,
                      "first WithWeights request", true)) {
        return false;
      }
    }
    return true;
  }

  bool checkRefusals() {
    const Uint32 length = GetCollationInfoReq::SignalLength;
    const Uint32 maxNameLength = GetCollationInfoReq::MaxNameLength;
    const Uint32 maxNameWords = GetCollationInfoReq::MaxNameWords;
    const CHARSET_INFO *utf8mb4_bin = get_charset_by_name("utf8mb4_bin", MYF(0));
    const CHARSET_INFO *latin1 = get_charset_by_name("latin1_swedish_ci", MYF(0));
    if (utf8mb4_bin == nullptr || latin1 == nullptr) {
      g_err << "utf8mb4_bin or latin1_swedish_ci unknown to this program"
            << endl;
      return false;
    }
    const Uint32 id = utf8mb4_bin->number;
    /* "utf8mb4_bin": 11 bytes, a NUL, and NULs to the end */
    Uint32 nameWords[GetCollationInfoReq::MaxNameWords + 1];
    memset(nameWords, 0, sizeof(nameWords));
    memcpy(nameWords, "utf8mb4_bin", 11);

    /* The request itself */
    GetCollationInfoReq otherRef = makeReq(GetCollationInfoReq::ById, id, 0);
    otherRef.senderRef = numberToRef(DBDICT, m_nodeId);
    GetCollationInfoReq unknownFlag = makeReq(GetCollationInfoReq::ById, id, 0);
    unknownFlag.requestFlags = 0x2;
    if (!expectRef(send(makeReq(GetCollationInfoReq::ById, id, 0), 2),
                   GetCollationInfoRef::InvalidSignal, "two words") ||
        !expectRef(send(otherRef, length),
                   GetCollationInfoRef::InvalidSenderRef,
                   "senderRef on the data node") ||
        !expectRef(send(makeReq(3, id, 0), length),
                   GetCollationInfoRef::InvalidRequestType,
                   "request type 3") ||
        !expectRef(send(unknownFlag, length),
                   GetCollationInfoRef::InvalidRequestType,
                   "request flag 0x2")) {
      return false;
    }

    /* ById: ids that cannot be a collation, and a section it does not take */
    if (!expectRef(send(makeReq(GetCollationInfoReq::ById, 0, 0), length),
                   GetCollationInfoRef::InvalidCollationId, "id 0") ||
        !expectRef(send(makeReq(GetCollationInfoReq::ById,
                                MY_ALL_CHARSETS_SIZE, 0),
                        length),
                   GetCollationInfoRef::InvalidCollationId,
                   "id MY_ALL_CHARSETS_SIZE") ||
        !expectRef(
            send(makeReq(GetCollationInfoReq::ById, ~Uint32(0), 0), length),
            GetCollationInfoRef::InvalidCollationId, "id 0xffffffff") ||
        !expectRef(send(makeReq(GetCollationInfoReq::ById, id, 0), length,
                        nameWords, 3),
                   GetCollationInfoRef::InvalidSignal, "id with a section")) {
      return false;
    }

    /* By name: no section, or one longer than any name */
    if (!expectRef(send(makeReq(GetCollationInfoReq::ByCollationName, 0, 11),
                        length),
                   GetCollationInfoRef::InvalidSignal, "name without section") ||
        !expectRef(send(makeReq(GetCollationInfoReq::ByCollationName, 0, 11),
                        length, nameWords, maxNameWords + 1),
                   GetCollationInfoRef::InvalidSignal,
                   "name section too long")) {
      return false;
    }

    /**
     * nameLength 0, beyond MaxNameLength, beyond the section, and over
     * the NUL after "utf8mb4_bin"
     */
    if (!expectRef(send(makeReq(GetCollationInfoReq::ByCollationName, 0, 0),
                        length, nameWords, 3),
                   GetCollationInfoRef::InvalidName, "nameLength 0") ||
        !expectRef(send(makeReq(GetCollationInfoReq::ByCollationName, 0,
                                maxNameLength + 1),
                        length, nameWords, maxNameWords),
                   GetCollationInfoRef::InvalidName,
                   "nameLength MaxNameLength + 1") ||
        !expectRef(send(makeReq(GetCollationInfoReq::ByCollationName, 0, 11),
                        length, nameWords, 2),
                   GetCollationInfoRef::InvalidName,
                   "nameLength beyond the section") ||
        !expectRef(send(makeReq(GetCollationInfoReq::ByCharsetName, 0, 12),
                        length, nameWords, 3),
                   GetCollationInfoRef::InvalidName, "NUL in the name")) {
      return false;
    }

    /* Well-formed names of nothing */
    if (!expectRef(sendName(GetCollationInfoReq::ByCollationName,
                            "no_such_collation"),
                   GetCollationInfoRef::UnknownCollation,
                   "unknown collation name") ||
        !expectRef(sendName(GetCollationInfoReq::ByCharsetName,
                            "no_such_charset"),
                   GetCollationInfoRef::UnknownCharset,
                   "unknown character set name") ||
        !expectRef(sendName(GetCollationInfoReq::ByCharsetName, "utf8mb4_bin"),
                   GetCollationInfoRef::UnknownCharset,
                   "collation name as character set name")) {
      return false;
    }

    /* WithWeights: a position it does not take */
    if (!expectRef(
            send(makeWeightsReq(id, GetCollationInfoReq::AllWeightsSent),
                 length),
            GetCollationInfoRef::InvalidWeightsRequest,
            "position AllWeightsSent")) {
      return false;
    }

    /**
     * After all that, DBDICT still answers.  A NUL after the name in the
     * section is allowed, and names match without regard to case.  Asking
     * for weights past the contractions gives none.
     */
    const CollationInfoReply pastEnd = send(
        makeWeightsReq(latin1->number,
                       GetCollationInfoReq::ContractionsPosition + 1000),
        length);
    if (!expectConf(send(makeReq(GetCollationInfoReq::ByCollationName, 0, 11),
                         length, nameWords, 3),
                    utf8mb4_bin, "name with a NUL after it") ||
        !expectConf(
            sendName(GetCollationInfoReq::ByCollationName, "UTF8MB4_BIN"),
            utf8mb4_bin, "upper case collation name") ||
        !expectConf(pastEnd, latin1, "weights past the contractions", true)) {
      return false;
    }
    if (pastEnd.conf()->weightEntries != 0 ||
        pastEnd.conf()->contractionEntries != 0 ||
        pastEnd.conf()->nextWeightsPosition !=
            GetCollationInfoReq::AllWeightsSent ||
        !pastEnd.sections[GetCollationInfoConf::WEIGHTS].empty()) {
      g_err << "Node " << m_nodeId << ": weights past the contractions"
            << endl;
      return false;
    }
    g_info << "Node " << m_nodeId << " refuses malformed requests" << endl;
    return true;
  }
};

}  // namespace

static int runGetCollationInfo(NDBT_Context *ctx, NDBT_Step *step) {
  Ndb *pNdb = GETNDB(step);
  SignalSender ss(&pNdb->get_ndb_cluster_connection());
  CollationInfoChecker checker(&ss);
  NdbRestarter restarter;

  const int numDbNodes = restarter.getNumDbNodes();
  if (numDbNodes <= 0) {
    g_err << "No data nodes found" << endl;
    return NDBT_FAILED;
  }
  int nodesChecked = 0;
  for (int i = 0; i < numDbNodes; i++) {
    const Uint32 nodeId = restarter.getDbNodeId(i);
    ss.lock();
    const bool alive = ss.get_node_alive(nodeId);
    const Uint32 version = ss.getNodeInfo(nodeId).m_info.m_version;
    ss.unlock();
    if (!alive) {
      g_err << "Data node " << nodeId << " is not alive" << endl;
      return NDBT_FAILED;
    }
    if (!ndbd_support_get_collation_info(version)) {
      g_info << "Data node " << nodeId << " has version " << hex << version
             << dec << " without GET_COLLATION_INFO_REQ, skipped" << endl;
      continue;
    }
    /* Every node builds weights alike; checking one is enough */
    if (checker.runTest(nodeId, nodesChecked == 0) != NDBT_OK) {
      return NDBT_FAILED;
    }
    nodesChecked++;
  }
  return nodesChecked > 0 ? NDBT_OK : NDBT_SKIPPED;
}

NDBT_TESTSUITE(testCollationInfo);
TESTCASE("GetCollationInfo",
         "Ask every data node for its character sets and collations with "
         "GET_COLLATION_INFO_REQ, compare the answers and the weights of "
         "a set of collations with this program's collation library, and "
         "check that malformed requests are refused") {
  STEP(runGetCollationInfo);
}
NDBT_TESTSUITE_END(testCollationInfo)

int main(int argc, const char **argv) {
  ndb_init();
  NDBT_TESTSUITE_INSTANCE(testCollationInfo);
  testCollationInfo.setCreateTable(false);
  testCollationInfo.setRunAllTables(true);
  return testCollationInfo.execute(argc, argv);
}
