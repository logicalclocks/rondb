/*
   Copyright (c) 2011, 2025, Oracle and/or its affiliates.
   Copyright (c) 2021, 2026, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#include "NdbQueryOperation.hpp"
#include <ndb_global.h>
#include <NdbAggregator.hpp>
#include <NdbAggregationCommon.hpp>
#include <NdbDictionary.hpp>
#include <NdbIndexScanOperation.hpp>
#include "API.hpp"
#include "NdbInterpretedCode.hpp"
#include "NdbQueryBuilder.hpp"
#include "NdbQueryBuilderImpl.hpp"
#include "NdbDictionaryImpl.hpp"
#include "NdbQueryOperationImpl.hpp"
#include "util/require.h"

#include <signaldata/DbspjErr.hpp>
#include <signaldata/QueryTree.hpp>
#include <signaldata/ScanTab.hpp>
#include <signaldata/TcKeyRef.hpp>
#include <signaldata/TcKeyReq.hpp>
#include <signaldata/TransIdAI.hpp>

#include "AttributeHeader.hpp"

#include <Bitmask.hpp>

#if 0
#define DEBUG_CRASH() assert(false)
#else
#define DEBUG_CRASH()
#endif

/**
 * DEBUG_JOIN_AGG_TRACE: Enable hex dumps of QueryTree (AttrInfo section 1)
 * and combined Section 2 (bounds + aggReceiverId + aggProgram) as sent
 * from NDB API in SCAN_TABREQ.  Uncomment to activate.
 */
#ifdef VM_TRACE
//#define DEBUG_JOIN_AGG_TRACE 1
//#define DEBUG_CTE_API 1
#endif

#ifdef DEBUG_CTE_API
#define DEB_CTE_API(...) fprintf(stderr, "[CTE_API] " __VA_ARGS__)
#else
#define DEB_CTE_API(...) do {} while(0)
#endif

#ifdef DEBUG_JOIN_AGG_TRACE
static void dumpJoinAggHex(const char *label, const Uint32 *buf, Uint32 len) {
  fprintf(stderr, "=== NdbQuery %s (%u words) ===\n", label, len);
  for (Uint32 i = 0; i < len; i++) {
    fprintf(stderr, "  [%3u] 0x%08x", i, buf[i]);
    if ((i % 4) == 3 || i == len - 1) fprintf(stderr, "\n");
  }
}
#define DEB_JOIN_AGG(label, buf, len) dumpJoinAggHex(label, buf, len)
#else
#define DEB_JOIN_AGG(label, buf, len) do {} while(0)
#endif

/** To prevent compiler warnings about variables that are only used in asserts
 * (when building optimized version).
 */
#define UNUSED(x) ((void)(x))

// To force usage of SCAN_NEXTREQ even for small scans resultsets:
// - '0' is default (production) value
// - '4' is normally a good value for testing batch related problems
static const int enforcedBatchSize = 0;

// Use double buffered ResultSets, may later change
// to be more adaptive based on query type
static const bool useDoubleBuffers = true;

/** Set to true to trace incoming signals.*/
static const bool traceSignals = false;

// The tupleId's are limited to the lower 12 bits (a correlationId constraint)
// Thus, we can use the 4 remaining upper bits to define some special values:

// A 'void' index for a tuple in internal parent / child correlation structs.
static constexpr Uint16 tupleNotFound = 0xffff;

// We use the upper tupleId bit to flag a 'skip' of that tupleId
static constexpr Uint16 skipTupleFlag = 0x8000;

static bool isUnsignedAggMetaType(NdbDictionary::Column::Type type) {
  switch (type) {
    case NdbDictionary::Column::Tinyunsigned:
    case NdbDictionary::Column::Smallunsigned:
    case NdbDictionary::Column::Mediumunsigned:
    case NdbDictionary::Column::Unsigned:
    case NdbDictionary::Column::Bigunsigned:
    case NdbDictionary::Column::Decimalunsigned:
      return true;
    default:
      return false;
  }
}

static void appendJoinAggColumnMetaEntry(
    Uint32Buffer &dst,
    const NdbDictionary::Column *col,
    Uint32 gbProgramWord,
    Uint32 gbIndex,
    Uint32 tableId,
    Uint32 schemaVersion) {
  const Uint32 encodedColId = gbProgramWord >> 16;
  const bool isLinked = (encodedColId & AGG_LINKED_COL_FLAG) != 0;
  const Uint32 sourceId = encodedColId & ~AGG_LINKED_COL_FLAG;
  const NdbDictionary::Column::Type type = col->getType();
  Uint32 flags = JOIN_AGG_META_FLAG_GROUP_BY;
  if (isUnsignedAggMetaType(type)) {
    flags |= JOIN_AGG_META_FLAG_UNSIGNED;
  }
  if (col->getNullable()) {
    flags |= JOIN_AGG_META_FLAG_NULLABLE;
  }

  dst.append(isLinked ? JOIN_AGG_META_SOURCE_LINKED_COLUMN
                      : JOIN_AGG_META_SOURCE_LOCAL_COLUMN);
  dst.append(sourceId);
  dst.append(8 + gbIndex);
  dst.append(gbIndex);
  dst.append(isLinked ? RNIL : tableId);
  dst.append(isLinked ? 0 : schemaVersion);
  dst.append(col->getAttrId());
  dst.append((Uint32)type);
  dst.append(col->getSizeInBytes());
  dst.append(col->getCharset() != nullptr ? col->getCharsetNumber() : 0);
  dst.append((col->getPrecision() << 16) | (col->getScale() & 0xFFFF));
  dst.append(flags);
}

static bool buildJoinAggGbColumnMetaBlock(
    Uint32Buffer &block,
    const Uint32 *program,
    Uint32 programLen,
    const NdbDictionary::Column *const *gbColumns,
    Uint32 nGbColumns,
    Uint32 tableId,
    Uint32 schemaVersion) {
  if (program == nullptr || gbColumns == nullptr || nGbColumns == 0) {
    return false;
  }
  if (programLen < 8 + nGbColumns) {
    return false;
  }

  Uint32 entryCount = 0;
  block.append(JOIN_AGG_META_MARKER);
  block.append(JOIN_AGG_META_VERSION);
  const Uint32 entryCountPos = block.getSize();
  block.append(0);
  for (Uint32 i = 0; i < nGbColumns; i++) {
    const NdbDictionary::Column *col = gbColumns[i];
    if (col == nullptr) {
      continue;
    }
    appendJoinAggColumnMetaEntry(block, col, program[8 + i], i,
                                 tableId, schemaVersion);
    entryCount++;
  }
  if (entryCount == 0) {
    return false;
  }
  block.put(entryCountPos, entryCount);
  return true;
}

/* Various error codes that are not specific to NdbQuery. */
static const int Err_TupleNotFound = 626;
static const int Err_FalsePredicate = 899;
static const int Err_MemoryAlloc = 4000;
static const int Err_SendFailed = 4002;
static const int Err_FunctionNotImplemented = 4003;
static const int Err_UnknownColumn = 4004;
static const int Err_ReceiveTimedOut = 4008;
static const int Err_NodeFailCausedAbort = 4028;
static const int Err_ParameterError = 4118;
static const int Err_SimpleDirtyReadFailed = 4119;
static const int Err_WrongFieldLength = 4209;
static const int Err_ReadTooMuch = 4257;
static const int Err_InvalidRangeNo = 4286;
static const int Err_DifferentTabForKeyRecAndAttrRec = 4287;
static const int Err_KeyIsNULL = 4316;
static const int Err_FinaliseNotCalled = 4519;
static const int Err_InterpretedCodeWrongTab = 4524;
static const int Err_OutstandingResultsMismatch = 4117;

/**
 * Set NdbQueryOperationImpl::m_parallelism to this value to indicate that
 * scan parallelism should be adaptive.
 */
static constexpr const Uint32 Parallelism_adaptive = 0xffff0000;

/**
 * Set NdbQueryOperationImpl::m_parallelism to this value to indicate that
 * all fragments should be scanned in parallel.
 */
static constexpr const Uint32 Parallelism_max = 0xffff0001;

/**
 * A class for accessing the correlation data at the end of a tuple (for
 * scan queries). These data have the following layout:
 *
 * Word 0: AttributeHeader
 * Word 1, upper halfword: tuple id of parent tuple.
 * Word 1, lower halfword: tuple id of this tuple.
 * Word 2: Id of receiver for root operation (where the ancestor tuple of this
 *         tuple will go).
 *
 * Both tuple identifiers are unique within this batch of SPJ-worker results.
 * With these identifiers, it is possible to relate a tuple to its parent and
 * children. That way, results for child operations can be updated correctly
 * when the application iterates over the results of the root scan operation.
 */
class TupleCorrelation {
 public:
  static const Uint32 wordCount = 1;

  explicit TupleCorrelation()
      : m_correlation((tupleNotFound << 16) | tupleNotFound) {}

  /** Conversion to/from Uint32 to store/fetch from buffers */
  explicit TupleCorrelation(Uint32 val) : m_correlation(val) {}

  Uint32 toUint32() const { return m_correlation; }

  Uint16 getTupleId() const { return m_correlation & 0xffff; }

  Uint16 getParentTupleId() const { return m_correlation >> 16; }

 private:
  Uint32 m_correlation;
};  // class TupleCorrelation

class CorrelationData {
 public:
  static const Uint32 wordCount = 3;

  explicit CorrelationData(const Uint32 *tupleData, Uint32 tupleLength)
      : m_corrPart(tupleData + tupleLength - wordCount) {
    assert(tupleLength >= wordCount);
    assert(AttributeHeader(m_corrPart[0]).getAttributeId() ==
           AttributeHeader::CORR_FACTOR64);
    assert(AttributeHeader(m_corrPart[0]).getByteSize() == 2 * sizeof(Uint32));
    assert(getTupleCorrelation().getTupleId() < tupleNotFound);
    assert(getTupleCorrelation().getParentTupleId() < tupleNotFound);
  }

  Uint32 getRootReceiverId() const { return m_corrPart[2]; }

  const TupleCorrelation getTupleCorrelation() const {
    return TupleCorrelation(m_corrPart[1]);
  }

 private:
  const Uint32 *const m_corrPart;
};  // class CorrelationData

/**
 * The NdbWorker handles results produced by a request to a single SPJ instance.
 *
 * If 'MultiFragment' scan is requested, the NdbWorker handles root and
 * related child rows from all fragments specified in the MultiFragment scan
 * request.
 *
 * If a query has a scan operation as its root, then that scan will normally
 * read from several fragments of its target table. Each such root fragment
 * scan, along with any child lookup operations that are spawned from it,
 * runs independently, in the sense that:
 * - The API will know when it has received all data from a fragment for a
 *   given batch and all child operations spawned from it.
 * - When one fragment is complete (for a batch) the API will make these data
 *   available to the application, even if other fragments are not yet complete.
 * - The tuple identifiers that are used for matching children with parents are
 *   only guaranteed to be unique within one batch of SPJ-worker results.
 *   Tuples derived from different worker result sets must thus be kept apart.
 *
 * This class manages the state of one such read operation, from one particular
 * request to a SPJ block instance. If the root operation is a lookup,
 * then there will be only one instance of this class.
 */
class NdbWorker {
 public:
  /** Build hash map for mapping from root receiver id to NdbWorker
   * instance.*/
  static void buildReceiverIdMap(NdbWorker *workers, Uint32 noOfWorkers);

  /** Find NdbWorker instance corresponding to a given root receiver id.*/
  static NdbWorker *receiverIdLookup(NdbWorker *frags, Uint32 noOfWorkers,
                                     Uint32 receiverId);

  explicit NdbWorker();

  ~NdbWorker();

  /**
   * Initialize object.
   * @param query Enclosing query.
   * @param workerNo This object manages state for reading from the
   * workerNo'th worker result that the root operation accesses.
   */
  void init(NdbQueryImpl &query, Uint32 workerNo);

  static void clear(NdbWorker *frags, Uint32 noOfWorkers);

  Uint32 getWorkerNo() const { return m_workerNo; }
  Uint32 rootOpNo() const { return m_rootOpNo; }

  /**
   * Prepare for receiving another batch of results.
   */
  void prepareNextReceiveSet();

  bool hasRequestedMore() const;

  /**
   * Prepare for reading another batch of results.
   */
  void grabNextResultSet();  // Need mutex lock

  bool hasReceivedMore() const;  // Need mutex lock

  void setReceivedMore();  // Need mutex lock

  bool incrOutstandingResults(Int32 delta) {
    if (traceSignals) {
      ndbout << "incrOutstandingResults: " << m_outstandingResults
             << ", with: " << delta << endl;
    }
    m_outstandingResults += delta;
    if (unlikely(m_confReceived && m_outstandingResults < 0)) {
      // Data node sent more results than reported in SCAN_TABCONF.
      // Return error instead of continuing with corrupt state.
      ndbout << "ERROR: outstanding results mismatch: "
             << m_outstandingResults << " after delta=" << delta
             << ", confReceived=true" << endl;
      assert(false);
      return false;
    }
    return true;
  }

  void throwRemainingResults() {
    if (traceSignals) {
      ndbout << "throwRemainingResults: " << m_outstandingResults << endl;
    }
    m_outstandingResults = 0;
    m_confReceived = true;
    postFetchRelease();
  }

  void setConfReceived(Uint32 tcPtrI);

  /**
   * The worker will read from a number of fragments of a table.
   * This method checks if all results for the current batch has been
   * received from this worker. This includes both results for the root
   * operation and any child operations. Note that child operations may access
   * other fragments.
   *
   * @return True if current batch is complete for this worker.
   */
  bool isFragBatchComplete() const {
    assert(m_workerNo != voidWorkerNo);
    return m_confReceived && m_outstandingResults == 0;
  }

  /**
   * Get the result stream that handles results derived from this
   * SPJ-worker for a particular operation.
   * @param operationNo The id of the operation.
   * @return The result stream for this worker.
   */
  NdbResultStream &getResultStream(Uint32 operationNo) const;

  NdbResultStream &getResultStream(const NdbQueryOperationImpl &op) const {
    return getResultStream(op.getQueryOperationDef().getOpNo());
  }

  Uint32 getReceiverId() const;
  Uint32 getReceiverTcPtrI() const;

  /**
   * @return True if there are no more batches to be received for this worker.
   */
  bool finalBatchReceived() const;

  /**
   * @return True if there are no more results from this worker (for
   * the current batch).
   */
  bool isEmpty() const;

  /**
   * This method is used for marking which streams belonging to this
   * NdbWorker which has remaining batches for a sub scan
   * instantiated from the current batch of its parent operation.
   *
   * moreMask:   Set of streams which we may receive more result from
   *             in *next* batch.
   * activeMask: Set of streams currently not returned their last row.
   *             (Will return 'more' in next or later REQuests)
   */
  void setRemainingSubScans(Uint32 moreMask, Uint32 activeMask) {
    m_nextScans.assign(SpjTreeNodeMask::Size, &moreMask);
    m_activeScans.assign(SpjTreeNodeMask::Size, &activeMask);
  }

  /**
   * Each NdbResultStream may have a 'm_currentRow'. This row
   * also depends on the currentRow's of ancestors of this operation,
   * such that if any ancestor navigate to a new first- or next-row,
   * the m_currentRow of their 'dependants' is invalidated.
   */
  bool hasValidRow(const NdbResultStream *resultStream) const;
  void setValidRow(const NdbResultStream *resultStream);

  /** Release resources after last row has been returned */
  void postFetchRelease();

 private:
  /** No copying.*/
  NdbWorker(const NdbWorker &);
  NdbWorker &operator=(const NdbWorker &);

  static constexpr Uint32 voidWorkerNo = 0xffffffff;

  /** Enclosing query.*/
  NdbQueryImpl *m_query;

  /** Number of this worker result set as assigned by ::init().*/
  Uint32 m_workerNo;

  /** Root operation number (cached from query, 0 for non-CTE). */
  Uint32 m_rootOpNo;

  /** For processing results originating from worker (Array of).*/
  NdbResultStream *m_resultStreams;

  /**
   * Number of requested (pre-)fetches which has either not completed
   * from datanodes yet, or which are completed, but not consumed.
   * (Which implies they are also counted in m_availResultSets)
   */
  Uint32 m_pendingRequests;

  /**
   * Number of available 'm_pendingRequests' ( <= m_pendingRequests)
   * which has been completely received. Will be made available
   * for reading by calling ::grabNextResultSet()
   */
  Uint32 m_availResultSets;  // Need mutex

  /**
   * The number of outstanding TCKEYREF or TRANSID_AI messages to receive
   * for the worker. This includes both messages related to the
   * root operation and any descendant operation that was instantiated as
   * a consequence of tuples found by the root operation.
   * This number may temporarily be negative if e.g. TRANSID_AI arrives
   * before SCAN_TABCONF.
   */
  Int32 m_outstandingResults;

  /**
   * This is an array with one element for each fragment that the root
   * operation accesses (i.e. one for a lookup, all for a table scan).
   *
   * Each element is true iff a SCAN_TABCONF (for that fragment) or
   * TCKEYCONF message has been received
   */
  bool m_confReceived;

  /**
   * A bitmask of resultStreams where the m_currentRow refers a valid
   * row. A current row is invalidated when an ancestor which we depends on
   * fetches a new currentRow.
   */
  SpjTreeNodeMask m_validResultStreams;

  /**
   * A bitmask of operation id's which has been set up to receive more
   * ResultSets by prepareNextReceiveSet().
   */
  SpjTreeNodeMask m_preparedReceiveSet;

  /**
   * A bitmask of operation id's for which we will receive more
   * ResultSets in a NEXTREQ.
   * Note: This is the next set of op's to be prepared (before NEXTREQ)
   * Note: Due to protocol legacy, only the uppermost scan op's in the branch
   *       getting new rows are set - However, all descendants will also get
   *       new ResultSets.
   */
  SpjTreeNodeMask m_nextScans;

  /**
   * A bitmask of operation id's still being 'active' on the SPJ side.
   * These will sooner or later return 'm_nextScans', but not necessarily
   * in the next round. It follows from this that 'active' contains 'remaining'.
   */
  SpjTreeNodeMask m_activeScans;

  /**
   * Used for implementing a hash map from root receiver ids to a
   * NdbWorker instance. m_idMapHead is the index of the first
   * NdbWorker in the m_workerNo'th hash bucket.
   */
  int m_idMapHead;

  /**
   * Used for implementing a hash map from root receiver ids to a
   * NdbWorker instance. m_idMapNext is the index of the next
   * NdbWorker in the same hash bucket as this one.
   */
  int m_idMapNext;
};  // NdbWorker

/**
 * 'class NdbResultSet' is a helper for 'class NdbResultStream'.
 *  It manages the buffers which rows are received into and
 *  read from.
 */
class NdbResultSet {
  friend class NdbResultStream;

 public:
  explicit NdbResultSet();

  void init(NdbQueryImpl &query, Uint32 maxRows, Uint32 bufferSize);

  void prepareReceive(NdbReceiver &receiver) {
    m_rowCount = 0;
    receiver.prepareReceive(m_buffer);
  }

  Uint32 getRowCount() const { return m_rowCount; }

 private:
  /** No copying.*/
  NdbResultSet(const NdbResultSet &);
  NdbResultSet &operator=(const NdbResultSet &);

  /** The buffers which we receive the results into */
  NdbReceiverBuffer *m_buffer;

  /** Array of TupleCorrelations for all rows in m_buffer */
  TupleCorrelation *m_correlations;

  /** The current #rows in 'm_buffer'.*/
  Uint32 m_rowCount;

};  // class NdbResultSet

/**
 * This class manages the subset of result data for one operation that is
 * produced from one SPJ-worker. Note that the child result tuples
 * may come from any fragment, but they all have initial ancestors from the
 * root-fragment(s) scanned by the same SPJ-worker.
 * For each operation there will thus be one NdbResultStream for each worker
 * employed by this SPJ query (one in the case of lookups.)
 * This class has an NdbReceiver object for processing tuples as well as
 * structures for correlating child and parent tuples.
 */
class NdbResultStream {
 public:
  /**
   * @param operation The operation for which we will receive results.
   * @param worker the NdbWorker delivering the result to this 'stream.
   */
  explicit NdbResultStream(NdbQueryOperationImpl &operation, NdbWorker &worker);

  ~NdbResultStream();

  /**
   * Prepare for receiving first results.
   */
  void prepare();

  /** Prepare for receiving next batch of scan results, return nodes prepared */
  SpjTreeNodeMask prepareNextReceiveSet();

  NdbReceiver &getReceiver() { return m_receiver; }

  const NdbReceiver &getReceiver() const { return m_receiver; }

  const char *getCurrentRow() { return m_receiver.getCurrentRow(); }

  /** Get the RANGE_NO for 'CurrentRow', or -1 if not available. */
  int getCurrentRangeNo() const { return m_receiver.get_range_no(); }

  /**
   * Process an incoming tuple for this stream. Extract parent and own tuple
   * ids and pass it on to m_receiver.
   *
   * @param ptr buffer holding tuple.
   * @param len buffer length.
   */
  void execTRANSID_AI(const Uint32 *ptr, Uint32 len,
                      TupleCorrelation correlation);
  void execTRANSID_AI(const Uint32 *ptr, Uint32 len,
                      TupleCorrelation correlation,
                      const NdbApiSignal *aSignal,
                      Uint32 totalLen);

  /**
   * A complete batch has been received from the 'worker' delivering to
   * NdbResultStream. Update whatever required before the appl. are allowed to
   * navigate the result.
   */
  void prepareResultSet(SpjTreeNodeMask expectingResults,
                        SpjTreeNodeMask stillActiveScans);

  /**
   * Navigate within the current ResultSet to resp. first and next row.
   * For non-parent operations in the pushed query, navigation is with respect
   * to any preceding parents which results in this ResultSet depends on.
   * Returns either the tupleNo within TupleSet[] which we navigated to, or
   * tupleNotFound().
   */
  Uint16 firstResult();
  Uint16 nextResult();

  /**
   * Returns true if last row matching the current parent tuple has been
   * consumed.
   */
  bool isEmpty() const { return m_iterState == Iter_finished; }

  /**
   * The internalOpNo is the identifier for this OpNo used in the
   * 'matchingChild' logic in ::prepareResultSet().
   */
  Uint32 getInternalOpNo() const { return m_internalOpNo; }

  /**
   * Get the 'dependants' bitmask - See comments for 'm_dependants as well
   */
  SpjTreeNodeMask getDependants() const { return m_dependants; }

  /**
   * Returns true if this result stream holds the last batch of a sub scan.
   * This means that it is the last batch of the scan that was instantiated
   * from the current batch of its parent operation.
   */
  bool isSubScanComplete(SpjTreeNodeMask remainingScans) const {
    return !remainingScans.get(m_internalOpNo);
  }

  bool isScanQuery() const { return (m_properties & Is_Scan_Query); }

  bool isScanResult() const { return (m_properties & Is_Scan_Result); }
  bool isSortedResult() const { return (m_properties & Is_Sorted_Result); }

  bool isInnerJoin() const { return (m_properties & Is_Inner_Join); }
  bool isOuterJoin() const { return !(m_properties & Is_Inner_Join); }
  bool isAntiJoin() const { return (m_properties & Is_Anti_Join); }

  bool isFirstInner() const { return (m_properties & Is_First_Inner); }

  bool useFirstMatch() const { return (m_properties & Is_First_Match); }

  /** For debugging.*/
  friend NdbOut &operator<<(NdbOut &out, const NdbResultStream &);

  /**
   * TupleSet contain two logically distinct set of information:
   *
   *  - Child/Parent correlation set required to correlate
   *    child tuples with its parents. Child/Tuple pairs are indexed
   *    by tuple number which is the same as the order in which tuples
   *    appear in the NdbReceiver buffers.
   *
   *  - A HashMap on 'm_parentId' used to locate tuples correlated
   *    to a parent tuple. Indexes by hashing the parentId such that:
   *     - [hash(parentId)].m_hash_head will then index the first
   *       TupleSet entry potential containing the parentId to locate.
   *     - .m_hash_next in the indexed TupleSet may index the next TupleSet
   *       to consider.
   *
   * Both the child/parent correlation set and the parentId HashMap has been
   * folded into the same structure in order to reduce number of objects
   * being dynamically allocated.
   * As an advantage this results in an autoscaling of the hash bucket size.
   *
   * Structure is only present if 'isScanQuery'
   */
  class TupleSet {
   public:
    // Tuple ids are unique within this batch and stream
    Uint16
        m_parentId;    // Id of parent tuple which this tuple is correlated with
    Uint16 m_tupleId;  // Id of this tuple

    Uint16 m_hash_head;  // Index of first item in TupleSet[] matching a hashed
                         // parentId.
    Uint16 m_hash_next;  // 'next' index matching

    /**
     * m_matchingChild keep track of current and previous matches found for
     * this tuple in the TupleSet:
     *
     * Bit 0 is the 'skip' bit for the current row. If set the row should be
     * ignored when preparing the result sets and presenting the results rows
     * through the API
     *
     * Note that there are no children with an m_internalOpNo of 0, so using
     * bit-0 as a skip bit should not interfere with matching of child rows.
     *
     * The rest of the bits in m_matchingChild keep track of match history
     * in previous result batches relating to this TupleSet. It has two usages,
     * depending on whether it is an outer- or firstMatch-semi-join:
     *
     * outer-join:
     *   The aggregated set of (outer joined) nests which matched this tuple.
     *   (NULL-extensions excluded.) Only the bit representing the firstInner
     *   of the nest having a matching set of rows is set. Needed in order to
     *   decide when/if a NULL extension of the rows in this outer joined
     *   nest should be emitted or not.
     *
     * firstMatch semi-join:
     *   The aggregated set of treeNodes which has a previous match with tuple.
     *   Used to decide if a firstMatch had already been found for this tuple,
     *   such that further matches should be skipped.
     *
     * The firstMatch-bits are used together with the 'skipTupleFlag' in each
     * tupleId. If a firstMatch has previously been found, we start skipping any
     * later matches by setting the skipTupleFlag. Also see the
     * *SkippedFirstMatch() methods.
     */
    SpjTreeNodeMask m_matchingChild;

    explicit TupleSet() : m_hash_head(tupleNotFound) {}

   private:
    /** No copying.*/
    TupleSet(const TupleSet &);
    TupleSet &operator=(const TupleSet &);
  };

 private:
  /**
   * This stream handles results derived from specified
   * 'm_worker' creating partial SPJ results.
   */
  NdbWorker &m_worker;

  /** Operation to which this resultStream belong.*/
  NdbQueryOperationImpl &m_operation;

  /** Cached internal OpNo, as retrieves from m_operation.getInternalOpNo() */
  const Uint32 m_internalOpNo;

  /** ResultStream for my parent operation, or nullptr if I am root */
  const NdbResultStream *const m_parent;
  /** Children of this operation.*/
  Vector<NdbResultStream *> m_children;

  /**
   * The 'skipFirstInnerOpNo' is used as part of ::prepareResultSet()
   * when there is a non-match for an outer-joined child. It will
   * hold the internalOpNo of the NdbResultStream being either:
   *
   *  1) The first_inner of the (outer-joined) join_nest this
   *     NdbResultStream is a member of.
   *  --OR--
   *  2) If this NdbResultStream is the first_inner itself, it
   *     will hold the first_inner of the join_nest we are embedded within
   *     ( -> or outer joined with...)
   *
   * Thus, in case an outer joined match is not found, *and* a NULL-extended
   * result row should not be created, skipFirstInnerOpNo will then identify
   * the first_inner of an join_nest where the entire nest will not match.
   * prepareResultSet() use this to early-skip impossible matches.
   */
  Uint32 m_skipFirstInnerOpNo;

  /**
   * The dependants node map contain those nodes depending on (the existence of)
   * this internalOpNo. That includes all Op's in the same join nest *after*
   * this op as well as all nodes in other join nests which are nested within
   * the nest of this Op. In terms of QueryOperands that translates to:
   *  - All children of this op.
   *  - All Op's in branches referring this op as a firstUpper/Inner
   *
   * By convention this node itself is also contained in the dependants map
   */
  const SpjTreeNodeMask m_dependants;

  /**
   * The firstMatchedNodes are the set of children nodes, including their
   * dependants, which are firstMatch/semi-joined which this node. It is used
   * together with the TupleSet::m_matchingChild bitmap to test and set when a
   * firstMatch has been found for a particular Tuple.
   */
  SpjTreeNodeMask m_firstMatchedNodes;

  const enum properties {
    Is_Scan_Query = 0x01,
    Is_Scan_Result = 0x02,
    Is_Sorted_Result = 0x04,
    Is_Inner_Join = 0x10,   // As opposed to outer join
    Is_First_Match = 0x20,  // Return FirstMatch only (semijoin)
    Is_Anti_Join = 0x40,
    Is_First_Inner = 0x80
  } m_properties;

  /** The receiver object that unpacks transid_AI messages.*/
  NdbReceiver m_receiver;

  /**
   * ResultSets are received into and read from this stream,
   * possibly doublebuffered,
   */
  NdbResultSet m_resultSets[2];
  Uint32 m_read;  // We read from m_resultSets[m_read]
  Uint32 m_recv;  // We receive into m_resultSets[m_recv]

  /** This is the state of the iterator used by firstResult(), nextResult().*/
  enum {
    /** The first row has not been fetched yet.*/
    Iter_notStarted,
    /** Is iterating the ResultSet, (implies 'm_currentRow!=tupleNotFound').*/
    Iter_started,
    /** Last row for current ResultSet has been returned.*/
    Iter_finished
  } m_iterState;

  /**
   * Tuple id of the current tuple, or 'tupleNotFound'
   * if Iter_notStarted or Iter_finished.
   */
  Uint16 m_currentRow;

  /** Max #rows which this stream may receive in its TupleSet structures */
  Uint32 m_maxRows;

  /** TupleSet contains the correlation between parent/children */
  TupleSet *m_tupleSet;

  void buildResultCorrelations();

  Uint16 getTupleId(Uint16 tupleNo) const {
    return (m_tupleSet) ? m_tupleSet[tupleNo].m_tupleId : 0;
  }

  Uint16 getCurrentTupleId() const {
    return (m_currentRow == tupleNotFound) ? tupleNotFound
                                           : getTupleId(m_currentRow);
  }

  Uint16 findTupleWithParentId(Uint16 parentId) const;

  Uint16 findNextTuple(Uint16 tupleNo) const;

  /** Set/clear/check whether the specified tupleNo should become invisible */
  void setSkipped(Uint16 tupleNo) {
    m_tupleSet[tupleNo].m_matchingChild.set(0U);
  }

  void clearSkipped(Uint16 tupleNo) {
    m_tupleSet[tupleNo].m_matchingChild.clear(0U);
  }

  bool isSkipped(Uint16 tupleNo) const {
    return m_tupleSet[tupleNo].m_matchingChild.get(0U);
  }

  /**
   * The skip methods above are a 'one time'-skip, where the tuples are
   * skipped for this result batch only, and the skip recalculated for the
   * next batch. For FirstMatch we need to skip the matched row for multiple
   * batches, so we have a special variant for doing firstMatch-skip.
   * (Also see comment for the 'm_matchingChild' member variable).
   *
   * Note that a firstMatch-skip also implies a 'normal' skip, but not the
   * other way around.
   */
  void setSkippedFirstMatch(Uint16 tupleNo) {
    // Assert: has already seen a firstMatch
    assert(m_tupleSet[tupleNo].m_matchingChild.contains(m_firstMatchedNodes));
    m_tupleSet[tupleNo].m_tupleId |= skipTupleFlag;
    setSkipped(tupleNo);
  }

  void clearSkippedFirstMatch(Uint16 tupleNo) {
    // Assert: has already seen a firstMatch
    assert(m_tupleSet[tupleNo].m_matchingChild.contains(m_firstMatchedNodes));
    m_tupleSet[tupleNo].m_tupleId &= ~skipTupleFlag;
    m_tupleSet[tupleNo].m_matchingChild.bitANDC(m_firstMatchedNodes);
  }

  bool isSkippedFirstMatch(Uint16 tupleNo) const {
    // Assert: has already seen a firstMatch
    assert(m_tupleSet[tupleNo].m_matchingChild.contains(m_firstMatchedNodes));
    assert(isSkipped(tupleNo) ||
           !(m_tupleSet[tupleNo].m_tupleId & skipTupleFlag));
    return (m_tupleSet[tupleNo].m_tupleId & skipTupleFlag);
  }

  /** No copying.*/
  NdbResultStream(const NdbResultStream &);
  NdbResultStream &operator=(const NdbResultStream &);
};  // class NdbResultStream

//////////////////////////////////////////////
/////////  NdbBulkAllocator methods ///////////
//////////////////////////////////////////////

NdbBulkAllocator::NdbBulkAllocator(size_t objSize)
    : m_objSize(objSize), m_maxObjs(0), m_buffer(nullptr), m_nextObjNo(0) {}

int NdbBulkAllocator::init(Uint32 maxObjs) {
  assert(m_buffer == nullptr);
  m_maxObjs = maxObjs;
  // Add check for buffer overrun.
  m_buffer = new char[m_objSize * m_maxObjs + 1];
  if (unlikely(m_buffer == nullptr)) {
    return Err_MemoryAlloc;
  }
  m_buffer[m_maxObjs * m_objSize] = endMarker;
  return 0;
}

void NdbBulkAllocator::reset() {
  // Overrun check.
  assert(m_buffer == nullptr || m_buffer[m_maxObjs * m_objSize] == endMarker);
  delete[] m_buffer;
  m_buffer = nullptr;
  m_nextObjNo = 0;
  m_maxObjs = 0;
}

void *NdbBulkAllocator::allocObjMem(Uint32 noOfObjs) {
  assert(m_nextObjNo + noOfObjs <= m_maxObjs);
  void *const result = m_buffer + m_objSize * m_nextObjNo;
  m_nextObjNo += noOfObjs;
  return m_nextObjNo > m_maxObjs ? nullptr : result;
}

///////////////////////////////////////////
/////////  NdbResultSet methods ///////////
///////////////////////////////////////////
NdbResultSet::NdbResultSet()
    : m_buffer(nullptr), m_correlations(nullptr), m_rowCount(0) {}

void NdbResultSet::init(NdbQueryImpl &query, Uint32 maxRows,
                        Uint32 bufferSize) {
  {
    NdbBulkAllocator &bufferAlloc = query.getRowBufferAlloc();
    Uint32 *buffer =
        reinterpret_cast<Uint32 *>(bufferAlloc.allocObjMem(bufferSize));
    m_buffer = NdbReceiver::initReceiveBuffer(buffer, bufferSize, maxRows);

    if (query.getQueryDef().isScanQuery()) {
      m_correlations = reinterpret_cast<TupleCorrelation *>(
          bufferAlloc.allocObjMem(maxRows * sizeof(TupleCorrelation)));
    }
  }
}

//////////////////////////////////////////////
/////////  NdbResultStream methods ///////////
//////////////////////////////////////////////

NdbResultStream::NdbResultStream(NdbQueryOperationImpl &operation,
                                 NdbWorker &worker)
    : m_worker(worker),
      m_operation(operation),
      m_internalOpNo(operation.getInternalOpNo()),
      m_parent(operation.getParentOperation()
                   ? &worker.getResultStream(*operation.getParentOperation())
                   : nullptr),
      m_children(),
      m_skipFirstInnerOpNo(~0U),
      m_dependants(operation.getDependants()),
      m_firstMatchedNodes(),
      m_properties((enum properties)(
          (operation.getQueryDef().isScanQuery() ? Is_Scan_Query : 0) |
          (operation.getQueryOperationDef().isScanOperation() ? Is_Scan_Result
                                                              : 0) |
          (operation.getOrdering() != NdbQueryOptions::ScanOrdering_unordered
               ? Is_Sorted_Result
               : 0)
          // Note1: If an ancestor is a firstMatch-type, we only need to
          // firstMatch this as well. Note2: FirstMatch is only relevant for
          // scans (Both are optimizations only)
          | ((operation.getQueryOperationDef().getMatchType() &
                  NdbQueryOptions::MatchFirst ||
              operation.getQueryOperationDef().hasFirstMatchAncestor()) &&
                     operation.getQueryOperationDef().isScanOperation()
                 ? Is_First_Match
                 : 0) |
          (operation.getQueryOperationDef().getMatchType() &
                   NdbQueryOptions::MatchNonNull
               ? Is_Inner_Join
               : 0) |
          (operation.getQueryOperationDef().getMatchType() &
                   NdbQueryOptions::MatchNullOnly
               ? Is_Anti_Join
               : 0)
          // Is_first_Inner; if outer joined (with upper nest) and another
          // firstInner than this 'operation' not specified
          |
          ((operation.getQueryOperationDef().getMatchType() &
            NdbQueryOptions::MatchNonNull) == 0 &&
                   (operation.getQueryOperationDef().getFirstInner() ==
                        &operation.getQueryOperationDef() ||
                    operation.getQueryOperationDef().getFirstInner() == nullptr)
               ? Is_First_Inner
               : 0))),
      m_receiver(operation.getQuery().getNdbTransaction().getNdb()),
      m_resultSets(),
      m_read(0xffffffff),
      m_recv(0),
      m_iterState(Iter_finished),
      m_currentRow(tupleNotFound),
      m_maxRows(0),
      m_tupleSet(nullptr) {
  if (m_parent != nullptr) {
    const int res =
        const_cast<NdbResultStream *>(m_parent)->m_children.push_back(this);
    if (res != 0) {
      operation.getQuery().setErrorCode(Err_MemoryAlloc);
      return;
    }

    if (isOuterJoin()) {
      /**
       * Outer joined scan child need to know the first inner of  the
       * join nest it is a member of. Used by prepareResultSet() to decide
       * if/when a NULL extended row should be allowed for the outer join.
       */
      const NdbQueryOperationDefImpl &queryOperationDef =
          m_operation.getQueryOperationDef();
      const NdbQueryOperationDefImpl *firstInEmbeddingNestDef =
          queryOperationDef.getFirstInEmbeddingNest();

      if (firstInEmbeddingNestDef == nullptr) {
        m_skipFirstInnerOpNo = m_parent->getInternalOpNo();
      } else {
        if (firstInEmbeddingNestDef->getInternalOpNo() <=
            m_parent->getInternalOpNo()) {
          // First is 'above' parent -> Is parent or an ancestor of 'this'
          // stream
          m_skipFirstInnerOpNo = m_parent->getInternalOpNo();
        } else {
          assert(!isScanResult() ||
                 firstInEmbeddingNestDef->getParentOperation() ==
                     queryOperationDef.getParentOperation());
          m_skipFirstInnerOpNo = firstInEmbeddingNestDef->getInternalOpNo();
        }
      }
    }
  }
}

NdbResultStream::~NdbResultStream() {
  for (int i = static_cast<int>(m_maxRows) - 1; i >= 0; i--) {
    m_tupleSet[i].~TupleSet();
  }
}

void NdbResultStream::prepare() {
  NdbQueryImpl &query = m_operation.getQuery();

  const Uint32 resultBufferSize = m_operation.getResultBufferSize();
  if (isScanQuery()) {
    /* Parent / child correlation is only relevant for scan type queries
     * Don't create a m_tupleSet with these correlation id's for lookups!
     */
    const Uint32 fragsPerWorker = query.getFragsPerWorker();

    m_maxRows = fragsPerWorker * m_operation.getMaxBatchRows();
    m_tupleSet = new (query.getTupleSetAlloc().allocObjMem(m_maxRows))
        TupleSet[m_maxRows];

    // Scan results may be double buffered
    m_resultSets[0].init(query, m_maxRows, fragsPerWorker * resultBufferSize);
    m_resultSets[1].init(query, m_maxRows, fragsPerWorker * resultBufferSize);
  } else {
    m_maxRows = 1;
    m_resultSets[0].init(query, m_maxRows, resultBufferSize);
  }

  /* Alloc buffer for unpacked NdbRecord row */
  const Uint32 rowSize = m_operation.getRowSize();
  assert((rowSize % sizeof(Uint32)) == 0);
  char *rowBuffer =
      reinterpret_cast<char *>(query.getRowBufferAlloc().allocObjMem(rowSize));
  assert(rowBuffer != nullptr);

  m_receiver.init(NdbReceiver::NDB_QUERY_OPERATION, &m_operation);
  m_receiver.do_setup_ndbrecord(m_operation.getNdbRecord(), rowBuffer,
                                m_operation.needRangeNo(),
                                /*read_key_info=*/false);
}  // NdbResultStream::prepare

/** Locate, and return 'tupleNo', of first tuple with specified parentId.
 *  parentId == tupleNotFound is use as a special value for iterating results
 *  from the root operation in the order which they was inserted by
 *  ::buildResultCorrelations()
 *
 *  Position of 'currentRow' is *not* updated and should be modified by callee
 *  if it want to keep the new position.
 */
Uint16 NdbResultStream::findTupleWithParentId(Uint16 parentId) const {
  assert((parentId == tupleNotFound) == (m_parent == nullptr));

  if (likely(m_resultSets[m_read].m_rowCount > 0)) {
    if (m_tupleSet == nullptr) {
      assert(m_resultSets[m_read].m_rowCount <= 1);
      return 0;
    }

    const Uint16 hash = (parentId % m_maxRows);
    Uint16 currentRow = m_tupleSet[hash].m_hash_head;
    while (currentRow != tupleNotFound) {
      assert(currentRow < m_maxRows);
      if (!isSkipped(currentRow) &&
          m_tupleSet[currentRow].m_parentId == parentId) {
        return currentRow;
      }
      currentRow = m_tupleSet[currentRow].m_hash_next;
    }
  }
  return tupleNotFound;
}  // NdbResultStream::findTupleWithParentId()

/** Locate, and return 'tupleNo', of next tuple with same parentId as currentRow
 *  Position of 'currentRow' is *not* updated and should be modified by callee
 *  if it want to keep the new position.
 */
Uint16 NdbResultStream::findNextTuple(Uint16 tupleNo) const {
  if (tupleNo != tupleNotFound && m_tupleSet != nullptr) {
    assert(tupleNo < m_maxRows);
    Uint16 parentId = m_tupleSet[tupleNo].m_parentId;
    Uint16 nextRow = m_tupleSet[tupleNo].m_hash_next;

    while (nextRow != tupleNotFound) {
      assert(nextRow < m_maxRows);
      if (!isSkipped(nextRow) && m_tupleSet[nextRow].m_parentId == parentId) {
        return nextRow;
      }
      nextRow = m_tupleSet[nextRow].m_hash_next;
    }
  }
  return tupleNotFound;
}  // NdbResultStream::findNextTuple()

Uint16 NdbResultStream::firstResult() {
  Uint16 parentId = tupleNotFound;
  if (m_parent != nullptr) {
    if (!m_worker.hasValidRow(m_parent) ||
        (parentId = m_parent->getCurrentTupleId()) == tupleNotFound) {
      m_currentRow = tupleNotFound;
      m_iterState = Iter_finished;
      return tupleNotFound;
    }
  }

  if ((m_currentRow = findTupleWithParentId(parentId)) != tupleNotFound) {
    m_iterState = Iter_started;
    const char *p =
        m_receiver.getRow(m_resultSets[m_read].m_buffer, m_currentRow);
    assert(p != nullptr);
    ((void)p);
    m_worker.setValidRow(this);
    return m_currentRow;
  }

  m_iterState = Iter_finished;
  return tupleNotFound;
}  // NdbResultStream::firstResult()

Uint16 NdbResultStream::nextResult() {
  DBUG_ENTER("NdbResultStream::nextResult");
  // Fetch next row for this stream
  if (m_worker.hasValidRow(this) && m_currentRow != tupleNotFound &&
      (m_currentRow = findNextTuple(m_currentRow)) != tupleNotFound) {
    m_iterState = Iter_started;
    const char *p =
        m_receiver.getRow(m_resultSets[m_read].m_buffer, m_currentRow);
    assert(p != nullptr);
    ((void)p);
    DBUG_PRINT("info", ("p: 0x%p, m_currentRow: %u", p, m_currentRow));
    m_worker.setValidRow(this);
    DBUG_RETURN(m_currentRow);
  }
  m_iterState = Iter_finished;
  DBUG_RETURN(tupleNotFound);
}  // NdbResultStream::nextResult()

/**
 * Callback when a TRANSID_AI signal (receive row) is processed.
 * Handles non-fragmented signals.
 */
void NdbResultStream::execTRANSID_AI(const Uint32 *ptr, Uint32 len,
                                     TupleCorrelation correlation) {
  NdbResultSet &receiveSet = m_resultSets[m_recv];
  if (isScanQuery()) {
    /**
     * Store TupleCorrelation.
     */
    receiveSet.m_correlations[receiveSet.m_rowCount] = correlation;
  }

  m_receiver.execTRANSID_AI(ptr, len);
  receiveSet.m_rowCount++;
}  // NdbResultStream::execTRANSID_AI()

/* Handle fragmented signals for pushdown joins */
void NdbResultStream::execTRANSID_AI(const Uint32 *ptr, Uint32 len,
                                     TupleCorrelation correlation,
                                     const NdbApiSignal *aSignal,
                                     Uint32 totalLen) {
  m_receiver.execTRANSID_AI(ptr, len, aSignal, totalLen);
  if (aSignal->isLastFragment()) {
    NdbResultSet &receiveSet = m_resultSets[m_recv];
    if (isScanQuery()) {
      /**
       * Store TupleCorrelation.
       */
      receiveSet.m_correlations[receiveSet.m_rowCount] = correlation;
    }

    receiveSet.m_rowCount++;
  }
}

/**
 * Make preparation for another batch of results to be received.
 * This NdbResultStream, and all its sibling will receive a batch
 * of results from the datanodes.
 */
SpjTreeNodeMask NdbResultStream::prepareNextReceiveSet() {
  SpjTreeNodeMask prepared;

  if (isScanQuery())  // Doublebuffered ResultSet[] if isScanQuery()
  {
    m_recv = (m_recv + 1) % 2;  // Receive into next ResultSet
    assert(m_recv != m_read);
  }

  m_resultSets[m_recv].prepareReceive(m_receiver);
  prepared.set(getInternalOpNo());

  /**
   * If this stream will get new rows in the next batch, then so will
   * all of its descendants.
   */
  for (Uint32 childNo = 0; childNo < m_operation.getNoOfChildOperations();
       childNo++) {
    NdbQueryOperationImpl &child = m_operation.getChildOperation(childNo);
    prepared.bitOR(m_worker.getResultStream(child).prepareNextReceiveSet());
  }
  return prepared;
}  // NdbResultStream::prepareNextReceiveSet

/**
 * Make preparations for another batch of result to be read:
 *  - Advance to next NdbResultSet. (or reuse last)
 *  - Fill in parent/child result correlations in m_tupleSet[]
 *    for those getting a new ResulSet in this batch.
 *  - Apply inner/outer join filtering to remove non qualifying
 *    rows.
 */
void NdbResultStream::prepareResultSet(const SpjTreeNodeMask expectingResults,
                                       const SpjTreeNodeMask stillActive) {
  /**
   * Prepare NdbResultSet for reading - either the next
   * 'new' received from datanodes or reuse the last as has been
   * determined by ::prepareNextReceiveSet()
   */
  m_read = m_recv;
  const NdbResultSet &readResult = m_resultSets[m_read];

  if (m_tupleSet != nullptr && expectingResults.get(getInternalOpNo())) {
    buildResultCorrelations();
  }

  for (int childNo = m_children.size() - 1; childNo >= 0; childNo--) {
    NdbResultStream &childStream = *m_children[childNo];
    if (expectingResults.overlaps(childStream.m_dependants)) {
      // childStream got new result rows
      childStream.prepareResultSet(expectingResults, stillActive);
    }
  }

  // The 'highest order' child treeNode in expectingResults decides
  // whether firstMatch elimination should be done in result set or not.
  const uint firstInExpected = expectingResults.find_first();

  // Prepare rows from the NdbQueryOperation's accessible now
  if (m_tupleSet != nullptr) {
    const Uint32 thisOpId = getInternalOpNo();
    const Uint32 rowCount = readResult.getRowCount();

    /**
     * For sorted result streams only the last row will get new related
     * child rows in a 'nextResult', other rows can be skipped immediately.
     * Note that such skipped rows would also have been NULL-expand already
     * if they were part of an outer join.
     */
    Uint32 tupleNo = 0;
    if (isSortedResult() && !expectingResults.get(thisOpId)) {
      while (tupleNo < rowCount - 1) {
        setSkipped(tupleNo++);
      }
    }

    for (; tupleNo < rowCount; tupleNo++) {
      /**
       * FirstMatch handling: If this tupleNo already found a match from all
       * tables, we skip it from further result processing:
       */
      if (!m_firstMatchedNodes
               .isclear() &&  // Some children are firstmatch-semi-joins
          m_tupleSet[tupleNo].m_matchingChild.contains(m_firstMatchedNodes)) {
        // We have already found a match for (all of) our firstMatchedNodes.
        // Should we skip potentially duplicates now? :

        if (m_firstMatchedNodes.get(firstInExpected)) {
          // Got a new set of firstMatch'ed rows, starting with semi-joined
          // tables. Skip parent rows which already had its 'firstMatch'
          if (unlikely(traceSignals)) {
            ndbout << "prepareResultSet, useFirstMatch"
                   << ", seen matches -> skip tupleNo"
                   << ", opNo: " << thisOpId << ", row: " << tupleNo << endl;
          }
          // Done with this tupleNo
          setSkippedFirstMatch(tupleNo);
          continue;  // Skip further processing of this row
        } else if (!m_firstMatchedNodes.overlaps(expectingResults)) {
          // No semi joined tables affected by the 'expecting'.
          // Do nothing, except keeping 'isSkipped' if already set.
          if (unlikely(traceSignals)) {
            ndbout << "prepareResultSet, 'expecting' doesn't overlaps "
                      "FirstMatchNodes"
                   << ", opNo: " << thisOpId << ", row: " << tupleNo
                   << ", isSkipped?: " << isSkippedFirstMatch(tupleNo) << endl;
          }
          if (isSkippedFirstMatch(tupleNo))  // Already had a firstMatch
            continue;                        // Keep skipping it
        } else {
          // Set of new children rows start with a full-join. Thus, the
          // firstMatch handling is reset as part of preparing the new
          // joined result set.
          if (unlikely(traceSignals)) {
            ndbout << "prepareResultSet, Join-useFirstMatch"
                   << ", cleared 'hadMatching'-> un-skip"
                   << ", opNo: " << thisOpId << ", row: " << tupleNo << endl;
          }
          clearSkippedFirstMatch(tupleNo);
        }
      }  // FirstMatch

      /**
       * For each children; try to locate a matching row for tupleNo.
       * Note down in hasMatchingChild when matching children are
       * (not) found. We try to break out of the child-loop as soon
       * as possible when we can conclude that a join-match is not
       * possible. In such cases the thisOpNo-bit in hasMatchingChild
       * is cleared in order to signal a 'skip' of this tupleNo.
       *
       * We can always 'skip' if the join-type is an InnerJoin.
       * Else we use 'm_skipFirstInnerOpNo' to decide if an early 'skip'
       * is possible or not.
       *
       * In addition there is extra logic for outer joins to decide
       * if a NULL extended row should be made visible or not.
       */
      SpjTreeNodeMask hasMatchingChild;  // Collected Join match properties
      hasMatchingChild.set();            // Assume a match
      const Uint16 tupleId = getTupleId(tupleNo);

      for (int childNo = m_children.size() - 1; childNo >= 0; childNo--) {
        const NdbResultStream &childStream = *m_children[childNo];
        const Uint32 childId = childStream.getInternalOpNo();

        /**
         * Check for a matching child row(s). A 'skipFirstInnerOpNo'
         * could possibly already have concluded the join-nest to be
         * a non-match, and cleared our hasMatchingChild-bit.
         */
        const bool childMatched =
            hasMatchingChild.get(childId)
                ? (childStream.findTupleWithParentId(tupleId) != tupleNotFound)
                : false;  // A previous (inner joined) Op already decided
                          // 'no-match'

        if (unlikely(traceSignals)) {
          const char *const state = (childMatched) ? "MATCHED" : "NO MATCH";
          ndbout << "prepareResultSet, " << state << ", opNo: " << thisOpId
                 << ", row: " << tupleNo << ", child: " << childId << endl;
        }

        if (childMatched == false)  // Didn't match
        {
          hasMatchingChild.clear(childId);
          if (childStream.isInnerJoin()) {
            if (unlikely(traceSignals)) {
              ndbout << "prepareResultSet, isInnerJoin"
                     << ", skip non-match"
                     << ", opNo: " << thisOpId << ", row: " << tupleNo
                     << ", child: " << childId << endl;
            }
            hasMatchingChild.clear(thisOpId);  // Skip this tupleNo
            break;
          }
        }

        if (childStream.isOuterJoin()) {
          /**
           * A NULL-extended row should be emitted when we know there are
           * no possibilities for finding a child-match, That is:
           *  1) There are no more unfetched result rows from any of the
           *     outer-joined tables, or descendants of these.
           *  2) This NdbResultStream is known to return a sorted result,
           *     which also guarantes that all child streams returned all
           *     related rows in the first batch. (Except the last one)
           */
          const bool last_child_seen =
              !stillActive.overlaps(childStream.m_dependants) ||  // 1)
              (isSortedResult() && tupleNo < rowCount - 1);       // 2)

          if (childMatched == true) {
            /**
             * Found a match for this outer joined child.
             * If child is the firstInner in this outer-joined_nest, the entire
             * nest matched the 'outer' join condition. Thus, no later
             * NULL-extended rows should be created for this nest.
             * -> Remember that to avoid later NULL extensions
             * (Also see comments for 'm_matchingChild' member variable)
             */
            if (childStream.isFirstInner()) {
              m_tupleSet[tupleNo].m_matchingChild.set(childId);
              if (unlikely(traceSignals)) {
                ndbout << "prepareResultSet, isOuterJoin"
                       << ", matched 'innerNest'"
                       << ", opNo: " << thisOpId << ", row: " << tupleNo
                       << ", child: " << childId << endl;
              }
              if (childStream.isAntiJoin()) {
                hasMatchingChild.clear(thisOpId);  // Skip this tupleNo/nest
                break;
              }
            }
          }
          /**
           * Else: No matching children found from 'childId'. Now we may either
           * create a NULL-extended row for the outer join(s), or keep looking
           * for matches in later batches.
           *
           * A NULL-extended row should be created if:
           *  1) This child is the firstInner in this outer-joined_nest, and
           *  2) There are no more unfetched result rows from childStreams.
           *  3) A previous join-match had not been found
           */
          else if (childStream.isFirstInner() &&                       // 1)
                   last_child_seen &&                                  // 2)
                   !m_tupleSet[tupleNo].m_matchingChild.get(childId))  // 3)
          {
            /**
             * NULL-extend join-nest:
             *
             * No previous match found in the nest where child is 'firstInner',
             * and no more rows expected. Make 'thisOpId' visible such that
             * a NULL-extended child row(s) can be created.
             */
            assert(hasMatchingChild.get(thisOpId));

            if (unlikely(traceSignals)) {
              const char *reason = childStream.isAntiJoin() ? "(antijoin match)"
                                                            : "(never matched)";
              ndbout << "prepareResultSet, isOuterJoin"
                     << ", NULL-extend, " << reason << ", opNo: " << thisOpId
                     << ", row: " << tupleNo << ", child: " << childId << endl;
            }
          } else {
            /**
             * This is a non-match, without a NULL-extended join-nest (yet).
             * Entire join-nest then becomes a match-failure itself.
             * Handle this by 'un-matching' the firstInner of the join-nest.
             */
            const Uint32 skipFirstInnerOpNo = childStream.m_skipFirstInnerOpNo;
            assert(skipFirstInnerOpNo != ~0U);
            hasMatchingChild.clear(skipFirstInnerOpNo);  // Un-match join-nest

            if (likely(skipFirstInnerOpNo == thisOpId)) {
              /**
               * firstInner in child's join-nest is thisOpId. 'un-matching'
               * it also allowes us to conclude that thisOpNo is a 'skip'.
               */
              if (unlikely(traceSignals)) {
                ndbout
                    << "prepareResultSet, isOuterJoin, ('child' is firstInner)"
                    << "  -> Skip it" << endl;
              }
              break;  // Skip further child matching against this tupleNo
            } else if (unlikely(traceSignals)) {
              /**
               * Join-nests has a first-inner being a sibling of (same parent
               * as) this childStream. Can not skip yet, but was un-matched
               * above such that we detect it as a failed-match when processed
               * later.
               */
              ndbout << "prepareResultSet, isOuterJoin (has firstInnerSibling)"
                     << ", un-match firstInner: " << skipFirstInnerOpNo << endl;
            }
          }
        }  // if (isOuterJoin())
      }    // for (childNo..)

      /**
       * If some 'required' descendants of tupleNo didn't 'Match' (possibly with
       * a NULL-row), the 'thisOpId' bit would have been cleared when checking
       * the descendant Op's above. This tuple then needs to be skipped for now.
       * May still be included in later result batches though, with a new set
       * of descendants' row either matching or allowing NULL extensions.
       */
      if (!hasMatchingChild.get(thisOpId)) {
        // Persist the decision to skip this tupleNo
        setSkipped(tupleNo);
      } else  // tupleNo is part of (intermediate) results
      {
        clearSkipped(tupleNo);

        /**
         * When we get here we know that all children matched, including the
         * 'firstMatchedNodes' (which are possibly a 0-mask if none
         * 'useFirstMatch') Anyway we note down that a potential firstmatch has
         * been found.
         */
        m_tupleSet[tupleNo].m_matchingChild.bitOR(m_firstMatchedNodes);
      }
    }  // for (tupleNo..)
  }    // if (m_tupleSet ..)

  // Set current position 'before first'
  m_iterState = Iter_notStarted;
  m_currentRow = tupleNotFound;
}  // NdbResultStream::prepareResultSet()

/**
 * Fill m_tupleSet[] with correlation data between parent
 * and child tuples. The 'TupleCorrelation' is stored
 * in an array of TupleCorrelations in each ResultSet
 * by execTRANSID_AI().
 *
 * NOTE: In order to reduce work done when holding the
 * transporter mutex, the 'TupleCorrelation' is only stored
 * in the buffer when it arrives. Later (here) we build the
 * correlation hashMap immediately before we prepare to
 * read the NdbResultSet.
 */
void NdbResultStream::buildResultCorrelations() {
  const NdbResultSet &readResult = m_resultSets[m_read];

  // Collect set of children being firstMatch (semi-)joined
  for (int childNo = m_children.size() - 1; childNo >= 0; childNo--) {
    NdbResultStream &childStream = *m_children[childNo];
    if (childStream.useFirstMatch()) {
      m_firstMatchedNodes.bitOR(childStream.m_dependants);
    }
  }

  // if (m_tupleSet!=NULL)
  {
    /* Clear the hashmap structures */
    for (Uint32 i = 0; i < m_maxRows; i++) {
      m_tupleSet[i].m_hash_head = tupleNotFound;
    }

    /* Rebuild correlation & hashmap from 'readResult' */
    for (Uint32 tupleNo = 0; tupleNo < readResult.m_rowCount; tupleNo++) {
      const Uint16 tupleId = readResult.m_correlations[tupleNo].getTupleId();
      const Uint16 parentId =
          (m_parent != nullptr)
              ? readResult.m_correlations[tupleNo].getParentTupleId()
              : tupleNotFound;

      // It is a protocol limitation that correlation-ids use the lower 12-bits
      // only The upper bit is used by the firstMatch skip logic
      assert((tupleId & skipTupleFlag) == 0);

      m_tupleSet[tupleNo].m_parentId = parentId;
      m_tupleSet[tupleNo].m_tupleId = tupleId;
      m_tupleSet[tupleNo].m_matchingChild.clear();

      /* Insert into parentId-hashmap */
      const Uint16 hash = (parentId % m_maxRows);
      if (m_parent == nullptr) {
        /* Root stream: Insert sequentially in hash_next to make it
         * possible to use ::findTupleWithParentId() and ::findNextTuple()
         * to navigate even the root operation.
         */
        /* Link into m_hash_next in order to let ::findNextTuple() navigate
         * correctly */
        if (tupleNo == 0)
          m_tupleSet[hash].m_hash_head = tupleNo;
        else
          m_tupleSet[tupleNo - 1].m_hash_next = tupleNo;
        m_tupleSet[tupleNo].m_hash_next = tupleNotFound;
      } else {
        /* Insert parentId in HashMap */
        m_tupleSet[tupleNo].m_hash_next = m_tupleSet[hash].m_hash_head;
        m_tupleSet[hash].m_hash_head = tupleNo;
      }
    }
  }
}  // NdbResultStream::buildResultCorrelations

/////////////////////////////////////
////////  NdbWorker methods /////////
/////////////////////////////////////
void NdbWorker::buildReceiverIdMap(NdbWorker *workers, Uint32 noOfWorkers) {
  for (Uint32 workerNo = 0; workerNo < noOfWorkers; workerNo++) {
    const Uint32 receiverId = workers[workerNo].getReceiverId();
    /**
     * For reasons unknown, NdbObjectIdMap shifts ids two bits to the left,
     * so we must do the opposite to get a good hash distribution.
     */
    assert((receiverId & 0x3) == 0);
    const int hash = (receiverId >> 2) % noOfWorkers;
    workers[workerNo].m_idMapNext = workers[hash].m_idMapHead;
    workers[hash].m_idMapHead = workerNo;
  }
}

// static
NdbWorker *NdbWorker::receiverIdLookup(NdbWorker *workers, Uint32 noOfWorkers,
                                       Uint32 receiverId) {
  /**
   * For reasons unknown, NdbObjectIdMap shifts ids two bits to the left,
   * so we must do the opposite to get a good hash distribution.
   */
  assert((receiverId & 0x3) == 0);
  const int hash = (receiverId >> 2) % noOfWorkers;
  int current = workers[hash].m_idMapHead;
  assert(current < static_cast<int>(noOfWorkers));
  while (current >= 0 && workers[current].getReceiverId() != receiverId) {
    current = workers[current].m_idMapNext;
    assert(current < static_cast<int>(noOfWorkers));
  }
  if (unlikely(current < 0)) {
    return nullptr;
  } else {
    return &workers[current];
  }
}

NdbWorker::NdbWorker()
    : m_query(nullptr),
      m_workerNo(voidWorkerNo),
      m_rootOpNo(0),
      m_resultStreams(nullptr),
      m_pendingRequests(0),
      m_availResultSets(0),
      m_outstandingResults(0),
      m_confReceived(false),
      m_validResultStreams(),
      m_preparedReceiveSet(),
      m_nextScans(),
      m_activeScans(),
      m_idMapHead(-1),
      m_idMapNext(-1) {
  m_nextScans.set();
}

NdbWorker::~NdbWorker() { assert(m_resultStreams == nullptr); }

void NdbWorker::init(NdbQueryImpl &query, Uint32 workerNo) {
  assert(m_workerNo == voidWorkerNo);
  m_query = &query;
  m_workerNo = workerNo;
  m_rootOpNo = query.getRootStreamOpNo();

  m_resultStreams = reinterpret_cast<NdbResultStream *>(
      query.getResultStreamAlloc().allocObjMem(query.getNoOfOperations()));
  assert(m_resultStreams != nullptr);

  for (unsigned opNo = 0; opNo < query.getNoOfOperations(); opNo++) {
    NdbQueryOperationImpl &op = query.getQueryOperation(opNo);
    new (&m_resultStreams[opNo]) NdbResultStream(op, *this);
    // CTE subtree containers and CTE-embedded ops don't receive
    // rows via the NDB API — skip receiver buffer allocation.
    const NdbQueryOperationDefImpl &def = op.getQueryOperationDef();
    if (def.getType() != NdbQueryOperationDef::CteSubtree &&
        !def.isCteEmbedded()) {
      m_resultStreams[opNo].prepare();
    }
  }
}

/**
 * Release what we want need anymore after last available row has been
 * returned from datanodes.
 */
void NdbWorker::postFetchRelease() {
  if (m_resultStreams != nullptr) {
    for (unsigned opNo = 0; opNo < m_query->getNoOfOperations(); opNo++) {
      m_resultStreams[opNo].~NdbResultStream();
    }
  }
  /**
   * Don't 'delete' the object as it was in-place constructed from
   * ResultStreamAlloc'ed memory. Memory is released by
   * ResultStreamAlloc::reset().
   */
  m_resultStreams = nullptr;
}

NdbResultStream &NdbWorker::getResultStream(Uint32 operationNo) const {
  assert(m_resultStreams);
  return m_resultStreams[operationNo];
}

bool NdbWorker::hasValidRow(const NdbResultStream *resultStream) const {
  return m_validResultStreams.get(resultStream->getInternalOpNo());
}

void NdbWorker::setValidRow(const NdbResultStream *resultStream) {
  /**
   * Register a new 'valid' row for resultStream. However, that also
   * *invalidate* all current rows in its dependants operations.
   */
  m_validResultStreams.bitANDC(resultStream->getDependants());
  m_validResultStreams.set(resultStream->getInternalOpNo());
}

/**
 * Throw any pending ResultSets from specified workers[]
 */
// static
void NdbWorker::clear(NdbWorker *workers, Uint32 noOfWorkers) {
  if (workers != nullptr) {
    for (Uint32 workerNo = 0; workerNo < noOfWorkers; workerNo++) {
      workers[workerNo].m_pendingRequests = 0;
      workers[workerNo].m_availResultSets = 0;
    }
  }
}

/**
 * Check if there has been requested more ResultSets from
 * this worker which has not been consumed yet.
 * (This is also a candicate check for ::hasReceivedMore())
 */
bool NdbWorker::hasRequestedMore() const { return (m_pendingRequests > 0); }

/**
 * Signal that another complete ResultSet is available from
 * this worker.
 * Need mutex lock as 'm_availResultSets' is accessed both from
 * receiver and application thread.
 */
void NdbWorker::setReceivedMore()  // Need mutex
{
  assert(m_availResultSets == 0);
  m_availResultSets++;
}

/**
 * Check if another ResultSets has been received and is available
 * for reading. It will be given to the application thread when it
 * call ::grabNextResultSet().
 * Need mutex lock as 'm_availResultSets' is accessed both from
 * receiver and application thread.
 */
bool NdbWorker::hasReceivedMore() const  // Need mutex
{
  return (m_availResultSets > 0);
}

void NdbWorker::prepareNextReceiveSet() {
  assert(m_workerNo != voidWorkerNo);
  assert(m_outstandingResults == 0);

  m_preparedReceiveSet.clear();
  for (unsigned opNo = 0; opNo < m_query->getNoOfOperations(); opNo++) {
    NdbResultStream &resultStream = getResultStream(opNo);
    if (!resultStream.isSubScanComplete(m_nextScans)) {
      /**
       * Reset resultStream and all its descendants, since all these
       * streams will get a new set of rows in the next batch.
       */
      m_preparedReceiveSet.bitOR(resultStream.prepareNextReceiveSet());
    }
  }
  m_confReceived = false;
  m_pendingRequests++;
}

/**
 * Let the application thread take ownership of an available
 * ResultSet, prepare it for reading first row.
 * Need mutex lock as 'm_availResultSets' is accessed both from
 * receiver and application thread.
 */
void NdbWorker::grabNextResultSet()  // Need mutex
{
  assert(m_availResultSets > 0);
  m_availResultSets--;

  assert(m_pendingRequests > 0);
  m_pendingRequests--;

  const Uint32 rootOp = rootOpNo();
  NdbResultStream &rootStream = getResultStream(rootOp);
  rootStream.prepareResultSet(m_preparedReceiveSet, m_activeScans);

  /* Position at the first (sorted?) row available from this worker.
   */
  rootStream.firstResult();
}

void NdbWorker::setConfReceived(Uint32 tcPtrI) {
  /* For a query with a lookup root, there may be more than one TCKEYCONF
     message. For a scan, there should only be one SCAN_TABCONF per
     worker result set.
  */
  const Uint32 rootOp = rootOpNo();
  assert(!getResultStream(rootOp).isScanQuery() || !m_confReceived);
  getResultStream(rootOp).getReceiver().m_tcPtrI = tcPtrI;
  m_confReceived = true;
}

bool NdbWorker::finalBatchReceived() const {
  return m_confReceived && getReceiverTcPtrI() == RNIL;
}

bool NdbWorker::isEmpty() const {
  return getResultStream(rootOpNo()).isEmpty();
}

/**
 * SPJ requests are identified by the receiver-id of the
 * *root* ResultStream for each NdbWorker. Furthermore
 * a NEXTREQ use the tcPtrI saved in this ResultStream to
 * identify the 'cursor' to restart.
 *
 * We provide some convenient accessors for fetching this info
 */
Uint32 NdbWorker::getReceiverId() const {
  return getResultStream(rootOpNo()).getReceiver().getId();
}

Uint32 NdbWorker::getReceiverTcPtrI() const {
  return getResultStream(rootOpNo()).getReceiver().m_tcPtrI;
}

///////////////////////////////////////////
/////////  NdbQuery API methods ///////////
///////////////////////////////////////////

NdbQuery::NdbQuery(NdbQueryImpl &impl) : m_impl(impl) {}

NdbQuery::~NdbQuery() {}

Uint32 NdbQuery::getNoOfOperations() const {
  return m_impl.getNoOfOperations();
}

NdbQueryOperation *NdbQuery::getQueryOperation(Uint32 index) const {
  return &m_impl.getQueryOperation(index).getInterface();
}

NdbQueryOperation *NdbQuery::getQueryOperation(const char *ident) const {
  NdbQueryOperationImpl *op = m_impl.getQueryOperation(ident);
  return (op) ? &op->getInterface() : nullptr;
}

int NdbQuery::setBound(const NdbRecord *keyRecord,
                       const NdbIndexScanOperation::IndexBound *bound) {
  const int error = m_impl.setBound(keyRecord, bound);
  if (unlikely(error)) {
    m_impl.setErrorCode(error);
    return -1;
  } else {
    return 0;
  }
}

int NdbQuery::getRangeNo() const { return m_impl.getRangeNo(); }

NdbQuery::NextResultOutcome NdbQuery::nextResult(bool fetchAllowed,
                                                 bool forceSend) {
  return m_impl.nextResult(fetchAllowed, forceSend);
}

void NdbQuery::close(bool forceSend) { m_impl.close(forceSend); }

NdbTransaction *NdbQuery::getNdbTransaction() const {
  return &m_impl.getNdbTransaction();
}

const NdbError &NdbQuery::getNdbError() const { return m_impl.getNdbError(); }

int NdbQuery::isPrunable(bool &prunable) const {
  return m_impl.isPrunable(prunable);
}

NdbAggregator *NdbQuery::getAggregator() const {
  return m_impl.getAggregator();
}

void NdbQueryImpl::execAggTRANSID_AI(const Uint32 *data, Uint32 len) {
  if (len == 0) return;
  m_aggResultOffsets.append(m_aggResultData.getSize());
  Uint32 *dst = m_aggResultData.alloc(len);
  if (likely(dst != nullptr)) {
    memcpy(dst, data, len * sizeof(Uint32));
  }
}

void NdbQueryImpl::execAggTRANSID_AI_frag(const Uint32 *data, Uint32 len,
                                           Uint32 fragInfo) {
  if (len == 0) return;
  if (fragInfo == 1) {
    // First fragment: record batch offset
    m_aggResultOffsets.append(m_aggResultData.getSize());
  }
  Uint32 *dst = m_aggResultData.alloc(len);
  if (likely(dst != nullptr)) {
    memcpy(dst, data, len * sizeof(Uint32));
  }
}

int NdbQueryImpl::processAggResults() {
  if (!m_hasAggregation || m_aggregator == nullptr) return 0;

  const Uint32 numBatches = m_aggResultOffsets.getSize();
#ifdef DEBUG_JOIN_AGG_TRACE
  const Uint32 totalWords = m_aggResultData.getSize();
  fprintf(stderr, "[AGG] processAggResults: %u batches, %u total words\n",
          numBatches, totalWords);
#endif
  for (Uint32 i = 0; i < numBatches; i++) {
    const Uint32 offset = m_aggResultOffsets.addr()[i];
    const Uint32 nextOffset = (i + 1 < numBatches)
                                  ? m_aggResultOffsets.addr()[i + 1]
                                  : m_aggResultData.getSize();
    const Uint32 batchLen = nextOffset - offset;
    const Uint32 *batchData = m_aggResultData.addr() + offset;

#ifdef DEBUG_JOIN_AGG_TRACE
    fprintf(stderr, "[AGG]   batch %u: offset=%u len=%u words:", i, offset, batchLen);
    for (Uint32 w = 0; w < batchLen && w < 16; w++) {
      fprintf(stderr, " %08x", batchData[w]);
    }
    fprintf(stderr, "\n");
#endif

    /**
     * Kernel sends [AttributeHeader, n_gb_cols|n_agg_results, ...].
     * Phase I.6 (F.2-K.5d): NdbAggregator::ProcessRes now reads the
     * marker word (AGG_RESULT vs AGG_CHAR_RESULT) itself, so pass
     * the buffer including the marker.
     */
    if (batchLen > 1) {
      m_aggregator->ProcessRes(
          const_cast<char *>(reinterpret_cast<const char *>(batchData)));
    }
  }
  m_aggregator->PrepareResults();
  return 0;
}

NdbQueryOperation::NdbQueryOperation(NdbQueryOperationImpl &impl)
    : m_impl(impl) {}
NdbQueryOperation::~NdbQueryOperation() {}

Uint32 NdbQueryOperation::getNoOfParentOperations() const {
  return m_impl.getNoOfParentOperations();
}

NdbQueryOperation *NdbQueryOperation::getParentOperation(Uint32 i) const {
  return &m_impl.getParentOperation(i).getInterface();
}

Uint32 NdbQueryOperation::getNoOfChildOperations() const {
  return m_impl.getNoOfChildOperations();
}

NdbQueryOperation *NdbQueryOperation::getChildOperation(Uint32 i) const {
  return &m_impl.getChildOperation(i).getInterface();
}

const NdbQueryOperationDef &NdbQueryOperation::getQueryOperationDef() const {
  return m_impl.getQueryOperationDef().getInterface();
}

NdbQuery &NdbQueryOperation::getQuery() const {
  return m_impl.getQuery().getInterface();
}

NdbRecAttr*
NdbQueryOperation::getValue(const char* anAttrName,
			    char* resultBuffer,
                            Uint32 aStartPos,
                            Uint32 aSize)
{
  return m_impl.getValue(anAttrName,
                         resultBuffer,
                         aStartPos,
                         aSize);
}

NdbRecAttr*
NdbQueryOperation::getValue(Uint32 anAttrId, 
			    char* resultBuffer,
                            Uint32 aStartPos,
                            Uint32 aSize)
{
  return m_impl.getValue(anAttrId,
                         resultBuffer,
                         aStartPos,
                         aSize);
}

NdbRecAttr*
NdbQueryOperation::getValue(const NdbDictionary::Column* column, 
			    char* resultBuffer,
                            Uint32 aStartPos,
                            Uint32 aSize)
{
  if (unlikely(column==nullptr)) {
    m_impl.getQuery().setErrorCode(QRY_REQ_ARG_IS_NULL);
    return nullptr;
  }
  return m_impl.getValue(NdbColumnImpl::getImpl(*column),
                         resultBuffer,
                         aStartPos,
                         aSize);
}

int NdbQueryOperation::setResultRowBuf(const NdbRecord *rec, char *resBuffer,
                                       const unsigned char *result_mask) {
  if (unlikely(rec == nullptr || resBuffer == nullptr)) {
    m_impl.getQuery().setErrorCode(QRY_REQ_ARG_IS_NULL);
    return -1;
  }
  return m_impl.setResultRowBuf(rec, resBuffer, result_mask);
}

int NdbQueryOperation::setResultRowRef(const NdbRecord *rec,
                                       const char *&bufRef,
                                       const unsigned char *result_mask) {
  return m_impl.setResultRowRef(rec, bufRef, result_mask);
}

int NdbQueryOperation::setOrdering(NdbQueryOptions::ScanOrdering ordering) {
  return m_impl.setOrdering(ordering);
}

NdbQueryOptions::ScanOrdering NdbQueryOperation::getOrdering() const {
  return m_impl.getOrdering();
}

int NdbQueryOperation::setParallelism(Uint32 parallelism) {
  return m_impl.setParallelism(parallelism);
}

int NdbQueryOperation::setMaxParallelism() {
  return m_impl.setMaxParallelism();
}

int NdbQueryOperation::setAdaptiveParallelism() {
  return m_impl.setAdaptiveParallelism();
}

int NdbQueryOperation::setBatchSize(Uint32 batchSize) {
  return m_impl.setBatchSize(batchSize);
}

int NdbQueryOperation::setInterpretedCode(
    const NdbInterpretedCode &code) const {
  return m_impl.setInterpretedCode(code);
}

NdbQuery::NextResultOutcome NdbQueryOperation::firstResult() {
  return m_impl.firstResult();
}

NdbQuery::NextResultOutcome NdbQueryOperation::nextResult(bool fetchAllowed,
                                                          bool forceSend) {
  return m_impl.nextResult(fetchAllowed, forceSend);
}

bool NdbQueryOperation::isRowNULL() const { return m_impl.isRowNULL(); }

/////////////////////////////////////////////////
/////////  NdbQueryParamValue methods ///////////
/////////////////////////////////////////////////

enum Type {
  Type_NULL,
  Type_raw,         // Raw data formatted according to bound Column format.
  Type_raw_shrink,  // As Type_raw, except short VarChar has to be shrunk.
  Type_string,      // '\0' terminated C-type string, char/varchar data only
  Type_Uint16,
  Type_Uint32,
  Type_Uint64,
  Type_Double
};

NdbQueryParamValue::NdbQueryParamValue(Uint16 val) : m_type(Type_Uint16) {
  m_value.uint16 = val;
}

NdbQueryParamValue::NdbQueryParamValue(Uint32 val) : m_type(Type_Uint32) {
  m_value.uint32 = val;
}

NdbQueryParamValue::NdbQueryParamValue(Uint64 val) : m_type(Type_Uint64) {
  m_value.uint64 = val;
}

NdbQueryParamValue::NdbQueryParamValue(double val) : m_type(Type_Double) {
  m_value.dbl = val;
}

// C-type string, terminated by '\0'
NdbQueryParamValue::NdbQueryParamValue(const char *val) : m_type(Type_string) {
  m_value.string = val;
}

// Raw data
NdbQueryParamValue::NdbQueryParamValue(const void *val, bool shrinkVarChar)
    : m_type(shrinkVarChar ? Type_raw_shrink : Type_raw) {
  m_value.raw = val;
}

// NULL-value, also used as optional end marker
NdbQueryParamValue::NdbQueryParamValue() : m_type(Type_NULL) {}

int NdbQueryParamValue::serializeValue(const class NdbColumnImpl &column,
                                       Uint32Buffer &dst, Uint32 &len,
                                       bool &isNull) const {
  const Uint32 maxSize = column.getSizeInBytes();
  isNull = false;
  // Start at (32-bit) word boundary.
  dst.skipRestOfWord();

  // Fetch parameter value and length.
  // Rudimentary typecheck of paramvalue: At least length should be as expected:
  //  - Extend with more types if required
  //  - Consider to add simple type conversion, ex: Int64 -> Int32
  //  - Or
  //     -- Represent all exact numeric as Int64 and convert to 'smaller' int
  //     -- Represent all floats as Double and convert to smaller floats
  //
  switch (m_type) {
    case Type_NULL:
      isNull = true;
      len = 0;
      break;

    case Type_Uint16:
      if (unlikely(column.getType() != NdbDictionary::Column::Smallint &&
                   column.getType() != NdbDictionary::Column::Smallunsigned))
        return QRY_PARAMETER_HAS_WRONG_TYPE;

      len = static_cast<Uint32>(sizeof(m_value.uint16));
      assert(len == maxSize);
      dst.appendBytes(&m_value.uint16, len);
      break;

    case Type_Uint32:
      if (unlikely(column.getType() != NdbDictionary::Column::Int &&
                   column.getType() != NdbDictionary::Column::Unsigned))
        return QRY_PARAMETER_HAS_WRONG_TYPE;

      len = static_cast<Uint32>(sizeof(m_value.uint32));
      assert(len == maxSize);
      dst.appendBytes(&m_value.uint32, len);
      break;

    case Type_Uint64:
      if (unlikely(column.getType() != NdbDictionary::Column::Bigint &&
                   column.getType() != NdbDictionary::Column::Bigunsigned))
        return QRY_PARAMETER_HAS_WRONG_TYPE;

      len = static_cast<Uint32>(sizeof(m_value.uint64));
      assert(len == maxSize);
      dst.appendBytes(&m_value.uint64, len);
      break;

    case Type_Double:
      if (unlikely(column.getType() != NdbDictionary::Column::Double))
        return QRY_PARAMETER_HAS_WRONG_TYPE;

      len = static_cast<Uint32>(sizeof(m_value.dbl));
      assert(len == maxSize);
      dst.appendBytes(&m_value.dbl, len);
      break;

    case Type_string:
      if (unlikely(column.getType() != NdbDictionary::Column::Char &&
                   column.getType() != NdbDictionary::Column::Varchar &&
                   column.getType() != NdbDictionary::Column::Longvarchar))
        return QRY_PARAMETER_HAS_WRONG_TYPE;
      {
        len = static_cast<Uint32>(strlen(m_value.string));
        if (unlikely(len > maxSize)) return QRY_CHAR_PARAMETER_TRUNCATED;

        dst.appendBytes(m_value.string, len);
      }
      break;

    case Type_raw:
      // 'Raw' data is readily formatted according to the bound column
      if (likely(column.m_arrayType == NDB_ARRAYTYPE_FIXED)) {
        len = maxSize;
        dst.appendBytes(m_value.raw, maxSize);
      } else if (column.m_arrayType == NDB_ARRAYTYPE_SHORT_VAR) {
        len = 1 + *((const Uint8 *)(m_value.raw));

        assert(column.getType() == NdbDictionary::Column::Varchar ||
               column.getType() == NdbDictionary::Column::Varbinary);
        if (unlikely(len > 1 + static_cast<Uint32>(column.getLength())))
          return QRY_CHAR_PARAMETER_TRUNCATED;

        dst.appendBytes(m_value.raw, len);
      } else if (column.m_arrayType == NDB_ARRAYTYPE_MEDIUM_VAR) {
        len = 2 + uint2korr((const Uint8 *)m_value.raw);

        assert(column.getType() == NdbDictionary::Column::Longvarchar ||
               column.getType() == NdbDictionary::Column::Longvarbinary);
        if (unlikely(len > 2 + static_cast<Uint32>(column.getLength())))
          return QRY_CHAR_PARAMETER_TRUNCATED;
        dst.appendBytes(m_value.raw, len);
      } else {
        assert(0);
      }
      break;

    case Type_raw_shrink:
      // Only short VarChar can be shrunk
      if (unlikely(column.m_arrayType != NDB_ARRAYTYPE_SHORT_VAR))
        return QRY_PARAMETER_HAS_WRONG_TYPE;

      assert(column.getType() == NdbDictionary::Column::Varchar ||
             column.getType() == NdbDictionary::Column::Varbinary);

      {
        // Convert from two-byte to one-byte length field.
        len = 1 + uint2korr((const Uint8 *)m_value.raw);
        assert(len <= 0x100);

        if (unlikely(len > 1 + static_cast<Uint32>(column.getLength())))
          return QRY_CHAR_PARAMETER_TRUNCATED;

        const Uint8 shortLen = static_cast<Uint8>(len - 1);
        dst.appendBytes(&shortLen, 1);
        dst.appendBytes(((const Uint8 *)m_value.raw) + 2, shortLen);
      }
      break;

    default:
      assert(false);
  }
  if (unlikely(dst.isMemoryExhausted())) {
    return Err_MemoryAlloc;
  }
  return 0;
}  // NdbQueryParamValue::serializeValue

///////////////////////////////////////////
/////////  NdbQueryImpl methods ///////////
///////////////////////////////////////////

NdbQueryImpl::NdbQueryImpl(NdbTransaction &trans,
                           const NdbQueryDefImpl &queryDef)
    : m_interface(*this),
      m_state(Initial),
      m_tcState(Inactive),
      m_next(nullptr),
      m_queryDef(&queryDef),
      m_error(),
      m_errorReceived(0),
      m_transaction(trans),
      m_scanTransaction(nullptr),
      m_operations(nullptr),
      m_countOperations(0),
      m_globalCursor(0),
      m_rootOpNo(0),
      m_pendingWorkers(0),
      m_workerCount(0),
      m_fragsPerWorker(0),
      m_workers(nullptr),
      m_applFrags(),
      m_finalWorkers(0),
      m_num_bounds(0),
      m_shortestBound(0xffffffff),
      m_attrInfo(),
      m_keyInfo(),
      m_hasAggregation(queryDef.hasAggregation()),
      m_aggregator(nullptr),
      m_aggProgram(),
      m_aggReceivers(nullptr),
      m_numAggReceivers(0),
      m_aggResultData(),
      m_aggResultOffsets(),
      m_startIndicator(false),
      m_commitIndicator(false),
      m_prunability(Prune_No),
      m_pruneHashVal(0),
      m_operationAlloc(sizeof(NdbQueryOperationImpl)),
      m_tupleSetAlloc(sizeof(NdbResultStream::TupleSet)),
      m_resultStreamAlloc(sizeof(NdbResultStream)),
      m_pointerAlloc(sizeof(void *)),
      m_rowBufferAlloc(sizeof(char)) {
  // Allocate memory for all m_operations[] in a single chunk
  m_countOperations = queryDef.getNoOfOperations();
  const int error = m_operationAlloc.init(m_countOperations);
  if (unlikely(error != 0)) {
    setErrorCode(error);
    return;
  }
  m_operations = reinterpret_cast<NdbQueryOperationImpl *>(
      m_operationAlloc.allocObjMem(m_countOperations));

  // Then; use placement new to construct each individual
  // NdbQueryOperationImpl object in m_operations
  for (Uint32 i = 0; i < m_countOperations; ++i) {
    const NdbQueryOperationDefImpl &def = queryDef.getQueryOperation(i);
    new (&m_operations[i]) NdbQueryOperationImpl(*this, def);
    // Failed to create NdbQueryOperationImpl object.
    if (m_error.code != 0) {
      // Destroy those objects that we have already constructed.
      for (int j = static_cast<int>(i) - 1; j >= 0; j--) {
        m_operations[j].~NdbQueryOperationImpl();
      }
      m_operations = nullptr;
      return;
    }
  }

  // Serialized QueryTree definition is first part of ATTRINFO.
  m_attrInfo.append(queryDef.getSerialized());
}

NdbQueryImpl::~NdbQueryImpl() {
  /** BEWARE:
   *  Don't refer NdbQueryDef or NdbQueryOperationDefs after
   *  NdbQuery::close() as at this stage the appliaction is
   *  allowed to destruct the Def's.
   */
  assert(m_state == Closed);
  assert(m_workers == nullptr);

  // NOTE: m_operations[] was allocated as a single memory chunk with
  // placement new construction of each operation.
  // Requires explicit call to d'tor of each operation before memory is free'ed.
  if (m_operations != nullptr) {
    for (int i = m_countOperations - 1; i >= 0; --i) {
      m_operations[i].~NdbQueryOperationImpl();
    }
    m_operations = nullptr;
  }
  m_state = Destructed;
}

void NdbQueryImpl::postFetchRelease() {
  if (m_workers != nullptr) {
    for (unsigned i = 0; i < m_workerCount; i++) {
      m_workers[i].postFetchRelease();
    }
  }
  if (m_operations != nullptr) {
    for (unsigned i = 0; i < m_countOperations; i++) {
      m_operations[i].postFetchRelease();
    }
  }
  delete[] m_workers;
  m_workers = nullptr;

  // Release aggregation resources
  if (m_aggReceivers != nullptr) {
    Ndb *const ndb = m_transaction.getNdb();
    for (Uint32 i = 0; i < m_numAggReceivers; i++) {
      m_aggReceivers[i]->release();
      ndb->releaseNdbScanRec(m_aggReceivers[i]);
    }
    delete[] m_aggReceivers;
    m_aggReceivers = nullptr;
    m_numAggReceivers = 0;
  }
  // Keep m_aggregator alive — user accesses it via getAggregator()
  // after scan completes. It is deleted in close().
  m_aggResultData.releaseExtend();
  m_aggResultOffsets.releaseExtend();

  m_rowBufferAlloc.reset();
  m_tupleSetAlloc.reset();
  m_resultStreamAlloc.reset();
}

// static
NdbQueryImpl *NdbQueryImpl::buildQuery(NdbTransaction &trans,
                                       const NdbQueryDefImpl &queryDef) {
  assert(queryDef.getNoOfOperations() > 0);
  NdbQueryImpl *const query = new NdbQueryImpl(trans, queryDef);
  if (unlikely(query == nullptr)) {
    trans.setOperationErrorCodeAbort(Err_MemoryAlloc);
    return nullptr;
  }
  if (unlikely(query->m_error.code != 0)) {
    // Transaction error code set already.
    query->release();
    return nullptr;
  }
  assert(query->m_state == Initial);
  return query;
}

/** Assign supplied parameter values to the parameter placeholders
 *  Created when the query was defined.
 *  Values are *copied* into this NdbQueryImpl object:
 *  Memory location used as source for parameter values don't have
 *  to be valid after this assignment.
 */
int NdbQueryImpl::assignParameters(const NdbQueryParamValue paramValues[]) {
  /**
   * Immediately build the serialized parameter representation in order
   * to avoid storing param values elsewhere until query is executed.
   * Also calculates prunable property, and possibly its hashValue.
   */
  // Build explicit key/filter/bounds for root operation, possibly referring
  // paramValues
  const int error = getRoot().prepareKeyInfo(m_keyInfo, paramValues);
  if (unlikely(error != 0)) {
    setErrorCode(error);
    return -1;
  }

  // Serialize parameter values for the other (non-root) operations
  // (No need to serialize for root (i==0) as root key is part of keyInfo above)
  for (Uint32 i = 1; i < getNoOfOperations(); ++i) {
    if (getQueryDef().getQueryOperation(i).getNoOfParameters() > 0) {
      const int error = getQueryOperation(i).serializeParams(paramValues);
      if (unlikely(error != 0)) {
        setErrorCode(error);
        return -1;
      }
    }
  }
  assert(m_state < Defined);
  m_state = Defined;
  return 0;
}  // NdbQueryImpl::assignParameters

static int insert_bound(Uint32Buffer &keyInfo, const NdbRecord *key_record,
                        Uint32 column_index, const char *row,
                        Uint32 bound_type) {
  char buf[NdbRecord::Attr::SHRINK_VARCHAR_BUFFSIZE];
  const NdbRecord::Attr *column = &key_record->columns[column_index];

  bool is_null = column->is_null(row);
  Uint32 len = 0;
  const void *aValue = row + column->offset;

  if (!is_null) {
    bool len_ok;
    /* Support for special mysqld varchar format in keys. */
    if (column->flags & NdbRecord::IsMysqldShrinkVarchar) {
      len_ok = column->shrink_varchar(row, len, buf);
      aValue = buf;
    } else {
      len_ok = column->get_var_length(row, len);
    }
    if (!len_ok) {
      return Err_WrongFieldLength;
    }
  }

  AttributeHeader ah(column->index_attrId, len);
  keyInfo.append(bound_type);
  keyInfo.append(ah.m_value);
  keyInfo.appendBytes(aValue, len);

  return 0;
}

int NdbQueryImpl::setBound(const NdbRecord *key_record,
                           const NdbIndexScanOperation::IndexBound *bound) {
  m_prunability = Prune_Unknown;
  if (unlikely(key_record == nullptr || bound == nullptr))
    return QRY_REQ_ARG_IS_NULL;

  if (unlikely(getRoot().getQueryOperationDef().getType() !=
               NdbQueryOperationDef::OrderedIndexScan)) {
    return QRY_WRONG_OPERATION_TYPE;
  }

  assert(m_state >= Defined);
  if (m_state != Defined) {
    return QRY_ILLEGAL_STATE;
  }

  int startPos = m_keyInfo.getSize();

  // We don't handle both NdbQueryIndexBound defined in ::scanIndex()
  // in combination with a later ::setBound(NdbIndexScanOperation::IndexBound)
  // assert (m_bound.lowKeys==0 && m_bound.highKeys==0);

  if (unlikely(bound->range_no != m_num_bounds ||
               bound->range_no > NdbIndexScanOperation::MaxRangeNo)) {
    // setErrorCodeAbort(4286);
    return Err_InvalidRangeNo;
  }

  Uint32 key_count = bound->low_key_count;
  Uint32 common_key_count = key_count;
  if (key_count < bound->high_key_count)
    key_count = bound->high_key_count;
  else
    common_key_count = bound->high_key_count;

  if (m_shortestBound > common_key_count) {
    m_shortestBound = common_key_count;
  }
  /* Has the user supplied an open range (no bounds)? */
  const bool openRange =
      ((bound->low_key == nullptr || bound->low_key_count == 0) &&
       (bound->high_key == nullptr || bound->high_key_count == 0));
  if (likely(!openRange)) {
    /* If low and high key pointers are the same and key counts are
     * the same, we send as an Eq bound to save bandwidth.
     * This will not send an EQ bound if :
     *   - Different numbers of high and low keys are EQ
     *   - High and low keys are EQ, but use different ptrs
     */
    const bool isEqRange =
        (bound->low_key == bound->high_key) &&
        (bound->low_key_count == bound->high_key_count) &&
        (bound->low_inclusive && bound->high_inclusive);  // Does this matter?

    if (isEqRange) {
      /* Using BoundEQ will result in bound being sent only once */
      for (unsigned j = 0; j < key_count; j++) {
        const int error =
            insert_bound(m_keyInfo, key_record, key_record->key_indexes[j],
                         bound->low_key, NdbIndexScanOperation::BoundEQ);
        if (unlikely(error)) return error;
      }
    } else {
      /* Distinct upper and lower bounds, must specify them independently */
      /* Note :  Protocol allows individual columns to be specified as EQ
       * or some prefix of columns.  This is not currently supported from
       * NDBAPI.
       */
      for (unsigned j = 0; j < key_count; j++) {
        Uint32 bound_type;
        /* If key is part of lower bound */
        if (bound->low_key && j < bound->low_key_count) {
          /* Inclusive if defined, or matching rows can include this value */
          bound_type = bound->low_inclusive || j + 1 < bound->low_key_count
                           ? NdbIndexScanOperation::BoundLE
                           : NdbIndexScanOperation::BoundLT;
          const int error =
              insert_bound(m_keyInfo, key_record, key_record->key_indexes[j],
                           bound->low_key, bound_type);
          if (unlikely(error)) return error;
        }
        /* If key is part of upper bound */
        if (bound->high_key && j < bound->high_key_count) {
          /* Inclusive if defined, or matching rows can include this value */
          bound_type = bound->high_inclusive || j + 1 < bound->high_key_count
                           ? NdbIndexScanOperation::BoundGE
                           : NdbIndexScanOperation::BoundGT;
          const int error =
              insert_bound(m_keyInfo, key_record, key_record->key_indexes[j],
                           bound->high_key, bound_type);
          if (unlikely(error)) return error;
        }
      }
    }
  } else {
    /* Open range - all rows must be returned.
     * To encode this, we'll request all rows where the first
     * key column value is >= NULL
     */
    AttributeHeader ah(0, 0);
    m_keyInfo.append(NdbIndexScanOperation::BoundLE);
    m_keyInfo.append(ah.m_value);
  }

  Uint32 length = m_keyInfo.getSize() - startPos;
  if (unlikely(m_keyInfo.isMemoryExhausted())) {
    return Err_MemoryAlloc;
  } else if (unlikely(length > 0xFFFF)) {
    return QRY_DEFINITION_TOO_LARGE;  // Query definition too large.
  } else if (likely(length > 0)) {
    m_keyInfo.put(startPos, m_keyInfo.get(startPos) | (length << 16) |
                                (bound->range_no << 4));
  }

#ifdef TRACE_SERIALIZATION
  ndbout << "Serialized KEYINFO w/ bounds for indexScan root : ";
  for (Uint32 i = startPos; i < m_keyInfo.getSize(); i++) {
    char buf[12];
    sprintf(buf, "%.8x", m_keyInfo.get(i));
    ndbout << buf << " ";
  }
  ndbout << endl;
#endif

  m_num_bounds++;
  return 0;
}  // NdbQueryImpl::setBound()

int NdbQueryImpl::getRangeNo() const {
  const NdbWorker *worker = m_applFrags.getCurrent();
  if (worker != nullptr) {
    const int range_no = worker->getResultStream(m_rootOpNo).getCurrentRangeNo();
    if (range_no >= 0) return range_no;
    assert(!needRangeNo());
  }
  return 0;
}

Uint32 NdbQueryImpl::getNoOfOperations() const { return m_countOperations; }

Uint32 NdbQueryImpl::getNoOfLeafOperations() const {
  return getRoot().getNoOfLeafOperations();
}

NdbQueryOperationImpl &NdbQueryImpl::getQueryOperation(Uint32 index) const {
  assert(index < m_countOperations);
  return m_operations[index];
}

NdbQueryOperationImpl *NdbQueryImpl::getQueryOperation(
    const char *ident) const {
  for (Uint32 i = 0; i < m_countOperations; i++) {
    if (strcmp(m_operations[i].getQueryOperationDef().getName(), ident) == 0) {
      return &m_operations[i];
    }
  }
  return nullptr;
}

/**
 * NdbQueryImpl::nextResult() - The 'global' cursor on the query results
 *
 * Will iterate and fetch results for all combinations of results from the
 * NdbOperations which this query consists of. Except for the root operations
 * which will follow any optional ScanOrdering, we have no control of the
 * ordering which the results from the QueryOperations appear in.
 */

NdbQuery::NextResultOutcome NdbQueryImpl::nextResult(bool fetchAllowed,
                                                     bool forceSend) {
  if (unlikely(m_state < Executing || m_state >= Closed)) {
    assert(m_state >= Initial && m_state < Destructed);
    if (m_state == Failed)
      setErrorCode(QRY_IN_ERROR_STATE);
    else
      setErrorCode(QRY_ILLEGAL_STATE);
    DEBUG_CRASH();
    return NdbQuery::NextResult_error;
  }

  assert(m_globalCursor < getNoOfOperations());

  while (m_state != EndOfData)  // Or likely:  return when 'gotRow'
  {
    DEB_CTE_API("nextResult: cursor=%u state=%d rootOpNo=%u\n",
                m_globalCursor, (int)m_state, getRootOpNo());
    NdbQuery::NextResultOutcome res =
        getQueryOperation(m_globalCursor).nextResult(fetchAllowed, forceSend);

    DEB_CTE_API("nextResult: cursor=%u res=%d\n", m_globalCursor, (int)res);

    if (unlikely(res == NdbQuery::NextResult_error))
      return res;

    else if (res == NdbQuery::NextResult_scanComplete) {
      if (m_globalCursor <= getRootOpNo())  // Completed reading all from root
        break;
      m_globalCursor--;  // Get 'next' from ancestor
      // Skip CTE-embedded operations when walking back
      while (m_globalCursor > getRootOpNo()) {
        const NdbQueryOperationDefImpl &d =
            getQueryOperation(m_globalCursor).getQueryOperationDef();
        if (d.getType() != NdbQueryOperationDef::CteSubtree &&
            !d.isCteEmbedded())
          break;
        m_globalCursor--;
      }
    }

    else if (res == NdbQuery::NextResult_gotRow) {
      // Position to 'firstResult()' for all children.
      // Update m_globalCursor to iterate from last operation with results next
      // time
      //
      for (uint child = m_globalCursor + 1; child < getNoOfOperations();
           child++) {
        // Skip CTE-embedded operations in the result iteration
        const NdbQueryOperationDefImpl &cDef =
            getQueryOperation(child).getQueryOperationDef();
        if (cDef.getType() == NdbQueryOperationDef::CteSubtree ||
            cDef.isCteEmbedded())
          continue;
        res = getQueryOperation(child).firstResult();
        if (unlikely(res == NdbQuery::NextResult_error))
          return res;
        else if (res == NdbQuery::NextResult_gotRow)
          m_globalCursor = child;
      }
      return NdbQuery::NextResult_gotRow;
    } else {
      assert(res == NdbQuery::NextResult_bufferEmpty);
      return res;
    }
  }

  assert(m_state == EndOfData);
  return NdbQuery::NextResult_scanComplete;

}  // NdbQueryImpl::nextResult()

/**
 * Local cursor component which implements the special case of 'next' on the
 * root operation of entire NdbQuery. In addition to fetch 'next' result from
 * the root operation, we should also retrieve more results from the datanodes
 * if required and allowed.
 */
NdbQuery::NextResultOutcome NdbQueryImpl::nextRootResult(bool fetchAllowed,
                                                         bool forceSend) {
  /* To minimize lock contention, each query has the separate NdbWorker
   * container 'm_applFrags'. m_applFrags is only accessed by the application
   * thread, so it is safe to use it without locks.
   */
  while (m_state != EndOfData)  // Or likely:  return when 'gotRow' or error
  {
    const NdbWorker *worker = m_applFrags.getCurrent();
    if (unlikely(worker == nullptr)) {
      /* m_applFrags is empty, so we cannot get more results without
       * possibly blocking.
       *
       * ::awaitMoreResults() will either copy worker results that are already
       * complete (under mutex protection), or block until data
       * previously requested arrives.
       */
      DEB_CTE_API("nextRootResult: awaitMoreResults (blocking)\n");
      const FetchResult fetchResult = awaitMoreResults(forceSend);
      switch (fetchResult) {
        case FetchResult_ok:  // OK - got data wo/ error
          assert(m_state != Failed);
          worker = m_applFrags.getCurrent();
          assert(worker != nullptr);
          break;

        case FetchResult_noMoreData:  // No data, no error
          assert(m_state != Failed);
          assert(m_applFrags.getCurrent() == nullptr);
          getRoot().nullifyResult();
          m_state = EndOfData;
          if (m_hasAggregation) {
            processAggResults();
          }
          postFetchRelease();
          return NdbQuery::NextResult_scanComplete;

        case FetchResult_noMoreCache:  // No cached data, no error
          assert(m_state != Failed);
          assert(m_applFrags.getCurrent() == nullptr);
          getRoot().nullifyResult();
          if (fetchAllowed) {
            break;  // ::sendFetchMore() may request more results
          }
          return NdbQuery::NextResult_bufferEmpty;

        case FetchResult_gotError:  // Error in 'm_error.code'
          assert(m_error.code != 0);
          return NdbQuery::NextResult_error;

        default:
          assert(false);
      }
    } else {
      worker->getResultStream(m_rootOpNo).nextResult();  // Consume current
      m_applFrags.reorganize();                 // Calculate new current
      // ::reorganize(). may update 'current' worker.
      worker = m_applFrags.getCurrent();
    }

    /**
     * If allowed to request more rows from datanodes, we do this asynch
     * and request more rows as soon as we have consumed all rows from a
     * SPJ-worker. ::awaitMoreResults() may eventually block and wait for
     * these when required.
     */
    if (fetchAllowed) {
      // Ask for a new batch if we emptied some.
      NdbWorker **workers;
      const Uint32 cnt = m_applFrags.getFetchMore(workers);
      if (cnt > 0 && sendFetchMore(workers, cnt, forceSend) != 0) {
        return NdbQuery::NextResult_error;
      }
    }

    if (worker != nullptr) {
      if (unlikely(getRoot().fetchRow(
              worker->getResultStream(m_rootOpNo)) == -1))
        return NdbQuery::NextResult_error;
      return NdbQuery::NextResult_gotRow;
    }
  }  // m_state != EndOfData

  assert(m_state == EndOfData);
  return NdbQuery::NextResult_scanComplete;
}  // NdbQueryImpl::nextRootResult()

/**
 * Wait for more scan results which already has been REQuested to arrive.
 * @return 0 if some rows did arrive, a negative value if there are errors (in
 * m_error.code), and 1 of there are no more rows to receive.
 */
NdbQueryImpl::FetchResult NdbQueryImpl::awaitMoreResults(bool forceSend) {
  assert(m_applFrags.getCurrent() == nullptr);

  /* Check if there are any more completed fragments available.*/
  if (getQueryDef().isScanQuery()) {
    assert(m_scanTransaction);
    assert(m_state == Executing);

    NdbImpl *const ndb = m_transaction.getNdb()->theImpl;
    {
      /* This part needs to be done under mutex due to synchronization with
       * receiver thread.
       */
      PollGuard poll_guard(*ndb);

      /* There may be pending (asynchronous received, mutex protected) errors
       * from TC / datanodes. Propagate these into m_error.code in 'API space'.
       */
      while (likely(!hasReceivedError())) {
        /* Scan m_workers (under mutex protection) for workers
         * which has delivered a complete batch. Add these to m_applFrags.
         */
        m_applFrags.prepareMoreResults(m_workers, m_workerCount);
        if (m_applFrags.getCurrent() != nullptr) {
          return FetchResult_ok;
        }

        /* There are no more available worker results available without
         * first waiting for more to be received from the datanodes
         */
        if (m_pendingWorkers == 0) {
          // 'No more *pending* results', ::sendFetchMore() may make more
          // available
          return (m_finalWorkers < getWorkerCount()) ? FetchResult_noMoreCache
                                                     : FetchResult_noMoreData;
        }

        const Uint32 timeout = ndb->get_waitfor_timeout();
        const Uint32 nodeId = m_transaction.getConnectedNodeId();
        const Uint32 seq = m_transaction.theNodeSequence;

        /* More results are on the way, so we wait for them.*/
        const FetchResult waitResult = static_cast<FetchResult>
          (poll_guard.wait_scan(3*timeout, 
                                nodeId, 
                                forceSend,
                                &ndb->m_start_time));

        if (ndb->getNodeSequence(nodeId) != seq)
          setFetchTerminated(Err_NodeFailCausedAbort, false);
        else if (likely(waitResult == FetchResult_ok))
          continue;
        else if (waitResult == FetchResult_timeOut)
          setFetchTerminated(Err_ReceiveTimedOut, true);
        else
          setFetchTerminated(Err_NodeFailCausedAbort, false);

        assert(m_state != Failed);
      }  // while(!hasReceivedError())
    }    // Terminates scope of 'PollGuard'

    // Fall through only if ::hasReceivedError()
    assert(m_error.code);
    return FetchResult_gotError;
  } else  // is a Lookup query
  {
    /* The root operation is a lookup. Lookups are guaranteed to be complete
     * before NdbTransaction::execute() returns. Therefore we do not set
     * the lock, because we know that the signal receiver thread will not
     * be accessing m_workers at this time.
     */
    m_applFrags.prepareMoreResults(m_workers, m_workerCount);
    if (m_applFrags.getCurrent() != nullptr) {
      return FetchResult_ok;
    }

    /* Getting here means that either:
     *  - No results was returned (TCKEYREF)
     *  - There was no matching row for an inner join.
     *  - or, the application called nextResult() twice for a lookup query.
     */
    assert(m_pendingWorkers == 0);
    assert(m_finalWorkers == getWorkerCount());
    return FetchResult_noMoreData;
  }  // if(getQueryDef().isScanQuery())

}  // NdbQueryImpl::awaitMoreResults

/*
  ::handleBatchComplete() is intended to be called when receiving signals only.
  The PollGuard mutex is then set and the shared 'm_pendingWorkers' and
  'm_finalWorkers' can safely be updated and ::setReceivedMore() signaled.

  returns: 'true' when application thread should be resumed.
*/
bool NdbQueryImpl::handleBatchComplete(NdbWorker &worker) {
  if (traceSignals) {
    ndbout << "NdbQueryImpl::handleBatchComplete"
           << ", from workerNo=" << worker.getWorkerNo()
           << ", pendingWorkers=" << (m_pendingWorkers - 1)
           << ", finalWorkers=" << m_finalWorkers << endl;
  }
  assert(worker.isFragBatchComplete());

  /* May received SPJ results after a SCANREF() (timeout?)
   * terminated the scan.  We are about to close this query,
   * and didn't expect any more data - ignore it!
   */
  if (likely(m_errorReceived == 0)) {
    assert(m_pendingWorkers > 0);               // Check against underflow.
    assert(m_pendingWorkers <= m_workerCount);  // .... and overflow
    m_pendingWorkers--;

    if (worker.finalBatchReceived()) {
      m_finalWorkers++;
      assert(m_finalWorkers <= m_workerCount);
    }

    /* When application thread ::awaitMoreResults() it will later be
     * added to m_applFrags under mutex protection.
     */
    worker.setReceivedMore();
    return true;
  } else if (!getQueryDef().isScanQuery())  // A failed lookup query
  {
    /**
     * A lookup query will retrieve the rows as part of ::execute().
     * -> Error must be visible through API before we return control
     *    to the application.
     */
    setErrorCode(m_errorReceived);
    return true;
  }

  return false;
}  // NdbQueryImpl::handleBatchComplete

int NdbQueryImpl::close(bool forceSend) {
  int res = 0;

  assert(m_state >= Initial && m_state < Destructed);
  if (m_state != Closed) {
    if (m_tcState != Inactive) {
      /* We have started a scan, but we have not yet received the last batch
       * from all SPJ-workers. We must therefore close the scan to release
       * the scan context at TC/SPJ.*/
      res = closeTcCursor(forceSend);
    }

    // Throw any pending results
    NdbWorker::clear(m_workers, m_workerCount);
    m_applFrags.clear();

    Ndb *const ndb = m_transaction.getNdb();
    if (m_scanTransaction != nullptr) {
      assert(m_state != Closed);
      assert(m_scanTransaction->m_scanningQuery == this);
      m_scanTransaction->m_scanningQuery = nullptr;
      ndb->closeTransaction(m_scanTransaction);
      ndb->theRemainingStartTransactions--;  // Compensate; m_scanTransaction
                                             // was not a real Txn
      m_scanTransaction = nullptr;
    }

    postFetchRelease();
    m_state = Closed;  // Even if it was previously 'Failed' it is closed now!
  }

  // Delete aggregator after close — user may have accessed it via
  // getAggregator() between scan completion and close().
  delete m_aggregator;
  m_aggregator = nullptr;

  /** BEWARE:
   *  Don't refer NdbQueryDef or its NdbQueryOperationDefs after ::close()
   *  as the application is allowed to destruct the Def's after this point.
   */
  m_queryDef = nullptr;

  return res;
}  // NdbQueryImpl::close

void NdbQueryImpl::release() {
  assert(m_state >= Initial && m_state < Destructed);
  if (m_state != Closed) {
    close(true);  // Ignore any errors, explicit ::close() first if errors are
                  // of interest
  }

  delete this;
}

void NdbQueryImpl::setErrorCode(int aErrorCode) {
  assert(aErrorCode != 0);
  m_error.code = aErrorCode;
  m_transaction.theErrorLine = 0;
  m_transaction.theErrorOperation = nullptr;

  switch (aErrorCode) {
      // Not really an error. A root lookup found no match.
    case Err_TupleNotFound:
      // Simple or dirty read failed due to node failure. Transaction will be
      // aborted.
    case Err_SimpleDirtyReadFailed:
      m_transaction.setOperationErrorCode(aErrorCode);
      break;

      // For any other error, abort the transaction.
    default:
      m_state = Failed;
      m_transaction.setOperationErrorCodeAbort(aErrorCode);
      break;
  }
}

/*
 * ::setFetchTerminated() Should only be called with mutex locked.
 * Register result fetching as completed (possibly prematurely, w/ errorCode).
 */
void NdbQueryImpl::setFetchTerminated(int errorCode, bool needClose) {
  assert(m_finalWorkers < getWorkerCount());
  if (!needClose) {
    m_finalWorkers = getWorkerCount();
  }
  if (errorCode != 0) {
    m_errorReceived = errorCode;
  }
  m_pendingWorkers = 0;
}  // NdbQueryImpl::setFetchTerminated()

/* There may be pending (asynchronous received, mutex protected) errors
 * from TC / datanodes. Propagate these into 'API space'.
 * ::hasReceivedError() Should only be called with mutex locked
 */
bool NdbQueryImpl::hasReceivedError() {
  if (unlikely(m_errorReceived)) {
    setErrorCode(m_errorReceived);
    return true;
  }
  return false;
}  // NdbQueryImpl::hasReceivedError

bool NdbQueryImpl::execTCKEYCONF() {
  if (traceSignals) {
    ndbout << "NdbQueryImpl::execTCKEYCONF()" << endl;
  }
  assert(!getQueryDef().isScanQuery());
  NdbWorker &worker = m_workers[0];

  // We will get 1 + #leaf-nodes TCKEYCONF for a lookup...
  worker.setConfReceived(RNIL);
  if (unlikely(!worker.incrOutstandingResults(-1))) {
    setFetchTerminated(Err_OutstandingResultsMismatch, false);
    return false;
  }

  bool ret = false;
  if (worker.isFragBatchComplete()) {
    ret = handleBatchComplete(worker);
  }

  if (traceSignals) {
    ndbout << "NdbQueryImpl::execTCKEYCONF(): returns:" << ret
           << ", m_pendingWorkers=" << m_pendingWorkers << ", rootStream= {"
           << worker.getResultStream(m_rootOpNo) << "}" << endl;
  }
  return ret;
}  // NdbQueryImpl::execTCKEYCONF

void NdbQueryImpl::execCLOSE_SCAN_REP(int errorCode, bool needClose) {
  if (traceSignals) {
    ndbout << "NdbQueryImpl::execCLOSE_SCAN_REP()" << endl;
  }
  setFetchTerminated(errorCode, needClose);
}

NdbQueryOperationImpl &NdbQueryImpl::getRoot() const {
  for (Uint32 i = 0; i < m_countOperations; i++) {
    const NdbQueryOperationDefImpl &def =
        m_operations[i].getQueryOperationDef();
    if (def.getType() != NdbQueryOperationDef::CteSubtree &&
        !def.isCteEmbedded()) {
      return m_operations[i];
    }
  }
  return m_operations[0];
}

Uint32 NdbQueryImpl::getRootOpNo() const {
  for (Uint32 i = 0; i < m_countOperations; i++) {
    const NdbQueryOperationDefImpl &def =
        m_operations[i].getQueryOperationDef();
    if (def.getType() != NdbQueryOperationDef::CteSubtree &&
        !def.isCteEmbedded()) {
      return def.getOpNo();
    }
  }
  return 0;
}

const NdbTableImpl &NdbQueryImpl::getFragRoutingTable() const {
  const NdbQueryOperationDefImpl &rootDef =
      getRoot().getQueryOperationDef();
  const NdbTableImpl *table =
      rootDef.getIndex() ? rootDef.getIndex()->getIndexTable()
                         : &rootDef.getTable();
  // lookupCte: not a scan operation at all → no table.
  // scanCte over synthetic virt table: scan op but FragmentCount==0.
  // In both cases, fall back to the first CTE-embedded scan's table —
  // that's the real source whose fragment topology DBTC needs to
  // route SCAN_FRAGREQ correctly.  scanCte over a registered virt
  // table (existing testCteNdbApi pattern) has FragmentCount > 0
  // and the fallback is skipped.
  const bool rootIsCteScan =
      (rootDef.getType() == NdbQueryOperationDef::CteScan);
  const bool needOverride =
      !rootDef.isScanOperation() ||
      (rootIsCteScan &&
       static_cast<const NdbDictionary::Table &>(*table)
           .getFragmentCount() == 0);
  if (needOverride) {
    for (Uint32 i = 0; i < m_countOperations; i++) {
      const NdbQueryOperationDefImpl &opDef =
          m_operations[i].getQueryOperationDef();
      if (opDef.isCteEmbedded() && opDef.isScanOperation()) {
        table = &opDef.getTable();
        break;
      }
    }
  }
  return *table;
}

int NdbQueryImpl::prepareSend() {
  if (unlikely(m_state != Defined)) {
    assert(m_state >= Initial && m_state < Destructed);
    if (m_state == Failed)
      setErrorCode(QRY_IN_ERROR_STATE);
    else
      setErrorCode(QRY_ILLEGAL_STATE);
    DEBUG_CRASH();
    return -1;
  }

  // Determine execution parameters 'batch size'.
  // May be user specified (TODO), and/or,  limited/specified by config values
  //
  Uint32 rootFragments;
  if (getQueryDef().isScanQuery()) {
    NdbQueryOperationImpl &rootOp = getRoot();
    const NdbDictionary::Table &rootTable =
        static_cast<const NdbDictionary::Table &>(getFragRoutingTable());

    /* For CTE compound queries the root scan is not op[0] — CTE subtree
     * containers precede it. The constructor initializes m_parallelism
     * based on opNo==0, giving the actual root Parallelism_adaptive
     * instead of Parallelism_max. This must be corrected so that
     * SFP_PARALLEL is set in the scan parameters, ensuring DBSPJ starts
     * all fragments immediately rather than using adaptive parallelism
     * (which would stall waiting for SCAN_NEXTREQ that the API won't
     * send until all fragments report). */
    if (rootOp.m_parallelism == Parallelism_adaptive &&
        rootOp.getQueryOperationDef().getOpNo() != 0) {
      rootOp.m_parallelism = Parallelism_max;
    }

    rootFragments = rootTable.getFragmentCount();
    DEB_CTE_API("prepareSend: rootOpNo=%u rootFragments=%u parallelism=0x%x "
                "hasAgg=%d numOps=%u\n",
                rootOp.getQueryOperationDef().getOpNo(),
                rootFragments, rootOp.m_parallelism,
                m_hasAggregation, getNoOfOperations());
    if (rootFragments == 0) {
      // No fragments - should never happen
      setErrorCode(QRY_TABLE_HAVE_NO_FRAGMENTS);
      DEBUG_CRASH();
      return -1;
    }

    /* For the first batch, we read from all fragments for both ordered
     * and unordered scans.*/
    if (getQueryOperation(0U).m_parallelism != Parallelism_max) {
      const Uint32 parallelism = getRoot().m_parallelism;
      require(parallelism > 0);  // NdbQueryOperationImpl invariant
      assert(parallelism != Parallelism_adaptive);
      rootFragments = MIN(rootFragments, parallelism);
    }

    bool pruned = false;
    const int error = isPrunable(pruned);
    if (unlikely(error != 0)) {
      setErrorCode(error);
      return -1;
    }
    /**
     * A 'pruned scan' will only be sent to the single fragment identified
     * by the partition key.
     */
    if (pruned) {
      // Scan pruned to single fragment
      rootFragments = 1;
      m_fragsPerWorker = 1;
    } else if (rootOp.getOrdering() !=
               NdbQueryOptions::ScanOrdering_unordered) {
      // Merge-sort need one result set from each fragment
      m_fragsPerWorker = 1;
    } else if (!ndbd_spj_multifrag_scan(
                   m_transaction.getNdb()->getMinDbNodeVersion())) {
      // 'MultiFragment' not supported by all datanodes, partially upgraded?
      m_fragsPerWorker = 1;
    } else if (m_hasAggregation) {
      // Aggregate queries: each fragment gets its own SCAN_FRAGREQ to a
      // separate DBSPJ instance for maximum parallelism across TC threads.
      // Multi-fragment bundling would send all fragments on a node to one
      // DBSPJ instance, serializing the work. Since aggregation results
      // are per-node (not per-fragment), the extra API round-trips that
      // multi-fragment workers avoid are not a concern.
      m_fragsPerWorker = 1;
    } else {
      NdbNodeBitmask dataNodes;
      Uint32 cnt = 0;

      // Count number of nodes 'rootTable' is distributed over.
      for (Uint32 i = 0; i < rootFragments; i++) {
        Uint32 nodes[1];
        const Uint32 res =
            rootTable.getFragmentNodes(i, nodes, NDB_ARRAY_SIZE(nodes));
        assert(res > 0);
        if (res == 0) {
          // Fragment without node, should never happen
          setErrorCode(QRY_BAD_FRAGMENT_DATA);
          DEBUG_CRASH();
          return -1;
        }
        if (!dataNodes.get(nodes[0])) {
          dataNodes.set(nodes[0]);
          cnt++;
        }
      }
      require(cnt > 0);
      assert((rootFragments % cnt) == 0);
      m_fragsPerWorker = rootFragments / cnt;
    }

    /** Scan operations need a own sub-transaction object associated with each
     *  query.
     */
    Ndb *const ndb = m_transaction.getNdb();
    ndb->theRemainingStartTransactions++;  // Compensate; does not start a real
                                           // Txn
    NdbTransaction *scanTxn = ndb->hupp(&m_transaction);
    if (scanTxn == nullptr) {
      ndb->theRemainingStartTransactions--;
      m_transaction.setOperationErrorCodeAbort(ndb->getNdbError().code);
      return -1;
    }
    scanTxn->theMagicNumber = 0x37412619;
    scanTxn->m_scanningQuery = this;
    this->m_scanTransaction = scanTxn;
  } else  // Lookup query
  {
    rootFragments = 1;
    m_fragsPerWorker = 1;
  }
  m_workerCount = rootFragments / m_fragsPerWorker;
  assert(m_workerCount > 0);

  int error = m_resultStreamAlloc.init(m_workerCount * getNoOfOperations());
  if (error != 0) {
    setErrorCode(error);
    return -1;
  }
  // Allocate space for ptrs to NdbResultStream and NdbWorker objects.
  error =
      m_pointerAlloc.init(m_workerCount * (OrderedFragSet::pointersPerWorker));
  if (error != 0) {
    setErrorCode(error);
    return -1;
  }

  // Some preparation for later batchsize calculations pr. (sub) scan
  DEB_CTE_API("prepareSend: before calculateBatchedRows, "
              "workerCount=%u fragsPerWorker=%u\n",
              m_workerCount, m_fragsPerWorker);
  getRoot().calculateBatchedRows(nullptr);
  getRoot().setBatchedRows(1);
  DEB_CTE_API("prepareSend: after calculateBatchedRows, "
              "rootMaxBatchRows=%u\n", getRoot().getMaxBatchRows());

  /**
   * Calculate total amount of row buffer space for all operations and
   * fragments.
   */
  Uint32 totalBuffSize = 0;
  for (Uint32 opNo = 0; opNo < getNoOfOperations(); opNo++) {
    const NdbQueryOperationImpl &op = getQueryOperation(opNo);

    // Add space for batchBuffer & m_correlations
    Uint32 opBuffSize = op.getResultBufferSize();
    if (getQueryDef().isScanQuery()) {
      opBuffSize += (sizeof(TupleCorrelation) * op.getMaxBatchRows());
      opBuffSize *= 2;  // Scans are double buffered
    }
    opBuffSize += op.getRowSize();  // Unpacked row from buffers
    totalBuffSize += opBuffSize;
  }
  DEB_CTE_API("prepareSend: totalBuffSize=%u rootFragments=%u "
              "allocating=%u\n",
              totalBuffSize, rootFragments,
              rootFragments * totalBuffSize);
  m_rowBufferAlloc.init(rootFragments * totalBuffSize);

  if (getQueryDef().isScanQuery()) {
    Uint32 totalRows = 0;
    for (Uint32 i = 0; i < getNoOfOperations(); i++) {
      totalRows += getQueryOperation(i).getMaxBatchRows();
    }
    error = m_tupleSetAlloc.init(2 * rootFragments * totalRows);
    if (unlikely(error != 0)) {
      setErrorCode(error);
      return -1;
    }
  }

  /**
   * Allocate and initialize SPJ-worker state objects.
   * Will also cause a ResultStream object containing a
   * NdbReceiver to be constructed for each operation in QueryTree
   */
  // Compute root op number before worker init (workers cache it)
  m_rootOpNo = getRootOpNo();

  m_workers = new NdbWorker[m_workerCount];
  if (m_workers == nullptr) {
    setErrorCode(Err_MemoryAlloc);
    return -1;
  }
  for (Uint32 i = 0; i < m_workerCount; i++) {
    m_workers[i].init(*this, i);  // Set worker number.
  }

  const Uint32Buffer &queryTree = getQueryDef().getSerialized();
  const QueryNode *queryNode = (const QueryNode *)queryTree.addr(1);

  // Fill in parameters (into ATTRINFO) for QueryTree.
  for (Uint32 i = 0; i < m_countOperations; i++) {
    const int error = m_operations[i].prepareAttrInfo(m_attrInfo, queryNode);
    if (unlikely(error)) {
      setErrorCode(error);
      return -1;
    }
  }

  if (unlikely(m_attrInfo.isMemoryExhausted() ||
               m_keyInfo.isMemoryExhausted())) {
    setErrorCode(Err_MemoryAlloc);
    return -1;
  }

  if (unlikely(m_attrInfo.getSize() > ScanTabReq::MaxTotalAttrInfo ||
               m_keyInfo.getSize() > ScanTabReq::MaxTotalAttrInfo)) {
    setErrorCode(Err_ReadTooMuch);  // TODO: find a more suitable errorcode,
    return -1;
  }

  // Setup m_applStreams and m_fullStreams for receiving results
  const NdbRecord *keyRec = nullptr;
  if (getRoot().getQueryOperationDef().getIndex() != nullptr) {
    /* keyRec is needed for comparing records when doing ordered index scans.*/
    keyRec = getRoot().getQueryOperationDef().getIndex()->getDefaultRecord();
    assert(keyRec != nullptr);
  }
  m_applFrags.prepare(m_pointerAlloc, getRoot().getOrdering(), m_workerCount,
                      keyRec, getRoot().m_ndbRecord, getRoot().m_read_mask);

  if (getQueryDef().isScanQuery()) {
    NdbWorker::buildReceiverIdMap(m_workers, m_workerCount);
    for (Uint32 w = 0; w < m_workerCount; w++) {
      DEB_CTE_API("prepareSend: worker[%u] receiverId=0x%x\n",
                  w, m_workers[w].getReceiverId());
    }
  }

  // Aggregation setup (RONDB-733)
  if (m_hasAggregation) {
    if (prepareAggregation() != 0) {
      return -1;
    }
  }

#ifdef TRACE_SERIALIZATION
  ndbout << "Serialized ATTRINFO : ";
  for (Uint32 i = 0; i < m_attrInfo.getSize(); i++) {
    char buf[12];
    sprintf(buf, "%.8x", m_attrInfo.get(i));
    ndbout << buf << " ";
  }
  ndbout << endl;
#endif

  assert(m_pendingWorkers == 0);
  m_state = Prepared;

  // For CTE queries, set cursor to main root (skip CTE ops)
  m_rootOpNo = getRootOpNo();
  m_globalCursor = m_rootOpNo;

  return 0;
}  // NdbQueryImpl::prepareSend

/**
 * Aggregation setup (RONDB-733):
 * Allocate dedicated receivers for aggregation results, copy the
 * aggregation program for Section 2, and create NdbAggregator.
 */
int NdbQueryImpl::prepareAggregation() {
  // Aggregation is supported for both scan queries (scanCte main root)
  // and lookup queries (lookupCte main root with CTE-only aggregation).
  const Uint32 numLeaves = getQueryDef().getNumAggregateLeaves();
  const Uint32 numCtes = getQueryDef().getNumCtes();

  DEB_CTE_API("prepareAggregation: numLeaves=%u numCtes=%u "
              "numOps=%u workerCount=%u\n",
              numLeaves, numCtes,
              getQueryDef().getNoOfOperations(), m_workerCount);

  // Must have either aggregate leaves or CTEs (or both)
  if (unlikely(numLeaves == 0 && numCtes == 0)) {
    setErrorCode(QRY_WRONG_OPERATION_TYPE);
    return -1;
  }
  if (unlikely(numLeaves > NDB_SPJ_MAX_TREE_NODES)) {
    setErrorCode(QRY_WRONG_OPERATION_TYPE);
    return -1;
  }

  const NdbQueryOptionsImpl *firstOpts = nullptr;
  if (numLeaves > 0) {
    // Use first leaf for GROUP BY column info and table reference
    const Uint32 firstLeafOpNo = getQueryDef().getAggregateLeafOpNo(0);
    const NdbQueryOperationDefImpl &firstLeafDef =
        getQueryDef().getQueryOperation(firstLeafOpNo);
    firstOpts = &firstLeafDef.getOptions();

    // Section header: (0x0722 << 16) | numLeaves
    // Then per leaf: [progLen, program words...]
    m_aggProgram.append((0x0722 << 16) | numLeaves);
    for (Uint32 leaf = 0; leaf < numLeaves; leaf++) {
      const Uint32 leafOpNo = getQueryDef().getAggregateLeafOpNo(leaf);
      const NdbQueryOperationDefImpl &leafDef =
          getQueryDef().getQueryOperation(leafOpNo);
      const NdbQueryOptionsImpl &leafOpts = leafDef.getOptions();
      const Uint32 *progBuf = leafOpts.getAggProgramBuffer();
      const Uint32 progLen = leafOpts.getAggProgramLen();
      assert(progBuf != nullptr && progLen > 0);
      m_aggProgram.append(progLen);
      for (Uint32 i = 0; i < progLen; i++) {
        m_aggProgram.append(progBuf[i]);
      }
    }
  }
  // No main agg program when numLeaves==0 — CTE_DEFS_MARKER
  // immediately follows aggReceiverId in the KeyInfo section.

  // Append CTE definitions if present
  if (numCtes > 0) {
    m_aggProgram.append(CTE_DEFS_MARKER);
    m_aggProgram.append(numCtes);
    for (Uint32 c = 0; c < numCtes; c++) {
      const NdbQueryDefImpl::CteDefInfo &cte = getQueryDef().getCteDef(c);
      m_aggProgram.append(cte.tableId);
      m_aggProgram.append(cte.schemaVersion);
      m_aggProgram.append((Uint32)(cte.depMask & 0xFFFFFFFF));  // lo
      m_aggProgram.append((Uint32)(cte.depMask >> 32));          // hi
      m_aggProgram.append(cte.flags);
      m_aggProgram.append(cte.aggProgram.size());
      for (Uint32 i = 0; i < cte.aggProgram.size(); i++) {
        m_aggProgram.append(cte.aggProgram[i]);
      }
    }
  }

  {
    Uint32Buffer metaContainer;
    Uint32 blockCount = 0;
    metaContainer.append(JOIN_AGG_META_MARKER);
    metaContainer.append(JOIN_AGG_META_VERSION);
    const Uint32 blockCountPos = metaContainer.getSize();
    metaContainer.append(0);

    if (numLeaves > 0 && firstOpts != nullptr) {
      Uint32Buffer block;
      const NdbTableImpl *aggTable = firstOpts->getAggTable();
      const Uint32 *progBuf = firstOpts->getAggProgramBuffer();
      bool hasBlock = false;
      if (aggTable != nullptr && aggTable->m_facade != nullptr) {
        hasBlock = buildJoinAggGbColumnMetaBlock(
            block,
            progBuf,
            firstOpts->getAggProgramLen(),
            firstOpts->getAggGbColumns(),
            firstOpts->getAggNGroupByCols(),
            aggTable->m_facade->getObjectId(),
            aggTable->m_facade->getObjectVersion());
      }
      if (unlikely(block.isMemoryExhausted())) {
        setErrorCode(Err_MemoryAlloc);
        return -1;
      }
      if (hasBlock) {
        metaContainer.append(JOIN_AGG_META_KIND_MAIN);
        metaContainer.append(RNIL);
        metaContainer.append(block.getSize());
        metaContainer.append(block);
        blockCount++;
      }
    }

    for (Uint32 c = 0; c < numCtes; c++) {
      const NdbQueryDefImpl::CteDefInfo &cte = getQueryDef().getCteDef(c);
      Uint32Buffer block;
      const bool hasBlock = buildJoinAggGbColumnMetaBlock(
          block,
          cte.aggProgram.getBase(),
          cte.aggProgram.size(),
          cte.gbColumns.getBase(),
          cte.gbColumns.size(),
          cte.tableId,
          cte.schemaVersion);
      if (unlikely(block.isMemoryExhausted())) {
        setErrorCode(Err_MemoryAlloc);
        return -1;
      }
      if (hasBlock) {
        metaContainer.append(JOIN_AGG_META_KIND_CTE);
        metaContainer.append(c);
        metaContainer.append(block.getSize());
        metaContainer.append(block);
        blockCount++;
      }
    }

    if (blockCount > 0) {
      metaContainer.put(blockCountPos, blockCount);
      if (unlikely(metaContainer.isMemoryExhausted())) {
        setErrorCode(Err_MemoryAlloc);
        return -1;
      }
      m_aggProgram.append(metaContainer);
    }
  }

  DEB_CTE_API("prepareAggregation: aggProgram built, %u words\n",
              m_aggProgram.getSize());

  if (unlikely(m_aggProgram.isMemoryExhausted())) {
    setErrorCode(Err_MemoryAlloc);
    return -1;
  }

  // Allocate NdbReceiver objects and NdbAggregator only when the
  // main query has aggregate leaves. CTE-only queries (no main agg)
  // use standard scan receivers for CTE_LOOKUP result rows.
  if (numLeaves > 0) {
    // Use m_workerCount receivers — one per SPJ worker gives good
    // hash distribution for group routing.
    Ndb *const ndb = m_transaction.getNdb();
    m_numAggReceivers = m_workerCount;
    m_aggReceivers = new NdbReceiver *[m_numAggReceivers];
    if (unlikely(m_aggReceivers == nullptr)) {
      setErrorCode(Err_MemoryAlloc);
      return -1;
    }
    for (Uint32 i = 0; i < m_numAggReceivers; i++) {
      NdbReceiver *rec = ndb->getNdbScanRec();
      if (unlikely(rec == nullptr)) {
        setErrorCode(Err_MemoryAlloc);
        return -1;
      }
      rec->init(NdbReceiver::NDB_AGG_RECEIVER, this);
      m_aggReceivers[i] = rec;
    }

    // Create NdbAggregator for result collection.
    const NdbTableImpl *aggTable = firstOpts->getAggTable();
    assert(aggTable != nullptr);
    m_aggregator = new NdbAggregator(aggTable->m_facade);
  }

  if (numLeaves == 0) {
    // CTE-only query: no main aggregation, no aggregator init needed.
    // The m_aggProgram contains only CTE definitions.
  } else if (numLeaves == 1) {
    // Single-leaf: initialize directly from program
    const Uint32 *progBuf = firstOpts->getAggProgramBuffer();
    const Uint32 progLen = firstOpts->getAggProgramLen();
    m_aggregator->initForResults(progBuf, progLen,
                                 firstOpts->getAggGbColumns(),
                                 firstOpts->getAggNGroupByCols(),
                                 firstOpts->getAggColumns(),
                                 progBuf[1] & 0xFFFF);
  } else {
    // Multi-leaf: build combined result program with total n_agg_results
    // and all agg instructions from all leaves (with adjusted slot indices).
    // Program format: [header(8), gb_cols..., instructions...]
    // Header word 1: (n_gb_cols << 16) | n_agg_results_total
    const Uint32 HEADER_SIZE = 8;
    const Uint32 *firstProg = firstOpts->getAggProgramBuffer();
    const Uint32 nGbCols = firstProg[1] >> 16;

    // Calculate total n_agg_results and collect all instructions
    Uint32 totalAggResults = 0;
    struct LeafInstr {
      const Uint32 *buf;
      Uint32 len;
      Uint32 nResults;
    };
    LeafInstr leafInstrs[NDB_SPJ_MAX_TREE_NODES];
    assert(numLeaves <= NDB_SPJ_MAX_TREE_NODES);

    for (Uint32 leaf = 0; leaf < numLeaves; leaf++) {
      const Uint32 leafOpNo = getQueryDef().getAggregateLeafOpNo(leaf);
      const NdbQueryOperationDefImpl &leafDef =
          getQueryDef().getQueryOperation(leafOpNo);
      const NdbQueryOptionsImpl &leafOpts = leafDef.getOptions();
      const Uint32 *prog = leafOpts.getAggProgramBuffer();
      const Uint32 progLen = leafOpts.getAggProgramLen();
      const Uint32 leafNResults = prog[1] & 0xFFFF;
      const Uint32 instrStart = HEADER_SIZE + nGbCols;
      leafInstrs[leaf].buf = prog + instrStart;
      leafInstrs[leaf].len = progLen - instrStart;
      leafInstrs[leaf].nResults = leafNResults;
      totalAggResults += leafNResults;
    }

    // Build combined program: header + GB cols from first leaf + all instrs
    Uint32 combinedLen = HEADER_SIZE + nGbCols;
    for (Uint32 leaf = 0; leaf < numLeaves; leaf++) {
      combinedLen += leafInstrs[leaf].len;
    }

    Uint32 *combinedProg = new Uint32[combinedLen];
    // Copy header from first leaf, override totalLen and n_agg_results
    memcpy(combinedProg, firstProg, HEADER_SIZE * sizeof(Uint32));
    combinedProg[0] = (combinedProg[0] & 0xFFFF0000) | combinedLen;
    combinedProg[1] = (nGbCols << 16) | totalAggResults;

    // Copy GROUP BY column descriptors from first leaf
    memcpy(combinedProg + HEADER_SIZE, firstProg + HEADER_SIZE,
           nGbCols * sizeof(Uint32));

    // Copy instructions from all leaves, adjusting agg slot indices
    const NdbDictionary::Column **combinedAggColumns =
        new const NdbDictionary::Column *[totalAggResults];
    if (combinedAggColumns == nullptr) {
      delete[] combinedProg;
      return Err_MemoryAlloc;
    }
    Uint32 pos = HEADER_SIZE + nGbCols;
    Uint32 accOffset = 0;
    for (Uint32 leaf = 0; leaf < numLeaves; leaf++) {
      for (Uint32 j = 0; j < leafInstrs[leaf].len; j++) {
        Uint32 word = leafInstrs[leaf].buf[j];
        Uint32 op = (word >> 26) & 0x3F;
        // Agg ops (Sum, Count, Min, Max and their typed variants) have
        // the agg slot index in bits 0-15. Adjust by accOffset.
        if (op >= kOpSum && op <= kOpMinDouble) {
          Uint32 slot = word & 0xFFFF;
          word = (word & 0xFFFF0000) | (slot + accOffset);
        }
        combinedProg[pos++] = word;
      }
      const Uint32 leafOpNo = getQueryDef().getAggregateLeafOpNo(leaf);
      const NdbQueryOperationDefImpl &leafDef =
          getQueryDef().getQueryOperation(leafOpNo);
      const NdbQueryOptionsImpl &leafOpts = leafDef.getOptions();
      memcpy(combinedAggColumns + accOffset,
             leafOpts.getAggColumns(),
             leafInstrs[leaf].nResults *
                 sizeof(const NdbDictionary::Column *));
      accOffset += leafInstrs[leaf].nResults;
    }
    assert(pos == combinedLen);

    m_aggregator->initForResults(combinedProg, combinedLen,
                                 firstOpts->getAggGbColumns(),
                                 firstOpts->getAggNGroupByCols(),
                                 combinedAggColumns,
                                 totalAggResults);
    delete[] combinedAggColumns;
    delete[] combinedProg;
  }
  return 0;
}  // NdbQueryImpl::prepareAggregation

/** This iterator is used for inserting a sequence of receiver ids
 * for the initial batch of a scan into a section via a GenericSectionPtr.*/
class InitialReceiverIdIterator : public GenericSectionIterator {
 public:
  InitialReceiverIdIterator(NdbWorker workers[], Uint32 workerCount)
      : m_workers(workers), m_workerCount(workerCount), m_workerNo(0) {}

  ~InitialReceiverIdIterator() override {}

  /**
   * Get next batch of receiver ids.
   * @param sz This will be set to the number of receiver ids that have been
   * put in the buffer (0 if end has been reached.)
   * @return Array of receiver ids (or NULL if end reached.
   */
  const Uint32 *getNextWords(Uint32 &sz) override;

  void reset() override { m_workerNo = 0; }

 private:
  /**
   * Size of internal receiver id buffer. This value is arbitrary, but
   * a larger buffer would mean fewer calls to getNextWords(), possibly
   * improving efficiency.
   */
  static const Uint32 bufSize = 16;

  /** Set of SPJ workers which we want to itterate receiver ids for.*/
  const NdbWorker *const m_workers;
  const Uint32 m_workerCount;

  /**
   * The next SPJ-worker to be processed. (Range for 0 to no of workers.)
   */
  Uint32 m_workerNo;
  /** Buffer for storing one batch of receiver ids.*/
  Uint32 m_receiverIds[bufSize];
};

const Uint32 *InitialReceiverIdIterator::getNextWords(Uint32 &sz) {
  /**
   * For the initial batch, we want to retrieve one batch from each worker
   * whether it is a sorted scan or not.
   */
  if (m_workerNo >= m_workerCount) {
    sz = 0;
    return nullptr;
  } else {
    Uint32 cnt = 0;
    while (cnt < bufSize && m_workerNo < m_workerCount) {
      m_receiverIds[cnt] = m_workers[m_workerNo].getReceiverId();
      cnt++;
      m_workerNo++;
    }
    sz = cnt;
    return m_receiverIds;
  }
}

/** This iterator is used for inserting a sequence of 'TcPtrI'
 * for a NEXTREQ to a single or multiple SPJ-workers via a GenericSectionPtr.*/
class FetchMoreTcIdIterator : public GenericSectionIterator {
 public:
  FetchMoreTcIdIterator(NdbWorker *workers[], Uint32 cnt)
      : m_workers(workers), m_workerCount(cnt), m_currWorkerNo(0) {}

  ~FetchMoreTcIdIterator() override {}

  /**
   * Get next batch of receiver ids.
   * @param sz This will be set to the number of receiver ids that have been
   * put in the buffer (0 if end has been reached.)
   * @return Array of receiver ids (or NULL if end reached.
   */
  const Uint32 *getNextWords(Uint32 &sz) override;

  void reset() override { m_currWorkerNo = 0; }

 private:
  /**
   * Size of internal receiver id buffer. This value is arbitrary, but
   * a larger buffer would mean fewer calls to getNextWords(), possibly
   * improving efficiency.
   */
  static const Uint32 bufSize = 16;

  /** Set of SPJ workers which we want to itterate TcPtrI ids for.*/
  NdbWorker **const m_workers;
  const Uint32 m_workerCount;

  /** The next worker to be processed. (Range for 0 to no of workers.)
   */
  Uint32 m_currWorkerNo;
  /** Buffer for storing one batch of receiver ids.*/
  Uint32 m_receiverIds[bufSize];
};

const Uint32 *FetchMoreTcIdIterator::getNextWords(Uint32 &sz) {
  /**
   * For the initial batch, we want to retrieve one batch from each worker
   * whether it is a sorted scan or not.
   */
  if (m_currWorkerNo >= m_workerCount) {
    sz = 0;
    return nullptr;
  } else {
    Uint32 cnt = 0;
    while (cnt < bufSize && m_currWorkerNo < m_workerCount) {
      m_receiverIds[cnt] = m_workers[m_currWorkerNo]->getReceiverTcPtrI();
      cnt++;
      m_currWorkerNo++;
    }
    sz = cnt;
    return m_receiverIds;
  }
}

/******************************************************************************
int doSend()    Send serialized queryTree and parameters encapsulated in
                either a SCAN_TABREQ or TCKEYREQ to TC.

NOTE:           The TransporterFacade mutex is already set by callee.

Return Value:   Return >0 : send was successful, returns number of signals sent
                Return -1: In all other case.
Parameters:     nodeId: Receiving processor node
Remark:         Send a TCKEYREQ or SCAN_TABREQ (long) signal depending of
                the query being either a lookup or scan type.
                KEYINFO and ATTRINFO are included as part of the long signal
******************************************************************************/
#ifdef DEBUG_JOIN_AGG_TRACE
/**
 * Trace SCAN_TABREQ sections for JoinAgg debugging.
 */
void NdbQueryImpl::traceJoinAggScanTabReq(
    const Uint32Buffer &combinedAggSec2) {
  fprintf(stderr, "\n====== NdbQuery SCAN_TABREQ (JoinAgg) ======\n");
  fprintf(stderr, "workerCount=%u, scanParallelism=%u\n",
          m_workerCount, m_workerCount);

  /* Section 0: worker receiver IDs (for SPJ scan results) */
  fprintf(stderr, "--- Section 0: Worker Receiver IDs (%u) ---\n",
          m_workerCount);
  for (Uint32 i = 0; i < m_workerCount; i++)
    fprintf(stderr, "  worker[%u] receiverId=0x%x\n",
            i, m_workers[i].getReceiverId());

  /* Section 1: AttrInfo = QueryTree + Parameters */
  fprintf(stderr, "--- Section 1: AttrInfo / QueryTree (%u words) ---\n",
          m_attrInfo.getSize());
  {
    const Uint32 *ai = m_attrInfo.addr();
    Uint32 aiSz = m_attrInfo.getSize();
    /* Decode QueryTree header */
    if (aiSz > 0) {
      Uint32 cnt_len = ai[0];
      fprintf(stderr, "  QueryTree header: cnt=%u, len=%u\n",
              cnt_len >> 16, cnt_len & 0xFFFF);
    }
    DEB_JOIN_AGG("AttrInfo (QueryTree+Params)", ai, aiSz);
  }

  /* Section 2: Combined [boundsLen, bounds, aggReceiverId, aggProgram] */
  fprintf(stderr,
          "--- Section 2: Combined AggSection (%u words) ---\n",
          combinedAggSec2.getSize());
  {
    const Uint32 *s2 = combinedAggSec2.addr();
    Uint32 s2Sz = combinedAggSec2.getSize();
    if (s2Sz > 0) {
      Uint32 bLen = s2[0];
      fprintf(stderr, "  boundsLen=%u\n", bLen);
      if (1 + bLen < s2Sz)
        fprintf(stderr, "  aggReceiverId=0x%x\n", s2[1 + bLen]);
      Uint32 progStart = 2 + bLen;
      if (progStart < s2Sz) {
        fprintf(stderr, "  aggProgram (%u words):\n", s2Sz - progStart);
        for (Uint32 i = progStart; i < s2Sz && i < progStart + 12; i++)
          fprintf(stderr, "    [%u] 0x%08x\n", i - progStart, s2[i]);
        if (s2Sz - progStart > 12)
          fprintf(stderr, "    ... (%u more words)\n",
                  s2Sz - progStart - 12);
      }
    }
  }
  fprintf(stderr, "============================================\n\n");
}
#else
void NdbQueryImpl::traceJoinAggScanTabReq(const Uint32Buffer &) {}
#endif  /* DEBUG_JOIN_AGG_TRACE */

int NdbQueryImpl::doSend(int nodeId, bool lastFlag) {
  if (unlikely(m_state != Prepared)) {
    assert(m_state >= Initial && m_state < Destructed);
    if (m_state == Failed)
      setErrorCode(QRY_IN_ERROR_STATE);
    else
      setErrorCode(QRY_ILLEGAL_STATE);
    DEBUG_CRASH();
    return -1;
  }

  Ndb &ndb = *m_transaction.getNdb();
  NdbImpl *impl = ndb.theImpl;

  const NdbQueryOperationImpl &root = getRoot();
  const NdbQueryOperationDefImpl &rootDef = root.getQueryOperationDef();
  /* For CTE queries where the main root is a lookup (lookupCte) or
   * a scanCte over a synthetic virt table, getFragRoutingTable falls
   * back to the CTE base scan table so DBTC can route fragments
   * correctly via DIH.  Otherwise it's the root op's own table. */
  const NdbTableImpl *rootTable =
      getQueryDef().isScanQuery()
          ? &getFragRoutingTable()
          : (rootDef.getIndex() ? rootDef.getIndex()->getIndexTable()
                                : &rootDef.getTable());

  Uint32 tTableId = rootTable->m_id;
  Uint32 tSchemaVersion = rootTable->m_version;

  for (Uint32 i = 0; i < m_workerCount; i++) {
    m_workers[i].prepareNextReceiveSet();
  }

  if (rootDef.isScanOperation() || getQueryDef().isScanQuery()) {
    Uint32 scan_flags = 0;  // TODO: Specify with ScanOptions::SO_SCANFLAGS

    // The number of acc-scans are limited therefore use tup-scans instead.
    bool tupScan = (scan_flags & NdbScanOperation::SF_TupScan) || true;
#if defined(VM_TRACE)
    if (ndb.theImpl->forceAccTableScans) {
      tupScan = false;
    }
#endif

    bool rangeScan = false;

    /* Handle IndexScan specifics */
    if ((int)rootTable->m_indexType ==
        (int)NdbDictionary::Index::OrderedIndex) {
      rangeScan = true;
      tupScan = false;
    }
    const Uint32 descending =
        root.getOrdering() == NdbQueryOptions::ScanOrdering_descending ? 1 : 0;
    assert(descending == 0 || (int)rootTable->m_indexType ==
                                  (int)NdbDictionary::Index::OrderedIndex);

    assert(root.getMaxBatchRows() > 0);

    NdbApiSignal tSignal(&ndb);
    tSignal.setSignal(GSN_SCAN_TABREQ, refToBlock(m_scanTransaction->m_tcRef));

    ScanTabReq *const scanTabReq =
        CAST_PTR(ScanTabReq, tSignal.getDataPtrSend());
    Uint32 reqInfo = 0;

    const Uint64 transId = m_scanTransaction->getTransactionId();

    scanTabReq->apiConnectPtr = m_scanTransaction->theTCConPtr;
    scanTabReq->buddyConPtr =
        m_scanTransaction
            ->theBuddyConPtr;  // 'buddy' refers 'real-transaction'->theTCConPtr
    scanTabReq->spare = 0;     // Unused in later protocol versions
    scanTabReq->tableId = tTableId;
    scanTabReq->tableSchemaVersion = tSchemaVersion;
    scanTabReq->storedProcId = 0xFFFF;
    scanTabReq->transId1 = (Uint32)transId;
    scanTabReq->transId2 = (Uint32)(transId >> 32);

    Uint32 batchRows = root.getMaxBatchRows();
    const Uint32 batchByteSize = root.getMaxBatchBytes();

    /**
     * Check if query is a sorted scan-scan.
     * Ordering can then only be guaranteed by restricting
     * parent batch to contain single rows.
     * (Child scans will have 'normal' batch size).
     *
     * Note that this solved the problem only for the 'v1'
     * version of SPJ requests, and parameter. The v2 protocol
     * introduced 'batch_size_rows' as part of the parameter,
     * which took precedence over the batch size set in ScanTabReq.
     * This resulted in giving not-sorted results even though
     * sort order was requested. This is now fixed by setting a
     * 'SFP_SORTED_ORDER' flag in the ScanFragParameter
     * instead of hacking the batch size on the client side.
     */
    if (root.getOrdering() != NdbQueryOptions::ScanOrdering_unordered &&
        getQueryDef().getQueryType() == NdbQueryDef::MultiScanQuery) {
      batchRows = 1;
    }
    if (batchRows > MAX_PARALLEL_OP_PER_SCAN_WITH_LOCK) {
      if (ndbd_support_large_batch_size(ndb.getMinDbNodeVersion())) {
        ScanTabReq::setScanBatch(reqInfo, 0);
      } else {
        batchRows = MAX_PARALLEL_OP_PER_SCAN_WITH_LOCK;
        ScanTabReq::setScanBatch(reqInfo, batchRows);
      }
    } else {
      ScanTabReq::setScanBatch(reqInfo, batchRows);
    }
    scanTabReq->batch_byte_size = batchByteSize;
    scanTabReq->first_batch_size = batchRows;

    if (m_fragsPerWorker > 1) {
      ScanTabReq::setMultiFragFlag(reqInfo, 1);
    }
    ScanTabReq::setViaSPJFlag(reqInfo, 1);
    ScanTabReq::setPassAllConfsFlag(reqInfo, 1);

    ScanTabReq::setRangeScanFlag(reqInfo, rangeScan);
    ScanTabReq::setDescendingFlag(reqInfo, descending);
    ScanTabReq::setTupScanFlag(reqInfo, tupScan);
    ScanTabReq::setNoDiskFlag(reqInfo, !root.diskInUserProjection());
    ScanTabReq::setExtendedConf(reqInfo, 1);

    // Assume LockMode LM_ReadCommited, set related lock flags
    ScanTabReq::setLockMode(reqInfo, false);  // not exclusive
    ScanTabReq::setHoldLockFlag(reqInfo, false);
    ScanTabReq::setReadCommittedFlag(reqInfo, true);

    //  m_keyInfo = (scan_flags & NdbScanOperation::SF_KeyInfo) ? 1 : 0;

    // If scan is pruned, use optional 'distributionKey' to hold hashvalue
    if (m_prunability == Prune_Yes) {
      //    printf("Build pruned SCANREQ, w/ hashValue:%d\n", hashValue);
      ScanTabReq::setDistributionKeyFlag(reqInfo, 1);
      scanTabReq->distributionKey = m_pruneHashVal;
      tSignal.setLength(ScanTabReq::StaticLength + 1);
    } else {
      tSignal.setLength(ScanTabReq::StaticLength);
    }
    Uint32 user_id = getNdbTransaction().m_user_id;
    if (user_id != RNIL) {
      ScanTabReq::setUserIdFlag(reqInfo, 1);
      scanTabReq->userId = user_id;
      scanTabReq->userIdVersion = getNdbTransaction().m_user_id_version;
      tSignal.setLength(ScanTabReq::StaticLength + 4);
    }
    if (m_hasAggregation) {
      if (!ndbd_support_pushdown_join_agg(ndb.getMinDbNodeVersion())) {
        setErrorCode(Err_FunctionNotImplemented);
        return -1;
      }
      ScanTabReq::setJoinAggFlag(reqInfo, 1);
      scanTabReq->scanParallelism = m_workerCount * m_fragsPerWorker;
      tSignal.setLength(ScanTabReq::StaticLength + 5);
    }
    scanTabReq->requestInfo = reqInfo;

    /**
     * Then send the signal:
     *
     * SCANTABREQ always has 2 mandatory sections and an optional
     * third section
     * Section 0 : List of receiver Ids NDBAPI has allocated
     *             for the scan
     * Section 1 : ATTRINFO section
     * Section 2 : Optional KEYINFO section.
     *             For JoinAgg: [boundsLen, bounds..., aggProgram...]
     */

    /**
     * For JoinAgg queries, section 2 carries both optional bounds,
     * the agg receiver ID, and the aggregation program:
     *   Word 0:              boundsLen (0 if no bounds)
     *   Words 1..boundsLen:  bounds data (from m_keyInfo)
     *   Word boundsLen+1:    aggReceiverId (object map ID for results)
     *   Remaining words:     aggregation program
     * DBTC parses this header to split bounds from agg program,
     * and uses aggReceiverId as resultData in JOIN_AGG_SETUP_REQ.
     */
    Uint32Buffer combinedAggSec2;
    if (m_hasAggregation) {
      assert(m_aggProgram.getSize() > 0);
      const Uint32 boundsLen = m_keyInfo.getSize();
      combinedAggSec2.append(boundsLen);
      if (boundsLen > 0) {
        combinedAggSec2.append(m_keyInfo);
      }
      // aggReceiverId: use first agg receiver if main query has
      // aggregation, else use 0 (CTE-only queries — DBTC uses this
      // for result routing but CTE results go to hash tables, not API).
      if (m_numAggReceivers > 0) {
        combinedAggSec2.append(m_aggReceivers[0]->getId());
      } else {
        combinedAggSec2.append(Uint32(0));
      }
      combinedAggSec2.append(m_aggProgram);
      assert(!combinedAggSec2.isMemoryExhausted());
    }

    GenericSectionPtr secs[3];
    InitialReceiverIdIterator receiverIdIter(m_workers, m_workerCount);
    LinearSectionIterator attrInfoIter(m_attrInfo.addr(), m_attrInfo.getSize());
    LinearSectionIterator keyInfoIter(m_keyInfo.addr(), m_keyInfo.getSize());
    LinearSectionIterator combinedAggIter(combinedAggSec2.addr(),
                                          combinedAggSec2.getSize());

    secs[0].sectionIter = &receiverIdIter;
    secs[0].sz = m_workerCount;

    secs[1].sectionIter = &attrInfoIter;
    secs[1].sz = m_attrInfo.getSize();

    Uint32 numSections = 2;
    if (m_hasAggregation) {
      secs[2].sectionIter = &combinedAggIter;
      secs[2].sz = combinedAggSec2.getSize();
      numSections = 3;
    } else if (m_keyInfo.getSize() > 0) {
      secs[2].sectionIter = &keyInfoIter;
      secs[2].sz = m_keyInfo.getSize();
      numSections = 3;
    }
    DBUG_PRINT("info", ("Send SCAN_TABREQ: NdbQueryOperation"));

    if (m_hasAggregation) {
      traceJoinAggScanTabReq(combinedAggSec2);
    }

    /* Send Fragmented as SCAN_TABREQ can be large */
    const int res =
        impl->sendFragmentedSignal(&tSignal, nodeId, secs, numSections);
    if (unlikely(res == -1)) {
      setErrorCode(Err_SendFailed);  // Error: 'Send to NDB failed'
      return FetchResult_sendFail;
    }
    m_tcState = Active;

  } else {  // Lookup query

    DBUG_PRINT("info", ("Send TCKEYREQ: NdbQueryOperation"));
    NdbApiSignal tSignal(&ndb);
    tSignal.setSignal(GSN_TCKEYREQ, refToBlock(m_transaction.m_tcRef));

    TcKeyReq *const tcKeyReq = CAST_PTR(TcKeyReq, tSignal.getDataPtrSend());

    const Uint64 transId = m_transaction.getTransactionId();
    tcKeyReq->apiConnectPtr = m_transaction.theTCConPtr;
    tcKeyReq->apiOperationPtr = root.getIdOfReceiver();
    tcKeyReq->tableId = tTableId;
    tcKeyReq->tableSchemaVersion = tSchemaVersion;
    tcKeyReq->transId1 = (Uint32)transId;
    tcKeyReq->transId2 = (Uint32)(transId >> 32);

    Uint32 attrLen = 0;
    tcKeyReq->setAttrinfoLen(attrLen, 0);  // Not required for long signals.
    tcKeyReq->attrLen = attrLen;

    Uint32 reqInfo = 0;
    Uint32 interpretedFlag =
        root.hasInterpretedCode() &&
        rootDef.getType() == NdbQueryOperationDef::PrimaryKeyAccess;

    TcKeyReq::setOperationType(reqInfo, NdbOperation::ReadRequest);
    TcKeyReq::setViaSPJFlag(reqInfo, true);
    TcKeyReq::setKeyLength(reqInfo, 0);     // This is a long signal
    TcKeyReq::setAIInTcKeyReq(reqInfo, 0);  // Not needed
    TcKeyReq::setInterpretedFlag(reqInfo, interpretedFlag);
    TcKeyReq::setStartFlag(reqInfo, m_startIndicator);
    TcKeyReq::setExecuteFlag(reqInfo, lastFlag);
    TcKeyReq::setNoDiskFlag(reqInfo, !root.diskInUserProjection());
    TcKeyReq::setAbortOption(reqInfo, NdbOperation::AO_IgnoreError);

    TcKeyReq::setDirtyFlag(reqInfo, true);
    TcKeyReq::setSimpleFlag(reqInfo, true);
    TcKeyReq::setCommitFlag(reqInfo, m_commitIndicator);

    Uint32 user_id = getNdbTransaction().m_user_id;
    if (user_id != RNIL) {
      tcKeyReq->userId = user_id;
      tcKeyReq->userIdVersion = getNdbTransaction().m_user_id_version;
      TcKeyReq::setUserIdFlag(reqInfo, 1);
      tSignal.setLength(TcKeyReq::StaticLength + 2);
    } else {
      tSignal.setLength(TcKeyReq::StaticLength);
    }
    tcKeyReq->requestInfo = reqInfo;

    /****
        // Unused optional part located after TcKeyReq::StaticLength
        tcKeyReq->scanInfo = 0;
        tcKeyReq->distrGroupHashValue = 0;
        tcKeyReq->distributionKeySize = 0;
        tcKeyReq->storedProcId = 0xFFFF;
    ***/

    /**** TODO ... maybe - from NdbOperation::prepareSendNdbRecord(AbortOption
    ao) Uint8 abortOption= (ao == DefaultAbortOption) ? (Uint8) m_abortOption :
    (Uint8) ao;

        m_abortOption= theSimpleIndicator && theOperationType==ReadRequest ?
          (Uint8) AO_IgnoreError : (Uint8) abortOption;

        TcKeyReq::setAbortOption(reqInfo, m_abortOption);
        TcKeyReq::setCommitFlag(tcKeyReq->requestInfo, theCommitIndicator);
    *****/

    LinearSectionPtr secs[2];
    secs[TcKeyReq::KeyInfoSectionNum].p = m_keyInfo.addr();
    secs[TcKeyReq::KeyInfoSectionNum].sz = m_keyInfo.getSize();
    Uint32 numSections = 1;

    if (m_attrInfo.getSize() > 0) {
      secs[TcKeyReq::AttrInfoSectionNum].p = m_attrInfo.addr();
      secs[TcKeyReq::AttrInfoSectionNum].sz = m_attrInfo.getSize();
      numSections = 2;
    }

    int res = 0;
    const Uint32 long_sections_size =
        m_keyInfo.getSize() + m_attrInfo.getSize();
    const Uint32 nodeVersion = impl->getNodeNdbVersion(nodeId);
    if (long_sections_size <= NDB_MAX_LONG_SECTIONS_SIZE) {
      res = impl->sendSignal(&tSignal, nodeId, secs, numSections);
    } else if (ndbd_frag_tckeyreq(nodeVersion)) {
      res = impl->sendFragmentedSignal(&tSignal, nodeId, secs, numSections);
    } else {
      /* It should not be possible to see a table definition that supports
       * big rows unless all data nodes that are started also can handle it.
       */
      require(ndbd_frag_tckeyreq(nodeVersion));
    }

    if (unlikely(res == -1)) {
      setErrorCode(Err_SendFailed);  // Error: 'Send to NDB failed'
      return FetchResult_sendFail;
    }
    m_transaction.OpSent();
    m_workers[0].incrOutstandingResults(1 + getNoOfOperations() +
                                        getNoOfLeafOperations());
  }  // if

  assert(m_pendingWorkers == 0);
  m_pendingWorkers = m_workerCount;

  // Shrink memory footprint by removing structures not required after
  // ::execute()
  m_keyInfo.releaseExtend();
  m_attrInfo.releaseExtend();

  // TODO: Release m_interpretedCode now?

  /* Todo : Consider calling NdbOperation::postExecuteRelease()
   * Ideally it should be called outside TP mutex, so not added
   * here yet
   */

  m_state = Executing;
  return 1;
}  // NdbQueryImpl::doSend()

/******************************************************************************
int sendFetchMore() - Fetch another scan batch, optionally closing the scan

                Request another batch of rows to be retrieved from the scan.

Return Value:   0 if send succeeded, -1 otherwise.
Parameters:     emptyFrag: Root fragment for which to ask for another batch.
Remark:
******************************************************************************/
int NdbQueryImpl::sendFetchMore(NdbWorker *workers[], Uint32 cnt,
                                bool forceSend) {
  assert(getQueryDef().isScanQuery());

  for (Uint32 i = 0; i < cnt; i++) {
    NdbWorker *worker = workers[i];
    assert(worker->isFragBatchComplete());
    assert(!worker->finalBatchReceived());
    worker->prepareNextReceiveSet();
  }

  Ndb &ndb = *getNdbTransaction().getNdb();
  NdbApiSignal tSignal(&ndb);
  tSignal.setSignal(GSN_SCAN_NEXTREQ, refToBlock(m_scanTransaction->m_tcRef));
  ScanNextReq *const scanNextReq =
      CAST_PTR(ScanNextReq, tSignal.getDataPtrSend());

  assert(m_scanTransaction);
  const Uint64 transId = m_scanTransaction->getTransactionId();

  DBUG_PRINT("info", ("Send SCAN_NEXTREQ: NdbQueryOperation"));

  scanNextReq->apiConnectPtr = m_scanTransaction->theTCConPtr;
  scanNextReq->stopScan = 0;
  scanNextReq->transId1 = (Uint32)transId;
  scanNextReq->transId2 = (Uint32)(transId >> 32);
  tSignal.setLength(ScanNextReq::SignalLength);

  FetchMoreTcIdIterator receiverIdIter(workers, cnt);

  GenericSectionPtr secs[1];
  secs[ScanNextReq::ReceiverIdsSectionNum].sectionIter = &receiverIdIter;
  secs[ScanNextReq::ReceiverIdsSectionNum].sz = cnt;

  NdbImpl *impl = ndb.theImpl;
  Uint32 nodeId = m_transaction.getConnectedNodeId();
  Uint32 seq = m_transaction.theNodeSequence;

  /* This part needs to be done under mutex due to synchronization with
   * receiver thread.
   */
  PollGuard poll_guard(*impl);

  if (unlikely(hasReceivedError())) {
    // Errors arrived in between ::await released mutex, and sendFetchMore
    // grabbed it
    return -1;
  }
  if (impl->getNodeSequence(nodeId) != seq ||
      impl->sendSignal(&tSignal, nodeId, secs, 1) != 0) {
    setErrorCode(Err_NodeFailCausedAbort);
    return -1;
  }
  impl->do_forceSend(forceSend);

  m_pendingWorkers += cnt;
  assert(m_pendingWorkers <= getWorkerCount());

  return 0;
}  // NdbQueryImpl::sendFetchMore()

int NdbQueryImpl::closeTcCursor(bool forceSend) {
  assert(getQueryDef().isScanQuery());

  NdbImpl *const ndb = m_transaction.getNdb()->theImpl;
  const Uint32 timeout = ndb->get_waitfor_timeout();
  const Uint32 nodeId = m_transaction.getConnectedNodeId();
  const Uint32 seq = m_transaction.theNodeSequence;
  bool timeoutCase = (m_error.code == Err_ReceiveTimedOut);

  /* This part needs to be done under mutex due to synchronization with
   * receiver thread.
   */
  PollGuard poll_guard(*ndb);

  if (unlikely(ndb->getNodeSequence(nodeId) != seq)) {
    setErrorCode(Err_NodeFailCausedAbort);
    return -1;  // Transporter disconnected and reconnected, no need to close
  }

  /* Wait for outstanding scan results from current batch fetch */
  while (m_pendingWorkers > 0)
  {
    const FetchResult result = static_cast<FetchResult>
        (poll_guard.wait_scan(3*timeout,
                              nodeId,
                              forceSend,
                              &ndb->m_start_time));

    if (unlikely(ndb->getNodeSequence(nodeId) != seq))
      setFetchTerminated(Err_NodeFailCausedAbort, false);
    else if (unlikely(result != FetchResult_ok)) {
      if (result == FetchResult_timeOut) {
        setFetchTerminated(Err_ReceiveTimedOut, true);
        timeoutCase = true;
      } else
        setFetchTerminated(Err_NodeFailCausedAbort, false);
    }
    if (hasReceivedError()) {
      break;
    }
  }  // while

  assert(m_pendingWorkers == 0);
  NdbWorker::clear(m_workers, m_workerCount);
  m_errorReceived = 0;  // Clear errors caused by previous fetching
  m_error.code = 0;

  if (m_finalWorkers < getWorkerCount())  // TC has an open scan cursor.
  {
    /* Send SCAN_NEXTREQ(close) */
    const int error = sendClose(m_transaction.getConnectedNodeId());
    if (unlikely(error)) return error;

    assert(m_finalWorkers + m_pendingWorkers == getWorkerCount());

    /* Wait for close to be confirmed: */
    while (m_pendingWorkers > 0)
    {
      const FetchResult result = static_cast<FetchResult>
          (poll_guard.wait_scan(3*timeout,
                                nodeId,
                                forceSend,
                                &ndb->m_start_time));

      if (unlikely(ndb->getNodeSequence(nodeId) != seq))
        setFetchTerminated(Err_NodeFailCausedAbort, false);
      else if (unlikely(result != FetchResult_ok)) {
        if (result == FetchResult_timeOut) {
          setFetchTerminated(Err_ReceiveTimedOut, true);
          g_eventLogger->info(
              "NdbQueryOperation :: closeTcCursor() 4008 scan close failed");
          /* Kernel ApiConnectRecord in unknown state, cannot use it */
          /* Still connected, so request TC to release */
          m_scanTransaction->theForceReleaseOnClose = true;
        } else
          setFetchTerminated(Err_NodeFailCausedAbort, false);
      }
      if (hasReceivedError()) {
        break;
      }
    }  // while
  }    // if

  if (unlikely(timeoutCase && (m_error.code == 0))) {
    g_eventLogger->info(
        "NdbQueryOperation :: closeTcCursor() Successfully closed scan after "
        "timeout");
  }

  return 0;
}  // NdbQueryImpl::closeTcCursor

/*
 * This method is called with the PollGuard mutex held on the transporter.
 */
int NdbQueryImpl::sendClose(int nodeId) {
  assert(m_finalWorkers < getWorkerCount());
  m_pendingWorkers = getWorkerCount() - m_finalWorkers;

  Ndb &ndb = *m_transaction.getNdb();
  NdbApiSignal tSignal(&ndb);
  tSignal.setSignal(GSN_SCAN_NEXTREQ, refToBlock(m_scanTransaction->m_tcRef));
  ScanNextReq *const scanNextReq =
      CAST_PTR(ScanNextReq, tSignal.getDataPtrSend());

  assert(m_scanTransaction);
  const Uint64 transId = m_scanTransaction->getTransactionId();

  scanNextReq->apiConnectPtr = m_scanTransaction->theTCConPtr;
  scanNextReq->stopScan = true;
  scanNextReq->transId1 = (Uint32)transId;
  scanNextReq->transId2 = (Uint32)(transId >> 32);
  tSignal.setLength(ScanNextReq::SignalLength);

  NdbImpl *impl = ndb.theImpl;
  return impl->sendSignal(&tSignal, nodeId);

}  // NdbQueryImpl::sendClose()

int NdbQueryImpl::isPrunable(bool &prunable) {
  if (m_prunability == Prune_Unknown) {
    const int error = getRoot().getQueryOperationDef().checkPrunable(
        m_keyInfo, m_shortestBound, prunable, m_pruneHashVal);
    if (unlikely(error != 0)) {
      prunable = false;
      setErrorCode(error);
      return -1;
    }
    m_prunability = prunable ? Prune_Yes : Prune_No;
  }
  prunable = (m_prunability == Prune_Yes);
  return 0;
}

/****************
 * NdbQueryImpl::OrderedFragSet methods.
 ***************/

NdbQueryImpl::OrderedFragSet::OrderedFragSet()
    : m_capacity(0),
      m_activeWorkerCount(0),
      m_fetchMoreWorkerCount(0),
      m_finalResultReceivedCount(0),
      m_finalResultConsumedCount(0),
      m_ordering(NdbQueryOptions::ScanOrdering_void),
      m_keyRecord(nullptr),
      m_resultRecord(nullptr),
      m_resultMask(nullptr),
      m_activeWorkers(nullptr),
      m_fetchMoreWorkers(nullptr) {}

NdbQueryImpl::OrderedFragSet::~OrderedFragSet() {
  m_activeWorkers = nullptr;
  m_fetchMoreWorkers = nullptr;
}

void NdbQueryImpl::OrderedFragSet::clear() {
  m_activeWorkerCount = 0;
  m_fetchMoreWorkerCount = 0;
}

void NdbQueryImpl::OrderedFragSet::prepare(
    NdbBulkAllocator &allocator, NdbQueryOptions::ScanOrdering ordering,
    int capacity, const NdbRecord *keyRecord, const NdbRecord *resultRecord,
    const unsigned char *resultMask) {
  assert(m_activeWorkers == nullptr);
  assert(m_capacity == 0);
  assert(ordering != NdbQueryOptions::ScanOrdering_void);

  if (capacity > 0) {
    m_capacity = capacity;

    m_activeWorkers =
        reinterpret_cast<NdbWorker **>(allocator.allocObjMem(capacity));
    memset(m_activeWorkers, 0, capacity * sizeof(NdbWorker *));

    m_fetchMoreWorkers =
        reinterpret_cast<NdbWorker **>(allocator.allocObjMem(capacity));
    memset(m_fetchMoreWorkers, 0, capacity * sizeof(NdbWorker *));
  }
  m_ordering = ordering;
  m_keyRecord = keyRecord;
  m_resultRecord = resultRecord;
  m_resultMask = resultMask;
}  // OrderedFragSet::prepare()

/**
 *  Get current NdbWorker which to return results from.
 *  Logic relies on that ::reorganize() is called whenever the current
 *  NdbWorker is advanced to next result. This will eliminate
 *  empty NdbWorkers from the OrderedFragSet object
 */
NdbWorker *NdbQueryImpl::OrderedFragSet::getCurrent() const {
  if (m_ordering != NdbQueryOptions::ScanOrdering_unordered) {
    /**
     * Must have tuples for each (non-completed) worker when doing ordered
     * scan.
     */
    if (unlikely(m_activeWorkerCount + m_finalResultConsumedCount <
                 m_capacity)) {
      return nullptr;
    }
  }

  if (unlikely(m_activeWorkerCount == 0)) {
    return nullptr;
  } else {
    assert(!m_activeWorkers[m_activeWorkerCount - 1]->isEmpty());
    return m_activeWorkers[m_activeWorkerCount - 1];
  }
}  // OrderedFragSet::getCurrent()

/**
 *  Keep the set of worker results ordered, both with respect to
 *  specified ScanOrdering, and such that NdbWorkers which become
 *  empty are removed from  m_activeWorkers[].
 *  Thus, ::getCurrent() should be as lightweight as possible and only has
 *  to return the 'next' available from array wo/ doing any housekeeping.
 */
void NdbQueryImpl::OrderedFragSet::reorganize() {
  assert(m_activeWorkerCount > 0);
  NdbWorker *const worker = m_activeWorkers[m_activeWorkerCount - 1];

  // Remove the current worker if the batch has been emptied.
  if (worker->isEmpty()) {
    /**
     * MT-note: Although ::finalBatchReceived() normally requires mutex,
     * its safe to call it here wo/ mutex as:
     *
     *  - 'not hasRequestedMore()' guaranty that there can't be any
     *     receiver thread simultaneously accessing the mutex protected members.
     *  -  As this worker has already been added to (the mutex protected)
     *     class OrderedFragSet, we know that the mutex has been
     *     previously set for this 'frag'. This would have resolved
     *     any cache coherency problems related to mt'ed access to
     *     'worker->finalBatchReceived()'.
     */
    if (!worker->hasRequestedMore() && worker->finalBatchReceived()) {
      assert(m_finalResultReceivedCount > m_finalResultConsumedCount);
      m_finalResultConsumedCount++;
    }

    /**
     * Without doublebuffering we can't 'fetchMore' from workers until
     * the current ResultSet has been consumed by application.
     * (Compared to how ::prepareMoreResults() immediately 'fetchMore')
     */
    else if (!useDoubleBuffers) {
      m_fetchMoreWorkers[m_fetchMoreWorkerCount++] = worker;
    }
    m_activeWorkerCount--;
  }

  // Reorder worker results if add'ed nonEmpty worker to a sorted scan.
  else if (m_ordering != NdbQueryOptions::ScanOrdering_unordered) {
    /**
     * This is a sorted scan. There are more data to be read from
     * m_activeWorkers[m_activeWorkerCount-1]. Move it to its proper place.
     *
     * Use binary search to find the largest record that is smaller than or
     * equal to m_activeWorkers[m_activeWorkerCount-1].
     */
    int first = 0;
    int last = m_activeWorkerCount - 1;
    int middle = (first + last) / 2;

    while (first < last) {
      assert(middle < m_activeWorkerCount);
      const int cmpRes = compare(*worker, *m_activeWorkers[middle]);
      if (cmpRes < 0) {
        first = middle + 1;
      } else if (cmpRes == 0) {
        last = first = middle;
      } else {
        last = middle;
      }
      middle = (first + last) / 2;
    }

    // Move into correct sorted position
    if (middle < m_activeWorkerCount - 1) {
      assert(compare(*worker, *m_activeWorkers[middle]) >= 0);
      memmove(m_activeWorkers + middle + 1, m_activeWorkers + middle,
              (m_activeWorkerCount - middle - 1) * sizeof(NdbWorker *));
      m_activeWorkers[middle] = worker;
    }
    assert(verifySortOrder());
  }
  assert(m_activeWorkerCount + m_finalResultConsumedCount <= m_capacity);
  assert(m_fetchMoreWorkerCount + m_finalResultReceivedCount <= m_capacity);
}  // OrderedFragSet::reorganize()

void NdbQueryImpl::OrderedFragSet::add(NdbWorker &worker) {
  assert(m_activeWorkerCount + m_finalResultConsumedCount < m_capacity);

  m_activeWorkers[m_activeWorkerCount++] = &worker;  // Add avail worker
  reorganize();                                      // Move into position
}  // OrderedFragSet::add()

/**
 * Scan workers[] for fragments which has received a ResultSet batch.
 * Add these to m_applFrags (Require mutex protection)
 */
void NdbQueryImpl::OrderedFragSet::prepareMoreResults(NdbWorker workers[],
                                                      Uint32 cnt) {
  for (Uint32 workerNo = 0; workerNo < cnt; workerNo++) {
    NdbWorker &worker = workers[workerNo];
    if (worker.isEmpty() &&        // Current ResultSet is empty
        worker.hasReceivedMore())  // Another ResultSet is available
    {
      if (worker.finalBatchReceived()) {
        m_finalResultReceivedCount++;
      }
      /**
       * When doublebuffered fetch is active:
       * Received worker results is a candidates for immediate prefetch.
       */
      else if (useDoubleBuffers) {
        m_fetchMoreWorkers[m_fetchMoreWorkerCount++] = &worker;
      }  // useDoubleBuffers

      worker.grabNextResultSet();  // Get new ResultSet.
      add(worker);                 // Make avail. to appl. thread
    }
  }  // for all 'workers[]'

  assert(m_activeWorkerCount + m_finalResultConsumedCount <= m_capacity);
  assert(m_fetchMoreWorkerCount + m_finalResultReceivedCount <= m_capacity);
}  // OrderedFragSet::prepareMoreResults()

/**
 * Determine if a ::sendFetchMore() should be requested at this point.
 */
Uint32 NdbQueryImpl::OrderedFragSet::getFetchMore(NdbWorker **&workers) {
  /**
   * Decides (pre-)fetch strategy:
   *
   *  1) No doublebuffered ResultSets: Immediately request prefetch.
   *     (This is fetches related to 'isEmpty' fragments)
   *  2) If ordered ResultSets; Immediately request prefetch.
   *     (Need rows from all fragments to do sort-merge)
   *  3) When unordered, reduce #NEXTREQs to TC by avoid prefetch
   *     until there are pending request to all datanodes having more
   *     ResultSets
   */
  if (m_fetchMoreWorkerCount > 0 &&
      (!useDoubleBuffers ||                                      // 1)
       m_ordering != NdbQueryOptions::ScanOrdering_unordered ||  // 2)
       m_fetchMoreWorkerCount + m_finalResultReceivedCount >=
           m_capacity))  // 3)
  {
    const int cnt = m_fetchMoreWorkerCount;
    workers = m_fetchMoreWorkers;
    m_fetchMoreWorkerCount = 0;
    return cnt;
  }
  return 0;
}

bool NdbQueryImpl::OrderedFragSet::verifySortOrder() const {
  for (int i = 0; i < m_activeWorkerCount - 1; i++) {
    if (compare(*m_activeWorkers[i], *m_activeWorkers[i + 1]) < 0) {
      assert(false);
      return false;
    }
  }
  return true;
}

/**
 * Compare frags such that f1<f2 if f1 is empty but f2 is not.
 * - Otherwise compare record contents.
 * @return negative if frag1<frag2, 0 if frag1 == frag2, otherwise positive.
 */
int NdbQueryImpl::OrderedFragSet::compare(const NdbWorker &worker1,
                                          const NdbWorker &worker2) const {
  assert(m_ordering != NdbQueryOptions::ScanOrdering_unordered);

  /* f1<f2 if f1 is empty but f2 is not.*/
  if (worker1.isEmpty()) {
    if (!worker2.isEmpty()) {
      return -1;
    } else {
      return 0;
    }
  }

  /* Neither stream is empty so we must compare records.*/
  const Uint32 rootOp = worker1.rootOpNo();
  return compare_ndbrecord(
      &worker1.getResultStream(rootOp).getReceiver(),
      &worker2.getResultStream(rootOp).getReceiver(), m_keyRecord, m_resultRecord,
      m_resultMask, m_ordering == NdbQueryOptions::ScanOrdering_descending,
      false);
}

////////////////////////////////////////////////////
/////////  NdbQueryOperationImpl methods ///////////
////////////////////////////////////////////////////

NdbQueryOperationImpl::NdbQueryOperationImpl(
    NdbQueryImpl &queryImpl, const NdbQueryOperationDefImpl &def)
    : m_interface(*this),
      m_magic(MAGIC),
      m_queryImpl(queryImpl),
      m_operationDef(def),
      m_parent(nullptr),
      m_children(0),
      m_dependants(0),
      m_params(),
      m_resultBuffer(nullptr),
      m_resultRef(nullptr),
      m_isRowNull(true),
      m_ndbRecord(nullptr),
      m_read_mask(nullptr),
      m_firstRecAttr(nullptr),
      m_lastRecAttr(nullptr),
      m_ordering(NdbQueryOptions::ScanOrdering_unordered),
      m_interpretedCode(nullptr),
      m_diskInUserProjection(false),
      m_parallelism(def.getOpNo() == 0 ? Parallelism_max
                                       : Parallelism_adaptive),
      m_rowSize(0xffffffff),
      m_maxBatchRows(0),
      m_maxBatchBytes(0),
      m_resultBufferSize(0) {
  if (m_children.expand(def.getNoOfChildOperations())) {
    // Memory allocation during Vector::expand() failed.
    queryImpl.setErrorCode(Err_MemoryAlloc);
    return;
  }
  // Fill in operations parent refs, and append it as child of its parent
  const NdbQueryOperationDefImpl *parent = def.getParentOperation();
  if (parent != nullptr) {
    const Uint32 ix = parent->getOpNo();
    assert(ix < def.getOpNo());
    m_parent = &m_queryImpl.getQueryOperation(ix);
    const int res = m_parent->m_children.push_back(this);
    UNUSED(res);
    /**
      Enough memory should have been allocated when creating
      m_parent->m_children, so res!=0 should never happen.
    */
    assert(res == 0);
  }

  // Register the extra 'out of branch' (!isChildOf()) dependencies.
  // If we are not an ancestor of the 'first' treeNode of the join-nest
  // we are embedded within, we need to added to 'm_dependants' as
  // such an 'out of branch' dependent for this 'first_inner'
  const NdbQueryOperationDefImpl *firstInEmbeddingNestDef =
      def.getFirstInEmbeddingNest();
  if (firstInEmbeddingNestDef != nullptr &&
      !def.isChildOf(firstInEmbeddingNestDef)) {
    const Uint32 ix = firstInEmbeddingNestDef->getOpNo();
    NdbQueryOperationImpl *firstInEmbeddingNest =
        &m_queryImpl.getQueryOperation(ix);

    const int res = firstInEmbeddingNest->m_dependants.push_back(this);
    if (res != 0) {
      queryImpl.setErrorCode(Err_MemoryAlloc);
      return;
    }
  }

  if (def.getType() == NdbQueryOperationDef::OrderedIndexScan) {
    const NdbQueryOptions::ScanOrdering defOrdering =
        static_cast<const NdbQueryIndexScanOperationDefImpl &>(def)
            .getOrdering();
    if (defOrdering != NdbQueryOptions::ScanOrdering_void) {
      // Use value from definition, if one was set.
      m_ordering = defOrdering;
    }
  }
}

NdbQueryOperationImpl::~NdbQueryOperationImpl() {
  /**
   * We expect ::postFetchRelease to have deleted fetch related structures when
   * fetch completed. Either by fetching through last row, or calling ::close()
   * which forcefully terminates fetch
   */
  assert(m_firstRecAttr == nullptr);
  assert(m_interpretedCode == nullptr);
}  // NdbQueryOperationImpl::~NdbQueryOperationImpl()

/**
 * Release what we want need anymore after last available row has been
 * returned from datanodes.
 */
void NdbQueryOperationImpl::postFetchRelease() {
  Ndb *const ndb = m_queryImpl.getNdbTransaction().getNdb();
  NdbRecAttr *recAttr = m_firstRecAttr;
  while (recAttr != nullptr) {
    NdbRecAttr *saveRecAttr = recAttr;
    recAttr = recAttr->next();
    ndb->releaseRecAttr(saveRecAttr);
  }
  m_firstRecAttr = nullptr;

  // Set API exposed info to indicate NULL-row
  m_isRowNull = true;
  if (m_resultRef != nullptr) {
    *m_resultRef = nullptr;
  }

  // TODO: Consider if interpretedCode can be deleted imm. after ::doSend
  delete m_interpretedCode;
  m_interpretedCode = nullptr;
}  // NdbQueryOperationImpl::postFetchRelease()

Uint32 NdbQueryOperationImpl::getNoOfParentOperations() const {
  return (m_parent) ? 1 : 0;
}

NdbQueryOperationImpl &NdbQueryOperationImpl::getParentOperation(
    Uint32 i [[maybe_unused]]) const {
  assert(i == 0 && m_parent != nullptr);
  return *m_parent;
}
NdbQueryOperationImpl *NdbQueryOperationImpl::getParentOperation() const {
  return m_parent;
}

Uint32 NdbQueryOperationImpl::getNoOfChildOperations() const {
  return m_children.size();
}

NdbQueryOperationImpl &NdbQueryOperationImpl::getChildOperation(
    Uint32 i) const {
  return *m_children[i];
}

Int32 NdbQueryOperationImpl::getNoOfDescendantOperations() const {
  Int32 children = 0;

  for (unsigned i = 0; i < getNoOfChildOperations(); i++)
    children += 1 + getChildOperation(i).getNoOfDescendantOperations();

  return children;
}

SpjTreeNodeMask NdbQueryOperationImpl::getDependants() const {
  SpjTreeNodeMask dependants;
  dependants.set(getInternalOpNo());

  for (unsigned i = 0; i < m_children.size(); i++) {
    dependants.bitOR(m_children[i]->getDependants());
  }
  // Add extra dependants in sub-branches not being children
  for (unsigned i = 0; i < m_dependants.size(); i++) {
    dependants.bitOR(m_dependants[i]->getDependants());
  }
  return dependants;
}

Uint32 NdbQueryOperationImpl::getNoOfLeafOperations() const {
  if (getNoOfChildOperations() == 0) {
    return 1;
  } else {
    Uint32 sum = 0;
    for (unsigned i = 0; i < getNoOfChildOperations(); i++)
      sum += getChildOperation(i).getNoOfLeafOperations();

    return sum;
  }
}

NdbRecAttr*
NdbQueryOperationImpl::getValue(
                            const char* anAttrName,
                            char* resultBuffer,
                            Uint32 aStartPos,
                            Uint32 aSize) {
  if (unlikely(anAttrName == nullptr)) {
    getQuery().setErrorCode(QRY_REQ_ARG_IS_NULL);
    return nullptr;
  }
  const NdbColumnImpl *const column =
      m_operationDef.getTable().getColumn(anAttrName);
  if (unlikely(column == nullptr)) {
    getQuery().setErrorCode(Err_UnknownColumn);
    return nullptr;
  } else {
    return getValue(*column,
                    resultBuffer,
                    aStartPos,
                    aSize);
  }
}

NdbRecAttr*
NdbQueryOperationImpl::getValue(
                            Uint32 anAttrId, 
                            char* resultBuffer,
                            Uint32 aStartPos,
                            Uint32 aSize) {
  const NdbColumnImpl *const column =
      m_operationDef.getTable().getColumn(anAttrId);
  if (unlikely(column == nullptr)) {
    getQuery().setErrorCode(Err_UnknownColumn);
    return nullptr;
  } else {
    return getValue(*column,
                    resultBuffer,
                    aStartPos,
                    aSize);
  }
}

NdbRecAttr*
NdbQueryOperationImpl::getValue(
                            const NdbColumnImpl& column, 
                            char* resultBuffer,
                            Uint32 aStartPos,
                            Uint32 aSize) {
  if (unlikely(getQuery().m_state != NdbQueryImpl::Defined)) {
    int state = getQuery().m_state;
    assert(state >= NdbQueryImpl::Initial && state < NdbQueryImpl::Destructed);

    if (state == NdbQueryImpl::Failed)
      getQuery().setErrorCode(QRY_IN_ERROR_STATE);
    else
      getQuery().setErrorCode(QRY_ILLEGAL_STATE);
    DEBUG_CRASH();
    return nullptr;
  }
  Ndb *const ndb = getQuery().getNdbTransaction().getNdb();
  NdbRecAttr *const recAttr = ndb->getRecAttr();
  if (unlikely(recAttr == nullptr)) {
    getQuery().setErrorCode(Err_MemoryAlloc);
    return nullptr;
  }
  if (unlikely(aStartPos != 0 || aSize != 0)) {
    if (unlikely((column.getType() !=
                  NdbDictionary::Column::Longvarbinary) &&
                  column.getType() !=
                  NdbDictionary::Column::Varbinary)) {
      getQuery().setErrorCode(4566);
      return nullptr;
    }
  }
  if (unlikely(recAttr->setup(&column,
                              resultBuffer,
                              aStartPos,
                              aSize))) {
    ndb->releaseRecAttr(recAttr);
    getQuery().setErrorCode(Err_MemoryAlloc);
    return nullptr;
  }
  // Append to tail of list.
  if (m_firstRecAttr == nullptr) {
    m_firstRecAttr = recAttr;
  } else {
    m_lastRecAttr->next(recAttr);
  }
  m_lastRecAttr = recAttr;
  assert(recAttr->next() == nullptr);
  return recAttr;
}

int NdbQueryOperationImpl::setResultRowBuf(const NdbRecord *rec,
                                           char *resBuffer,
                                           const unsigned char *result_mask) {
  if (unlikely(rec == nullptr)) {
    getQuery().setErrorCode(QRY_REQ_ARG_IS_NULL);
    return -1;
  }
  if (unlikely(getQuery().m_state != NdbQueryImpl::Defined)) {
    int state = getQuery().m_state;
    assert(state >= NdbQueryImpl::Initial && state < NdbQueryImpl::Destructed);

    if (state == NdbQueryImpl::Failed)
      getQuery().setErrorCode(QRY_IN_ERROR_STATE);
    else
      getQuery().setErrorCode(QRY_ILLEGAL_STATE);
    DEBUG_CRASH();
    return -1;
  }
  if (rec->tableId !=
      static_cast<Uint32>(m_operationDef.getTable().getTableId())) {
    /* The key_record and attribute_record in primary key operation do not
       belong to the same table.*/
    getQuery().setErrorCode(Err_DifferentTabForKeyRecAndAttrRec);
    return -1;
  }
  if (unlikely(m_ndbRecord != nullptr)) {
    getQuery().setErrorCode(QRY_RESULT_ROW_ALREADY_DEFINED);
    return -1;
  }
  m_ndbRecord = rec;
  m_read_mask = result_mask;
  m_resultBuffer = resBuffer;
  return 0;
}

int NdbQueryOperationImpl::setResultRowRef(const NdbRecord *rec,
                                           const char *&bufRef,
                                           const unsigned char *result_mask) {
  m_resultRef = &bufRef;
  *m_resultRef = nullptr;  // No result row yet
  return setResultRowBuf(rec, nullptr, result_mask);
}

NdbQuery::NextResultOutcome NdbQueryOperationImpl::firstResult() {
  if (unlikely(getQuery().m_state < NdbQueryImpl::Executing ||
               getQuery().m_state >= NdbQueryImpl::Closed)) {
    int state = getQuery().m_state;
    assert(state >= NdbQueryImpl::Initial && state < NdbQueryImpl::Destructed);
    if (state == NdbQueryImpl::Failed)
      getQuery().setErrorCode(QRY_IN_ERROR_STATE);
    else
      getQuery().setErrorCode(QRY_ILLEGAL_STATE);
    DEBUG_CRASH();
    return NdbQuery::NextResult_error;
  }

  const NdbWorker *worker;

#if 0  // TODO ::firstResult() on root operation is unused, incomplete &
       // untested
  if (unlikely(getParentOperation()==NULL))
  {
    // Reset *all* ResultStreams, optionally order them, and find new current among them
    for( Uint32 i = 0; i<m_queryImpl.getRootFragCount(); i++)
    {
      m_resultStreams[i]->firstResult();
    }
    worker = m_queryImpl.m_applFrags.reorganize();
    assert(worker==NULL || worker==m_queryImpl.m_applFrags.getCurrent());
  }
  else
#endif

  {
    assert(getParentOperation() != nullptr);  // TODO, See above
    worker = m_queryImpl.m_applFrags.getCurrent();
  }

  if (worker != nullptr) {
    NdbResultStream &resultStream = worker->getResultStream(*this);
    if (resultStream.firstResult() != tupleNotFound) {
      if (unlikely(fetchRow(resultStream) == -1))
        return NdbQuery::NextResult_error;
      return NdbQuery::NextResult_gotRow;
    }
  }
  nullifyResult();
  return NdbQuery::NextResult_scanComplete;
}  // NdbQueryOperationImpl::firstResult()

NdbQuery::NextResultOutcome NdbQueryOperationImpl::nextResult(bool fetchAllowed,
                                                              bool forceSend) {
  DBUG_ENTER("NdbQueryOperationImpl::nextResult");
  if (unlikely(getQuery().m_state < NdbQueryImpl::Executing ||
               getQuery().m_state >= NdbQueryImpl::Closed)) {
    int state = getQuery().m_state;
    assert(state >= NdbQueryImpl::Initial && state < NdbQueryImpl::Destructed);
    if (state == NdbQueryImpl::Failed)
      getQuery().setErrorCode(QRY_IN_ERROR_STATE);
    else
      getQuery().setErrorCode(QRY_ILLEGAL_STATE);
    DEBUG_CRASH();
    DBUG_RETURN(NdbQuery::NextResult_error);
  }

  if (this == &getRoot()) {
    DBUG_PRINT("info", ("nextRootResult"));
    DBUG_RETURN(m_queryImpl.nextRootResult(fetchAllowed, forceSend));
  }
  /**
   * 'next' will never be able to return anything for a lookup operation.
   *  NOTE: This is a pure optimization shortcut!
   */
  else if (m_operationDef.isScanOperation()) {
    const NdbWorker *worker = m_queryImpl.m_applFrags.getCurrent();
    if (worker != nullptr) {
      NdbResultStream &resultStream = worker->getResultStream(*this);
      if (resultStream.nextResult() != tupleNotFound) {
        if (unlikely(fetchRow(resultStream) == -1))
          DBUG_RETURN(NdbQuery::NextResult_error);
        DBUG_RETURN(NdbQuery::NextResult_gotRow);
      }
    }
  }
  nullifyResult();
  DBUG_RETURN(NdbQuery::NextResult_scanComplete);
}  // NdbQueryOperationImpl::nextResult()

int NdbQueryOperationImpl::fetchRow(NdbResultStream &resultStream) {
  const char *buff = resultStream.getCurrentRow();
  assert(buff != nullptr ||
         (m_firstRecAttr == nullptr && m_ndbRecord == nullptr));

  m_isRowNull = false;
  if (m_firstRecAttr != nullptr) {
    // Retrieve any RecAttr (getValues()) for current row
    const int retVal =
        resultStream.getReceiver().get_AttrValues(m_firstRecAttr);
    assert(retVal == 0);
    if (unlikely(retVal == -1)) return -1;
  }
  if (m_ndbRecord != nullptr) {
    if (m_resultRef != nullptr) {
      // Set application pointer to point into internal buffer.
      *m_resultRef = buff;
    } else {
      assert(m_resultBuffer != nullptr);
      if (unlikely(m_resultBuffer == nullptr)) return -1;
      // Copy result to buffer supplied by application.
      memcpy(m_resultBuffer, buff, m_ndbRecord->m_row_size);
    }
  }
  return 0;
}  // NdbQueryOperationImpl::fetchRow

void NdbQueryOperationImpl::nullifyResult() {
  if (!m_isRowNull) {
    /* This operation gave no result for the current row.*/
    m_isRowNull = true;
    if (m_resultRef != nullptr) {
      // Set the pointer supplied by the application to NULL.
      *m_resultRef = nullptr;
    }
    /* We should not give any results for the descendants either.*/
    for (Uint32 i = 0; i < getNoOfChildOperations(); i++) {
      getChildOperation(i).nullifyResult();
    }
  }
}  // NdbQueryOperationImpl::nullifyResult

bool NdbQueryOperationImpl::isRowNULL() const { return m_isRowNull; }

static bool isSetInMask(const unsigned char *mask, int bitNo) {
  return mask[bitNo >> 3] & 1 << (bitNo & 7);
}

int NdbQueryOperationImpl::serializeProject(Uint32Buffer &attrInfo) {
  Uint32 startPos = attrInfo.getSize();
  attrInfo.append(0U);  // Temp write firste 'length' word, update later

  /**
   * If the columns in the projections are specified as
   * in NdbRecord format, attrId are assumed to be ordered ascending.
   * In this form the projection spec. can be packed as
   * a single bitmap.
   */
  if (m_ndbRecord != nullptr) {
    Bitmask<MAXNROFATTRIBUTESINWORDS> readMask;
    Uint32 requestedCols = 0;
    Uint32 maxAttrId = 0;

    for (Uint32 i = 0; i < m_ndbRecord->noOfColumns; i++) {
      const NdbRecord::Attr *const col = &m_ndbRecord->columns[i];
      Uint32 attrId = col->attrId;

      if (m_read_mask == nullptr || isSetInMask(m_read_mask, i)) {
        if (attrId > maxAttrId) maxAttrId = attrId;

        readMask.set(attrId);
        requestedCols++;

        const NdbColumnImpl *const column =
            getQueryOperationDef().getTable().getColumn(col->column_no);
        if (column->getStorageType() == NDB_STORAGETYPE_DISK) {
          m_diskInUserProjection = true;
        }
      }
    }

    // Test for special case, get all columns:
    if (requestedCols == (unsigned)m_operationDef.getTable().getNoOfColumns()) {
      Uint32 ah;
      AttributeHeader::init(&ah, AttributeHeader::READ_ALL, requestedCols);
      attrInfo.append(ah);
    } else if (requestedCols > 0) {
      /* Serialize projection as a bitmap.*/
      const Uint32 wordCount = 1 + maxAttrId / 32;  // Size of mask.
      Uint32 *dst = attrInfo.alloc(wordCount + 1);
      AttributeHeader::init(dst, AttributeHeader::READ_PACKED, 4 * wordCount);
      memcpy(dst + 1, &readMask, 4 * wordCount);
    }
  }  // if (m_ndbRecord...)

  /** Projection is specified in RecAttr format.
   *  This may also be combined with the NdbRecord format.
   */
  const NdbRecAttr *recAttr = m_firstRecAttr;
  /* Serialize projection as a list of Attribute id's.*/
  while (recAttr) {
    Uint32 ah;
    AttributeHeader::init(&ah, recAttr->attrId(), 0);
    attrInfo.append(ah);
    Uint32 extraAI = recAttr->getPartialReadAI();
    if (unlikely(extraAI != 0)) {
      attrInfo.append(extraAI);
    }
    if (recAttr->getColumn()->getStorageType() == NDB_STORAGETYPE_DISK) {
      m_diskInUserProjection = true;
    }
    recAttr = recAttr->next();
  }

  if (needRangeNo()) {
    Uint32 ah;
    AttributeHeader::init(&ah, AttributeHeader::RANGE_NO, 0);
    attrInfo.append(ah);
  }

  const bool withCorrelation = getQueryDef().isScanQuery();
  if (withCorrelation) {
    Uint32 ah;
    AttributeHeader::init(&ah, AttributeHeader::CORR_FACTOR64, 0);
    attrInfo.append(ah);
  }

  // Size of projection in words.
  Uint32 length = attrInfo.getSize() - startPos - 1;
  attrInfo.put(startPos, length);
  return 0;
}  // NdbQueryOperationImpl::serializeProject

int NdbQueryOperationImpl::serializeParams(
    const NdbQueryParamValue *paramValues) {
  if (unlikely(paramValues == nullptr)) {
    return QRY_REQ_ARG_IS_NULL;
  }

  const NdbQueryOperationDefImpl &def = getQueryOperationDef();
  for (Uint32 i = 0; i < def.getNoOfParameters(); i++) {
    const NdbParamOperandImpl &paramDef = def.getParameter(i);
    const NdbQueryParamValue &paramValue = paramValues[paramDef.getParamIx()];

    /**
     *  Add parameter value to serialized data.
     *  Each value has a Uint32 length field (in bytes), followed by
     *  the actual value. Allocation is in Uint32 units with unused bytes
     *  zero padded.
     **/
    const Uint32 oldSize = m_params.getSize();
    m_params.append(0);  // Place holder for length.
    bool null;
    Uint32 len;
    const int error =
        paramValue.serializeValue(*paramDef.getColumn(), m_params, len, null);
    if (unlikely(error)) return error;
    if (unlikely(null)) return Err_KeyIsNULL;

    if (unlikely(m_params.isMemoryExhausted())) {
      return Err_MemoryAlloc;
    }
    // Back patch length field.
    m_params.put(oldSize, len);
  }
  return 0;
}  // NdbQueryOperationImpl::serializeParams

Uint32 NdbQueryOperationImpl ::calculateBatchedRows(
    const NdbQueryOperationImpl *closestScan) {
  const NdbQueryOperationImpl *myClosestScan;
  if (m_operationDef.isScanOperation()) {
    myClosestScan = this;
  } else {
    myClosestScan = closestScan;
  }

  Uint32 maxBatchRows = 0;
  if (myClosestScan != nullptr) {
    // To force usage of SCAN_NEXTREQ even for small scans resultsets
    if (DBUG_EVALUATE_IF("max_4rows_in_spj_batches", true, false)) {
      m_maxBatchRows = 4;
    } else if (DBUG_EVALUATE_IF("max_64rows_in_spj_batches", true, false)) {
      m_maxBatchRows = 64;
    } else if (enforcedBatchSize) {
      m_maxBatchRows = enforcedBatchSize;
    }

    const Ndb &ndb = *getQuery().getNdbTransaction().getNdb();

    /**
     * For each batch, a lookup operation must be able to receive as many rows
     * as the closest ancestor scan operation.
     * We must thus make sure that we do not set a batch size for the scan
     * that exceeds what any of its scan descendants can use.
     *
     * Ignore calculated 'batchByteSize'
     * here - Recalculated when building signal after max-batchRows has been
     * determined.
     */
    const Uint32 rootFragments =
        getQuery().getFragRoutingTable().getFragmentCount();
    Uint32 batchByteSize;
    /**
     * myClosestScan->m_maxBatchRows may be zero to indicate that we
     * should use default values, or non-zero if the application had an
     * explicit preference.
     * ::calculate_batch_size() will then use the configured 'batchSize'
     * values to set, or cap, #rows / #bytes in batch for *each fragment*.
     */
    maxBatchRows = myClosestScan->m_maxBatchRows;
    /* Use rootFragments as parallelism for both Parallelism_max and
     * Parallelism_adaptive. The latter is the default for scan ops
     * that are not op[0] (e.g. root scans displaced by CTE subtrees).
     * Passing the sentinel value directly would divide batch_byte_size
     * by ~4 billion, producing zero. */
    const Uint32 rootPar = getRoot().m_parallelism;
    const Uint32 effectivePar =
        (rootPar == Parallelism_max || rootPar == Parallelism_adaptive)
            ? rootFragments
            : rootPar;
    NdbReceiver::calculate_batch_size(*ndb.theImpl,
                                      effectivePar,
                                      maxBatchRows,
                                      batchByteSize,
                                      MAX_PARALLEL_OP_PER_SCAN_SPJ);
    assert(maxBatchRows > 0);
    assert(maxBatchRows <= batchByteSize);

    /**
     * There is a 12-bit implementation limit of how large
     * the 'parent-row-correlation-id' may be. Thus, if rows
     * from this scan may be 'parents', we have to
     * reduce number of rows retrieved in each batch.
     */
    if (m_children.size() > 0)  // Is a 'parent'
    {
      static const Uint32 max_batch_size_rows = 0x1000;
      const Uint32 fragsPerWorker = getQuery().m_fragsPerWorker;
      maxBatchRows = MIN(maxBatchRows, max_batch_size_rows / fragsPerWorker);
    }
  }

  // Find the largest value that is acceptable to all lookup descendants.
  for (Uint32 i = 0; i < m_children.size(); i++) {
    const Uint32 childMaxBatchRows =
        m_children[i]->calculateBatchedRows(myClosestScan);
    maxBatchRows = MIN(maxBatchRows, childMaxBatchRows);
  }

  if (m_operationDef.isScanOperation()) {
    // Use this value for current op and all lookup descendants.
    m_maxBatchRows = maxBatchRows;
    // Return max(Unit32) to avoid interfering with batch size calculation
    // for parent.
    return 0xffffffff;
  } else {
    return maxBatchRows;
  }
}  // NdbQueryOperationImpl::calculateBatchedRows

void NdbQueryOperationImpl::setBatchedRows(Uint32 batchedRows) {
  if (!m_operationDef.isScanOperation()) {
    /** Lookup operations should handle the same number of rows as
     * the closest scan ancestor.
     */
    m_maxBatchRows = batchedRows;
  }

  for (Uint32 i = 0; i < m_children.size(); i++) {
    m_children[i]->setBatchedRows(m_maxBatchRows);
  }
}

int NdbQueryOperationImpl::prepareAttrInfo(Uint32Buffer &attrInfo,
                                           const QueryNode *&queryNode) {
  const NdbQueryOperationDefImpl &def = getQueryOperationDef();
#ifdef DEBUG_CTE_API
  const Uint32 entrySize = attrInfo.getSize();
  DEB_CTE_API("prepareAttrInfo: op[%u] type=%d treeOp=%u treeLen=%u "
              "attrInfoPos=%u\n",
              def.getInternalOpNo(), (int)def.getType(),
              QueryNode::getOpType(queryNode->len),
              QueryNode::getLength(queryNode->len), entrySize);
#endif

  /**
   * Serialize parameters referred by this NdbQueryOperation.
   * Params for the complete NdbQuery is collected in a single
   * serializedParams chunk. Each operations params are
   * proceeded by 'length' for this operation.
   */
  if (def.getType() == NdbQueryOperationDef::UniqueIndexAccess) {
    // Reserve memory for LookupParameters, fill in contents later when
    // 'length' and 'requestInfo' has been calculated.
    Uint32 startPos = attrInfo.getSize();
    attrInfo.alloc(QN_LookupParameters::NodeSize);
    Uint32 requestInfo = 0;

    if (m_params.getSize() > 0) {
      // parameter values has been serialized as part of
      // NdbTransaction::createQuery() Only need to append it to rest of the
      // serialized arguments
      requestInfo |= DABits::PI_KEY_PARAMS;
      attrInfo.append(m_params);
    }

    QN_LookupParameters *param =
        reinterpret_cast<QN_LookupParameters *>(attrInfo.addr(startPos));
    if (unlikely(param == nullptr)) return Err_MemoryAlloc;

    param->requestInfo = requestInfo;
    param->resultData = getIdOfReceiver();
    Uint32 length = attrInfo.getSize() - startPos;
    if (unlikely(length > 0xFFFF)) {
      return QRY_DEFINITION_TOO_LARGE;  // Query definition too large.
    }
    QueryNodeParameters::setOpLen(param->len, QueryNodeParameters::QN_LOOKUP,
                                  length);

#ifdef __TRACE_SERIALIZATION
    ndbout << "Serialized params for index node " << getInternalOpNo() - 1
           << " : ";
    for (Uint32 i = startPos; i < attrInfo.getSize(); i++) {
      char buf[12];
      sprintf(buf, "%.8x", attrInfo.get(i));
      ndbout << buf << " ";
    }
    ndbout << endl;
#endif

    queryNode = QueryNode::nextQueryNode(queryNode);
  }  // if (UniqueIndexAccess ...

  // Reserve memory for LookupParameters, fill in contents later when
  // 'length' and 'requestInfo' has been calculated.
  Uint32 startPos = attrInfo.getSize();
  Uint32 requestInfo = 0;
  /**
   * Create QueryNodeParameters type matching each QueryNode.
   */
  const Uint32 type = QueryNode::getOpType(queryNode->len);
  const QueryNodeParameters::OpType paramType =
      (QueryNodeParameters::OpType)type;
  switch (paramType) {
    case QueryNodeParameters::QN_LOOKUP:
      assert(!def.isScanOperation());
      attrInfo.alloc(QN_LookupParameters::NodeSize);
      break;
    case QueryNodeParameters::QN_SCAN_FRAG:
      assert(def.isScanOperation());
      attrInfo.alloc(QN_ScanFragParameters::NodeSize);
      break;
    case QueryNodeParameters::QN_SCAN_INDEX_v1:
      assert(def.isScanOperation() && def.getOpNo() > 0);
      attrInfo.alloc(QN_ScanIndexParameters_v1::NodeSize);
      break;
    case QueryNodeParameters::QN_SCAN_FRAG_v1:
      assert(def.isScanOperation() && def.getOpNo() == 0);
      attrInfo.alloc(QN_ScanFragParameters_v1::NodeSize);
      break;
    case QueryNodeParameters::QN_CTE_SUBTREE:
      attrInfo.alloc(QN_CteSubtreeParameters::NodeSize);
      break;
    case QueryNodeParameters::QN_CTE_LOOKUP:
      attrInfo.alloc(QN_CteLookupParameters::NodeSize);
      break;
    case QueryNodeParameters::QN_CTE_SCAN:
      assert(def.isScanOperation());
      attrInfo.alloc(QN_CteScanParameters::NodeSize);
      break;
    default:
      assert(false);
  }

  // SPJ block assume PARAMS to be supplied before ATTR_LIST
  if (m_params.getSize() > 0 &&
      def.getType() != NdbQueryOperationDef::UniqueIndexAccess) {
    // parameter values has been serialized as part of
    // NdbTransaction::createQuery() Only need to append it to rest of the
    // serialized arguments
    requestInfo |= DABits::PI_KEY_PARAMS;
    attrInfo.append(m_params);
  }

  if (hasInterpretedCode()) {
    requestInfo |= DABits::PI_ATTR_INTERPRET;
    const int error = prepareInterpretedCode(attrInfo);
    if (unlikely(error)) {
      return error;
    }
  } else if (def.isAggregateLeaf() &&
             def.getOptions().getLinkedProjection().size() > 0) {
    /**
     * Aggregate leaf with linked parent columns needs a minimal interpreter
     * program (ExitOK) so that DBSPJ creates the 5-word interpreter header.
     * Without it, the linked data appended to the subroutine section has no
     * proper framing and DBTUP misinterprets it as column read attributes.
     */
    requestInfo |= DABits::PI_ATTR_INTERPRET;
    // PI_ATTR_INTERPRET format: [len_word, program...]
    // len_word = (subroutine_len << 16) | program_len
    attrInfo.append(1);     // program_len=1, subroutine_len=0
    attrInfo.append(0x12);  // Interpreter::ExitOK (opcode 18)
  }

  if (m_ndbRecord == nullptr && m_firstRecAttr == nullptr) {
    // Leaf operations with empty projections are not supported,
    // unless this is an aggregate leaf (results come via aggregation,
    // not through the normal row projection path).
    // has children (intermediate node providing linked attributes).
    if (getNoOfChildOperations() == 0 && !def.isAggregateLeaf() &&
        !def.isQueryAggregation() &&
        def.getType() != NdbQueryOperationDef::CteSubtree &&
        def.getType() != NdbQueryOperationDef::CteLookup &&
        !def.isCteEmbedded()) {
      return QRY_EMPTY_PROJECTION;
    }
  } else if (def.getType() != NdbQueryOperationDef::CteLookup &&
             def.getType() != NdbQueryOperationDef::CteScan) {
    /* Standard projection: serialize real table column reads.
     * CTE_LOOKUP and CTE_SCAN use virtual column IDs (0, 1, ...) added
     * directly in the QN_CTE_* switch cases below — skip real column
     * projection so the serialization order
     * (PI_ATTR_INTERPRET before PI_ATTR_LIST) matches what parseDA
     * expects. */
    requestInfo |= DABits::PI_ATTR_LIST;
    const int error = serializeProject(attrInfo);
    if (unlikely(error)) {
      return error;
    }
  }

  if (diskInUserProjection()) {
    requestInfo |= DABits::PI_DISK_ATTR;
  }

  Uint32 length = attrInfo.getSize() - startPos;
  if (unlikely(length > 0xFFFF)) {
    return QRY_DEFINITION_TOO_LARGE;  // Query definition too large.
  }

  switch (paramType) {
    case QueryNodeParameters::QN_LOOKUP: {
      QN_LookupParameters *param =
          reinterpret_cast<QN_LookupParameters *>(attrInfo.addr(startPos));
      if (unlikely(param == nullptr)) return Err_MemoryAlloc;

      param->requestInfo = requestInfo;
      param->resultData = getIdOfReceiver();
      QueryNodeParameters::setOpLen(param->len, paramType, length);
      break;
    }
    case QueryNodeParameters::QN_SCAN_FRAG: {
      QN_ScanFragParameters *param =
          reinterpret_cast<QN_ScanFragParameters *>(attrInfo.addr(startPos));
      if (unlikely(param == nullptr)) return Err_MemoryAlloc;

      Uint32 batchRows, batchByteSize;
      if (def.isCteEmbedded()) {
        // CTE-embedded scans: use fixed defaults since API-side batch
        // computation doesn't apply (DBSPJ handles the scan internally).
        batchRows = 256;
        batchByteSize = 65536;
      } else {
        const Uint32 fragsPerWorker = getQuery().m_fragsPerWorker;
        batchRows = getMaxBatchRows() * fragsPerWorker;
        batchByteSize = getMaxBatchBytes() * fragsPerWorker;
      }
      assert(batchRows <= batchByteSize);
      assert(m_parallelism == Parallelism_max ||
             m_parallelism == Parallelism_adaptive);
      if (m_parallelism == Parallelism_max) {
        requestInfo |= QN_ScanFragParameters::SFP_PARALLEL;
      }
      if (def.hasParamInPruneKey()) {
        requestInfo |= QN_ScanFragParameters::SFP_PRUNE_PARAMS;
      }
      if (getOrdering() != NdbQueryOptions::ScanOrdering_unordered) {
        if (def.isCteEmbedded()) {
          // CTE-embedded scans: pass descending flag via SFP_DESCENDING
          // so DBSPJ sets DescendingFlag on the SCAN_FRAGREQ to DBLQH.
          // SFP_SORTED_ORDER is not used here (it triggers SPJ merge-sort
          // which is root-only and not needed for CTE materialization).
          if (getOrdering() == NdbQueryOptions::ScanOrdering_descending) {
            requestInfo |= QN_ScanFragParameters::SFP_DESCENDING;
          }
        } else {
          requestInfo |= QN_ScanFragParameters::SFP_SORTED_ORDER;
          // Only supported for root yet.
          assert(this == &getRoot());
        }
      }

      param->requestInfo = requestInfo;
      param->resultData = getIdOfReceiver();
      param->batch_size_rows = batchRows;
      param->batch_size_bytes = batchByteSize;
      param->unused0 = 0;  // Future
      param->unused1 = 0;
      param->maxRows = def.getOptions().getMaxRows();
      QueryNodeParameters::setOpLen(param->len, paramType, length);
      break;
    }
    // Check deprecated QueryNode types last:
    case QueryNodeParameters::QN_SCAN_INDEX_v1:  // Deprecated
    {
      QN_ScanIndexParameters_v1 *param =
          reinterpret_cast<QN_ScanIndexParameters_v1 *>(
              attrInfo.addr(startPos));
      if (unlikely(param == nullptr)) return Err_MemoryAlloc;

      assert(m_parallelism == Parallelism_max ||
             m_parallelism == Parallelism_adaptive);
      if (m_parallelism == Parallelism_max) {
        requestInfo |= QN_ScanIndexParameters_v1::SIP_PARALLEL;
      }
      if (def.hasParamInPruneKey()) {
        requestInfo |= QN_ScanIndexParameters_v1::SIP_PRUNE_PARAMS;
      }
      param->requestInfo = requestInfo;

      // Get Batch sizes, assert that both values fit in param->batchSize.
      const Uint32 batchRows = getMaxBatchRows();
      const Uint32 batchByteSize = getMaxBatchBytes();

      assert(batchRows < (1 << QN_ScanIndexParameters_v1::BatchRowBits));
      assert(batchByteSize < (1 << (sizeof param->batchSize * 8 -
                                    QN_ScanIndexParameters_v1::BatchRowBits)));
      param->batchSize =
          (batchByteSize << QN_ScanIndexParameters_v1::BatchRowBits) |
          batchRows;
      param->resultData = getIdOfReceiver();
      QueryNodeParameters::setOpLen(param->len, paramType, length);
      break;
    }
    case QueryNodeParameters::QN_SCAN_FRAG_v1:  // Deprecated
    {
      assert(paramType == QueryNodeParameters::QN_SCAN_FRAG_v1);
      QN_ScanFragParameters_v1 *param =
          reinterpret_cast<QN_ScanFragParameters_v1 *>(attrInfo.addr(startPos));
      if (unlikely(param == nullptr)) return Err_MemoryAlloc;

      param->requestInfo = requestInfo;
      param->resultData = getIdOfReceiver();
      QueryNodeParameters::setOpLen(param->len, paramType, length);
      break;
    }
    case QueryNodeParameters::QN_CTE_SUBTREE: {
      QN_CteSubtreeParameters *param =
          reinterpret_cast<QN_CteSubtreeParameters *>(attrInfo.addr(startPos));
      if (unlikely(param == nullptr)) return Err_MemoryAlloc;
      param->requestInfo = 0;
      param->resultData = getIdOfReceiver();
      QueryNodeParameters::setOpLen(param->len, paramType, length);
      break;
    }
    case QueryNodeParameters::QN_CTE_LOOKUP: {
      /* CTE_LOOKUP needs PI_ATTR_INTERPRET (ExitOK or user filter) and
       * PI_ATTR_LIST (virtual column reads) so DBSPJ builds proper
       * AttrInfo with FLUSH_AI for CTE_LOOKUP_REQ result delivery.
       * Read numResultCols from the serialized QN_CteLookupNode. */
      {
        const QN_CteLookupNode *cteNode =
            reinterpret_cast<const QN_CteLookupNode *>(queryNode);
        const Uint32 numCols = cteNode->numResultCols;

        /* PI_ATTR_INTERPRET: when the user attached a WHERE-clause
         * filter via setInterpretedCode(), prepareInterpretedCode()
         * above already emitted [prog_len, user_program...] and set
         * the PI_ATTR_INTERPRET flag.  Appending another [1, ExitOK]
         * stub here would concatenate a second interpreter header and
         * produce a malformed AttrInfo section.  Only emit the stub
         * when no user program is present. */
        if (!hasInterpretedCode()) {
          requestInfo |= DABits::PI_ATTR_INTERPRET;
          Uint32 interpHeader = (0u << 16) | 1u;  // sub_len=0, prog_len=1
          attrInfo.append(interpHeader);
          attrInfo.append(Uint32(18));  // Interpreter::ExitOK = 18
        }

        // PI_ATTR_LIST: virtual column reads + CORR_FACTOR64.
        // CORR_FACTOR must be in the user projection (before FLUSH_AI)
        // so it's included in the TRANSID_AI sent to the API. DBSPJ's
        // parseDA adds FLUSH_AI after PI_ATTR_LIST for scan queries.
        requestInfo |= DABits::PI_ATTR_LIST;
        attrInfo.append(numCols + 1);  // virtual cols + CORR_FACTOR
        for (Uint32 c = 0; c < numCols; c++) {
          attrInfo.append(c << 16);  // AttributeHeader(attrId=c, size=0)
        }
        attrInfo.append(AttributeHeader::CORR_FACTOR64 << 16);
      }

      /* Resolve param pointer AFTER all appends — appends may
       * reallocate the buffer, invalidating earlier pointers. */
      QN_CteLookupParameters *param =
          reinterpret_cast<QN_CteLookupParameters *>(attrInfo.addr(startPos));
      if (unlikely(param == nullptr)) return Err_MemoryAlloc;
      param->requestInfo = requestInfo;
      param->resultData = getIdOfReceiver();
      length = attrInfo.getSize() - startPos;
      QueryNodeParameters::setOpLen(param->len, paramType, length);
      break;
    }
    case QueryNodeParameters::QN_CTE_SCAN: {
      /* CTE_SCAN needs the same PI_ATTR_INTERPRET (ExitOK or user
       * filter) + PI_ATTR_LIST (virtual column reads) setup as
       * CTE_LOOKUP so DBSPJ builds the proper AttrInfo for delivering
       * scanned CTE rows. Read numResultCols from the serialized
       * QN_CteScanNode. */
      {
        const QN_CteScanNode *cteNode =
            reinterpret_cast<const QN_CteScanNode *>(queryNode);
        const Uint32 numCols = cteNode->numResultCols;

        /* See QN_CTE_LOOKUP case above: only emit the ExitOK stub when
         * the user has not attached their own filter program; otherwise
         * prepareInterpretedCode() above has already written the user
         * program and appending another header produces a malformed
         * AttrInfo section. */
        if (!hasInterpretedCode()) {
          requestInfo |= DABits::PI_ATTR_INTERPRET;
          Uint32 interpHeader = (0u << 16) | 1u;  // sub_len=0, prog_len=1
          attrInfo.append(interpHeader);
          attrInfo.append(Uint32(18));  // Interpreter::ExitOK = 18
        }

        // PI_ATTR_LIST: virtual column reads + CORR_FACTOR64.
        requestInfo |= DABits::PI_ATTR_LIST;
        attrInfo.append(numCols + 1);  // virtual cols + CORR_FACTOR
        for (Uint32 c = 0; c < numCols; c++) {
          attrInfo.append(c << 16);  // AttributeHeader(attrId=c, size=0)
        }
        attrInfo.append(AttributeHeader::CORR_FACTOR64 << 16);
      }

      /* Resolve param pointer AFTER all appends — appends may
       * reallocate the buffer, invalidating earlier pointers. */
      QN_CteScanParameters *param =
          reinterpret_cast<QN_CteScanParameters *>(attrInfo.addr(startPos));
      if (unlikely(param == nullptr)) return Err_MemoryAlloc;
      param->requestInfo = requestInfo;
      param->resultData = getIdOfReceiver();
      length = attrInfo.getSize() - startPos;
      QueryNodeParameters::setOpLen(param->len, paramType, length);
      break;
    }
    default:
      assert(false);
  }
  DEB_CTE_API("prepareAttrInfo: op[%u] param header: [0]=0x%08x "
              "[1]=0x%08x [2]=0x%08x\n",
              def.getInternalOpNo(),
              attrInfo.get(startPos),
              attrInfo.get(startPos + 1),
              attrInfo.get(startPos + 2));

#ifdef __TRACE_SERIALIZATION
  ndbout << "Serialized params for node " << getInternalOpNo() << " : ";
  for (Uint32 i = startPos; i < attrInfo.getSize(); i++) {
    char buf[12];
    sprintf(buf, "%.8x", attrInfo.get(i));
    ndbout << buf << " ";
  }
  ndbout << endl;
#endif

  // Parameter values was appended to AttrInfo, shrink param buffer
  // to reduce memory footprint.
  m_params.releaseExtend();

  DEB_CTE_API("prepareAttrInfo: op[%u] done, wrote %u param words "
              "(attrInfo now %u)\n",
              def.getInternalOpNo(), attrInfo.getSize() - entrySize,
              attrInfo.getSize());

  queryNode = QueryNode::nextQueryNode(queryNode);
  return 0;
}  // NdbQueryOperationImpl::prepareAttrInfo

int NdbQueryOperationImpl::prepareKeyInfo(
    Uint32Buffer &keyInfo, const NdbQueryParamValue *actualParam) {
  assert(this == &getRoot());  // Should only be called for root operation.
#ifdef TRACE_SERIALIZATION
  int startPos = keyInfo.getSize();
#endif

  const NdbQueryOperationDefImpl::IndexBound *bounds =
      m_operationDef.getBounds();
  if (bounds) {
    const int error = prepareIndexKeyInfo(keyInfo, bounds, actualParam);
    if (unlikely(error)) return error;
  }

  const NdbQueryOperandImpl *const *keys = m_operationDef.getKeyOperands();
  if (keys) {
    const int error = prepareLookupKeyInfo(keyInfo, keys, actualParam);
    if (unlikely(error)) return error;
  }

  if (unlikely(keyInfo.isMemoryExhausted())) {
    return Err_MemoryAlloc;
  }

#ifdef TRACE_SERIALIZATION
  ndbout << "Serialized KEYINFO for NdbQuery root : ";
  for (Uint32 i = startPos; i < keyInfo.getSize(); i++) {
    char buf[12];
    sprintf(buf, "%.8x", keyInfo.get(i));
    ndbout << buf << " ";
  }
  ndbout << endl;
#endif

  return 0;
}  // NdbQueryOperationImpl::prepareKeyInfo

/**
 * Convert constant operand into sequence of words that may be sent to data
 * nodes.
 * @param constOp Operand to convert.
 * @param buffer Destination buffer.
 * @param len Will be set to length in bytes.
 * @return 0 if ok, otherwise error code.
 */
static int serializeConstOp(const NdbConstOperandImpl &constOp,
                            Uint32Buffer &buffer, Uint32 &len) {
  // Check that column->shrink_varchar() not specified, only used by mySQL
  // assert (!(column->flags & NdbDictionary::RecMysqldShrinkVarchar));
  buffer.skipRestOfWord();
  len = constOp.getSizeInBytes();
  Uint8 shortLen[2];
  switch (constOp.getColumn()->getArrayType()) {
    case NdbDictionary::Column::ArrayTypeFixed:
      buffer.appendBytes(constOp.getAddr(), len);
      break;

    case NdbDictionary::Column::ArrayTypeShortVar:
      // Such errors should have been caught in convert2ColumnType().
      assert(len <= 0xFF);
      shortLen[0] = (unsigned char)len;
      buffer.appendBytes(shortLen, 1);
      buffer.appendBytes(constOp.getAddr(), len);
      len += 1;
      break;

    case NdbDictionary::Column::ArrayTypeMediumVar:
      // Such errors should have been caught in convert2ColumnType().
      assert(len <= 0xFFFF);
      shortLen[0] = (unsigned char)(len & 0xFF);
      shortLen[1] = (unsigned char)(len >> 8);
      buffer.appendBytes(shortLen, 2);
      buffer.appendBytes(constOp.getAddr(), len);
      len += 2;
      break;

    default:
      assert(false);
  }
  if (unlikely(buffer.isMemoryExhausted())) {
    return Err_MemoryAlloc;
  }
  return 0;
}  // static serializeConstOp

static int appendBound(Uint32Buffer &keyInfo,
                       NdbIndexScanOperation::BoundType type,
                       const NdbQueryOperandImpl *bound,
                       const NdbQueryParamValue *actualParam) {
  Uint32 len = 0;

  keyInfo.append(type);
  const Uint32 oldSize = keyInfo.getSize();
  keyInfo.append(0);  // Place holder for AttributeHeader

  switch (bound->getKind()) {
    case NdbQueryOperandImpl::Const: {
      const NdbConstOperandImpl &constOp =
          static_cast<const NdbConstOperandImpl &>(*bound);

      const int error = serializeConstOp(constOp, keyInfo, len);
      if (unlikely(error)) return error;

      break;
    }
    case NdbQueryOperandImpl::Param: {
      const NdbParamOperandImpl *const paramOp =
          static_cast<const NdbParamOperandImpl *>(bound);
      const int paramNo = paramOp->getParamIx();
      assert(actualParam != nullptr);

      bool null;
      const int error = actualParam[paramNo].serializeValue(
          *paramOp->getColumn(), keyInfo, len, null);
      if (unlikely(error)) return error;
      if (unlikely(null)) return Err_KeyIsNULL;
      break;
    }
    case NdbQueryOperandImpl::Linked:  // Root operation cannot have linked
                                       // operands.
    default:
      assert(false);
  }

  // Back patch attribute header.
  keyInfo.put(oldSize,
              AttributeHeader(bound->getColumn()->m_attrId, len).m_value);

  return 0;
}  // static appendBound()

int NdbQueryOperationImpl::prepareIndexKeyInfo(
    Uint32Buffer &keyInfo, const NdbQueryOperationDefImpl::IndexBound *bounds,
    const NdbQueryParamValue *actualParam) {
  int startPos = keyInfo.getSize();
  if (bounds->lowKeys == 0 && bounds->highKeys == 0)  // No Bounds defined
    return 0;

  const unsigned key_count = (bounds->lowKeys >= bounds->highKeys)
                                 ? bounds->lowKeys
                                 : bounds->highKeys;

  for (unsigned keyNo = 0; keyNo < key_count; keyNo++) {
    NdbIndexScanOperation::BoundType bound_type;

    /* If upper and lower limit is equal, a single BoundEQ is sufficient */
    if (keyNo < bounds->lowKeys && keyNo < bounds->highKeys &&
        bounds->low[keyNo] == bounds->high[keyNo]) {
      /* Inclusive if defined, or matching rows can include this value */
      bound_type = NdbIndexScanOperation::BoundEQ;
      int error =
          appendBound(keyInfo, bound_type, bounds->low[keyNo], actualParam);
      if (unlikely(error)) return error;

    } else {
      /* If key is part of lower bound */
      if (keyNo < bounds->lowKeys) {
        /* Inclusive if defined, or matching rows can include this value */
        bound_type = bounds->lowIncl || keyNo + 1 < bounds->lowKeys
                         ? NdbIndexScanOperation::BoundLE
                         : NdbIndexScanOperation::BoundLT;

        int error =
            appendBound(keyInfo, bound_type, bounds->low[keyNo], actualParam);
        if (unlikely(error)) return error;
      }

      /* If key is part of upper bound */
      if (keyNo < bounds->highKeys) {
        /* Inclusive if defined, or matching rows can include this value */
        bound_type = bounds->highIncl || keyNo + 1 < bounds->highKeys
                         ? NdbIndexScanOperation::BoundGE
                         : NdbIndexScanOperation::BoundGT;

        int error =
            appendBound(keyInfo, bound_type, bounds->high[keyNo], actualParam);
        if (unlikely(error)) return error;
      }
    }
  }

  Uint32 length = keyInfo.getSize() - startPos;
  if (unlikely(keyInfo.isMemoryExhausted())) {
    return Err_MemoryAlloc;
  } else if (unlikely(length > 0xFFFF)) {
    return QRY_DEFINITION_TOO_LARGE;  // Query definition too large.
  } else if (likely(length > 0)) {
    keyInfo.put(startPos, keyInfo.get(startPos) | (length << 16));
  }

  m_queryImpl.m_shortestBound = (bounds->lowKeys <= bounds->highKeys)
                                    ? bounds->lowKeys
                                    : bounds->highKeys;
  return 0;
}  // NdbQueryOperationImpl::prepareIndexKeyInfo

int NdbQueryOperationImpl::prepareLookupKeyInfo(
    Uint32Buffer &keyInfo, const NdbQueryOperandImpl *const keys[],
    const NdbQueryParamValue *actualParam) {
  const int keyCount =
      m_operationDef.getIndex() != nullptr
          ? static_cast<int>(m_operationDef.getIndex()->getNoOfColumns())
          : m_operationDef.getTable().getNoOfPrimaryKeys();

  for (int keyNo = 0; keyNo < keyCount; keyNo++) {
    Uint32 dummy;

    switch (keys[keyNo]->getKind()) {
      case NdbQueryOperandImpl::Const: {
        const NdbConstOperandImpl *const constOp =
            static_cast<const NdbConstOperandImpl *>(keys[keyNo]);
        const int error = serializeConstOp(*constOp, keyInfo, dummy);
        if (unlikely(error)) return error;

        break;
      }
      case NdbQueryOperandImpl::Param: {
        const NdbParamOperandImpl *const paramOp =
            static_cast<const NdbParamOperandImpl *>(keys[keyNo]);
        int paramNo = paramOp->getParamIx();
        assert(actualParam != nullptr);

        bool null;
        const int error = actualParam[paramNo].serializeValue(
            *paramOp->getColumn(), keyInfo, dummy, null);

        if (unlikely(error)) return error;
        if (unlikely(null)) return Err_KeyIsNULL;
        break;
      }
      case NdbQueryOperandImpl::Linked:  // Root operation cannot have linked
                                         // operands.
      default:
        assert(false);
    }
  }

  if (unlikely(keyInfo.isMemoryExhausted())) {
    return Err_MemoryAlloc;
  }

  return 0;
}  // NdbQueryOperationImpl::prepareLookupKeyInfo

/* Handle non-fragmented TRANSID_AI signals for pushdown joins */
bool NdbQueryOperationImpl::execTRANSID_AI(const Uint32 *ptr, Uint32 len) {
  TupleCorrelation tupleCorrelation;
  NdbWorker *worker = m_queryImpl.m_workers;

  DEB_CTE_API("execTRANSID_AI: opNo=%u type=%d len=%u "
              "workerCount=%u\n",
              m_operationDef.getOpNo(),
              (int)m_operationDef.getType(),
              len, m_queryImpl.getWorkerCount());

  if (getQueryDef().isScanQuery()) {
    const CorrelationData correlData(ptr, len);
    const Uint32 receiverId = correlData.getRootReceiverId();

    DEB_CTE_API("execTRANSID_AI: corr receiverId=0x%x "
                "lastWord=0x%x secondLastWord=0x%x\n",
                receiverId, ptr[len-1], len >= 2 ? ptr[len-2] : 0);

    /** receiverId holds the Id of the receiver of the corresponding stream
     * of the root operation. We can thus find the correct worker
     * number.
     */
    worker = NdbWorker::receiverIdLookup(
        m_queryImpl.m_workers, m_queryImpl.getWorkerCount(), receiverId);
    if (unlikely(worker == nullptr)) {
      DEB_CTE_API("execTRANSID_AI: FAILED receiverIdLookup! "
                  "receiverId=0x%x, expected root receiverId=0x%x\n",
                  receiverId,
                  m_queryImpl.getWorkerCount() > 0
                  ? m_queryImpl.m_workers[0].getReceiverId() : 0);
      assert(false);
      return false;
    }
    DBUG_PRINT("info", ("receiverId: %u, worker: 0x%p", receiverId, worker));

    // Extract tuple correlation.
    tupleCorrelation = correlData.getTupleCorrelation();
    len -= CorrelationData::wordCount;
  }

  if (traceSignals) {
    ndbout << "NdbQueryOperationImpl::execTRANSID_AI()"
           << ", from workerNo=" << worker->getWorkerNo()
           << ", operation no: " << getQueryOperationDef().getInternalOpNo()
           << endl;
  }

  // Process result values.
  worker->getResultStream(*this).execTRANSID_AI(ptr, len, tupleCorrelation);
  if (unlikely(!worker->incrOutstandingResults(-1))) {
    m_queryImpl.setFetchTerminated(Err_OutstandingResultsMismatch, false);
    return false;
  }

  bool ret = false;
  if (worker->isFragBatchComplete()) {
    ret = m_queryImpl.handleBatchComplete(*worker);
  }

  if (false && traceSignals) {
    ndbout << "NdbQueryOperationImpl::execTRANSID_AI(): returns:" << ret
           << ", *this=" << *this << endl;
  }
  return ret;
}  // NdbQueryOperationImpl::execTRANSID_AI

/* Handle fragmented signal for pushdown joins */
bool NdbQueryOperationImpl::execTRANSID_AI(const Uint32 *ptr,
                                           Uint32 len,
                                           const NdbApiSignal *aSignal,
                                           Uint32 totalLen) {
  DBUG_ENTER("NdbQueryOperationImpl::execTRANSID_AI long");
  TupleCorrelation tupleCorrelation;
  NdbWorker *worker = m_queryImpl.m_workers;

  if (getQueryDef().isScanQuery()) {
    const Uint32 *tDataPtr = aSignal->getDataPtr();
    const Uint32 sigLen = aSignal->getLength();
    require(sigLen >= TransIdAILong::HeaderWithCorrelationLength);
    const CorrelationData correlData(
      tDataPtr, TransIdAILong::HeaderWithCorrelationLength);
    const Uint32 receiverId = correlData.getRootReceiverId();

    /** receiverId holds the Id of the receiver of the corresponding stream
     * of the root operation. We can thus find the correct worker
     * number.
     */
    worker = NdbWorker::receiverIdLookup(
        m_queryImpl.m_workers, m_queryImpl.getWorkerCount(), receiverId);
    if (unlikely(worker == nullptr)) {
      assert(false);
      DBUG_RETURN(false);
    }
    DBUG_PRINT("info", ("receiverId: %u, worker: 0x%p", receiverId, worker));

    // Extract tuple correlation.
    tupleCorrelation = correlData.getTupleCorrelation();
  }

  if (traceSignals) {
    ndbout << "NdbQueryOperationImpl::execTRANSID_AI()"
           << ", from workerNo=" << worker->getWorkerNo()
           << ", operation no: " << getQueryOperationDef().getInternalOpNo()
           << endl;
  }

  // Process result values.
  worker->getResultStream(*this).execTRANSID_AI(ptr,
                                                len,
                                                tupleCorrelation,
                                                aSignal,
                                                totalLen);
  if (!aSignal->isLastFragment()) {
    DBUG_RETURN(false);
  }
  if (unlikely(!worker->incrOutstandingResults(-1))) {
    m_queryImpl.setFetchTerminated(Err_OutstandingResultsMismatch, false);
    DBUG_RETURN(false);
  }

  bool ret = false;
  if (worker->isFragBatchComplete()) {
    ret = m_queryImpl.handleBatchComplete(*worker);
  }

  if (false && traceSignals) {
    ndbout << "NdbQueryOperationImpl::execTRANSID_AI(): returns:" << ret
           << ", *this=" << *this << endl;
  }
  DBUG_RETURN(ret);
}  // NdbQueryOperationImpl::execTRANSID_AI

bool NdbQueryOperationImpl::execTCKEYREF(const NdbApiSignal *aSignal) {
  if (traceSignals) {
    ndbout << "NdbQueryOperationImpl::execTCKEYREF()" << endl;
  }

  /* The SPJ block does not forward TCKEYREFs for trees with scan roots.*/
  assert(!getQueryDef().isScanQuery());

  const TcKeyRef *ref = CAST_CONSTPTR(TcKeyRef, aSignal->getDataPtr());
  if (ref->errorCode == TcKeyRef::WriteRateOverflowError) {
    getQuery().getNdbTransaction().rateOverflowError();
  }
  if (!getQuery().m_transaction.checkState_TransId(ref->transId)) {
#ifdef NDB_NO_DROPPED_SIGNAL
    abort();
#endif
    return false;
  }

  // Suppress 'TupleNotFound' status for child operations.
  if (&getRoot() == this ||
      ref->errorCode != static_cast<Uint32>(Err_TupleNotFound)) {
    if (aSignal->getLength() == TcKeyRef::SignalLength) {
      // Signal may contain additional error data
      getQuery().m_error.details = (char *)UintPtr(ref->errorData);
    }
    getQuery().setFetchTerminated(ref->errorCode, false);
  }

  NdbWorker &worker = getQuery().m_workers[0];

  /**
   * Error may be either a 'soft' or a 'hard' error.
   * 'Soft error' are regarded 'informational', and we are
   * allowed to continue execution of the query. A 'hard error'
   * will terminate query, close communication, and further
   * incoming signals to this NdbReceiver will be discarded.
   */
  switch (ref->errorCode) {
    case Err_TupleNotFound:   // 'Soft error' : Row not found
    case Err_FalsePredicate:  // 'Soft error' : Interpreter_exit_nok
    {
      /**
       * Need to update 'outstanding' count:
       * Compensate for children results not produced.
       * (doSend() assumed all child results to be materialized)
       */
      Uint32 cnt = 1;  // self
      cnt += getNoOfDescendantOperations();
      if (getNoOfChildOperations() > 0) {
        cnt += getNoOfLeafOperations();
      }
      if (unlikely(!worker.incrOutstandingResults(-Int32(cnt)))) {
        m_queryImpl.setFetchTerminated(Err_OutstandingResultsMismatch, false);
        return false;
      }
      break;
    }
    default:                           // 'Hard error':
      worker.throwRemainingResults();  // Terminate receive -> complete
  }

  bool ret = false;
  if (worker.isFragBatchComplete()) {
    ret = m_queryImpl.handleBatchComplete(worker);
  }

  if (traceSignals) {
    ndbout << "NdbQueryOperationImpl::execTCKEYREF(): returns:" << ret
           << ", *this=" << *this << endl;
  }
  return ret;
}  // NdbQueryOperationImpl::execTCKEYREF

bool NdbQueryOperationImpl::execSCAN_TABCONF(Uint32 tcPtrI, Uint32 rowCount,
                                             Uint32 moreMask, Uint32 activeMask,
                                             const NdbReceiver *receiver) {
  DBUG_ENTER("NdbQueryOperationImpl::execSCAN_TABCONF");
  DEB_CTE_API("execSCAN_TABCONF: recvId=0x%x tcPtrI=0x%x rowCount=%u "
              "moreMask=0x%x activeMask=0x%x workerCount=%u\n",
              receiver->getId(), tcPtrI, rowCount, moreMask, activeMask,
              m_queryImpl.getWorkerCount());
  assert((tcPtrI == RNIL && moreMask == 0) ||
         (tcPtrI != RNIL && moreMask != 0));
  assert(checkMagicNumber());
  // For now, only the root operation may be a scan.
  assert(&getRoot() == this);
  assert(m_operationDef.isScanOperation() || getQuery().getQueryDef().isScanQuery());

  NdbWorker *worker = NdbWorker::receiverIdLookup(
      m_queryImpl.m_workers, m_queryImpl.getWorkerCount(), receiver->getId());
  if (unlikely(worker == nullptr)) {
    DEB_CTE_API("execSCAN_TABCONF: FAILED receiverIdLookup recvId=0x%x\n",
                receiver->getId());
    assert(false);
    DBUG_RETURN(false);
  }

  if (traceSignals) {
    ndbout << "NdbQueryOperationImpl::execSCAN_TABCONF"
           << " from workerNo=" << worker->getWorkerNo() << " rows " << rowCount
           << " moreMask: H'" << hex << moreMask << " activeMask: H'" << hex
           << activeMask << " tcPtrI " << tcPtrI << endl;
  }
  assert(moreMask != 0 || activeMask == 0);

  // Prepare for SCAN_NEXTREQ, tcPtrI==RNIL, moreMask==0 -> EOF
  worker->setConfReceived(tcPtrI);
  worker->setRemainingSubScans(moreMask, activeMask);
  if (unlikely(!worker->incrOutstandingResults(rowCount))) {
    DEB_CTE_API("execSCAN_TABCONF: OutstandingResultsMismatch worker=%u "
                "recvId=0x%x rowCount=%u\n",
                worker->getWorkerNo(), receiver->getId(), rowCount);
    m_queryImpl.setFetchTerminated(Err_OutstandingResultsMismatch, false);
    DBUG_RETURN(false);
  }

  const bool fragComplete = worker->isFragBatchComplete();
  DEB_CTE_API("execSCAN_TABCONF: worker=%u recvId=0x%x rowCount=%u "
              "fragComplete=%d pendingWorkers=%u finalWorkers=%u\n",
              worker->getWorkerNo(), receiver->getId(), rowCount,
              (int)fragComplete, m_queryImpl.m_pendingWorkers,
              m_queryImpl.m_finalWorkers);
  bool ret = false;
  if (fragComplete) {
    /* This fragment is now complete */
    ret = m_queryImpl.handleBatchComplete(*worker);
  }
  if (false && traceSignals) {
    ndbout << "NdbQueryOperationImpl::execSCAN_TABCONF():, returns:" << ret
           << ", tcPtrI=" << tcPtrI << " rowCount=" << rowCount
           << " *this=" << *this << endl;
  }
  DBUG_PRINT("info", ("SCAN_TABCONF returns %u, rowCount: %u, tcPtrI: %u"
                      ", receiverId: %u, activeMask: 0x%x, moreMask: 0x%x",
    ret, rowCount, tcPtrI, receiver->getId(), activeMask, moreMask));
  DBUG_RETURN(ret);
}  // NdbQueryOperationImpl::execSCAN_TABCONF

int NdbQueryOperationImpl::setOrdering(NdbQueryOptions::ScanOrdering ordering) {
  if (getQueryOperationDef().getType() !=
      NdbQueryOperationDef::OrderedIndexScan) {
    getQuery().setErrorCode(QRY_WRONG_OPERATION_TYPE);
    return -1;
  }

  if (m_parallelism != Parallelism_max) {
    getQuery().setErrorCode(QRY_SEQUENTIAL_SCAN_SORTED);
    return -1;
  }

  if (static_cast<const NdbQueryIndexScanOperationDefImpl &>(
          getQueryOperationDef())
          .getOrdering() != NdbQueryOptions::ScanOrdering_void) {
    getQuery().setErrorCode(QRY_SCAN_ORDER_ALREADY_SET);
    return -1;
  }

  m_ordering = ordering;
  return 0;
}  // NdbQueryOperationImpl::setOrdering()

int NdbQueryOperationImpl::setInterpretedCode(const NdbInterpretedCode &code) {
  if (code.m_instructions_length == 0) {
    return 0;
  }

  const NdbTableImpl &table = getQueryOperationDef().getTable();
  // Check if operation and interpreter code use the same table
  if (unlikely(table.getTableId() != code.getTable()->getTableId() ||
               table_version_major(table.getObjectVersion()) !=
                   table_version_major(code.getTable()->getObjectVersion()))) {
    getQuery().setErrorCode(Err_InterpretedCodeWrongTab);
    return -1;
  }

  if (unlikely((code.m_flags & NdbInterpretedCode::Finalised) == 0)) {
    //  NdbInterpretedCode::finalise() not called.
    getQuery().setErrorCode(Err_FinaliseNotCalled);
    return -1;
  }

  // Allocate an interpreted code object if we do not have one already.
  if (likely(m_interpretedCode == nullptr)) {
    m_interpretedCode = new NdbInterpretedCode();

    if (unlikely(m_interpretedCode == nullptr)) {
      getQuery().setErrorCode(Err_MemoryAlloc);
      return -1;
    }
  }

  /*
   * Make a deep copy, such that 'code' can be destroyed when this method
   * returns.
   */
  const int error = m_interpretedCode->copy(code);
  if (unlikely(error)) {
    getQuery().setErrorCode(error);
    return -1;
  }
  return 0;
}  // NdbQueryOperationImpl::setInterpretedCode()

int NdbQueryOperationImpl::setParallelism(Uint32 parallelism) {
  if (!getQueryOperationDef().isScanOperation()) {
    getQuery().setErrorCode(QRY_WRONG_OPERATION_TYPE);
    return -1;
  } else if (getOrdering() == NdbQueryOptions::ScanOrdering_ascending ||
             getOrdering() == NdbQueryOptions::ScanOrdering_descending) {
    getQuery().setErrorCode(QRY_SEQUENTIAL_SCAN_SORTED);
    return -1;
  } else if (getQueryOperationDef().getOpNo() > 0) {
    getQuery().setErrorCode(Err_FunctionNotImplemented);
    return -1;
  } else if (parallelism < 1 || parallelism > NDB_PARTITION_MASK) {
    getQuery().setErrorCode(Err_ParameterError);
    return -1;
  }
  m_parallelism = parallelism;
  return 0;
}

int NdbQueryOperationImpl::setMaxParallelism() {
  if (!getQueryOperationDef().isScanOperation()) {
    getQuery().setErrorCode(QRY_WRONG_OPERATION_TYPE);
    return -1;
  }
  m_parallelism = Parallelism_max;
  return 0;
}

int NdbQueryOperationImpl::setAdaptiveParallelism() {
  if (!getQueryOperationDef().isScanOperation()) {
    getQuery().setErrorCode(QRY_WRONG_OPERATION_TYPE);
    return -1;
  } else if (getQueryOperationDef().getOpNo() == 0) {
    getQuery().setErrorCode(Err_FunctionNotImplemented);
    return -1;
  }
  m_parallelism = Parallelism_adaptive;
  return 0;
}

int NdbQueryOperationImpl::setBatchSize(Uint32 batchSize) {
  if (!getQueryOperationDef().isScanOperation()) {
    getQuery().setErrorCode(QRY_WRONG_OPERATION_TYPE);
    return -1;
  }
  if (this != &getRoot() &&
      batchSize < getQueryOperationDef().getTable().getFragmentCount()) {
    /** Each SPJ block instance will scan each fragment, so the batch size
     * cannot be smaller than the number of fragments.*/
    getQuery().setErrorCode(QRY_BATCH_SIZE_TOO_SMALL);
    return -1;
  }
  m_maxBatchRows = batchSize;
  return 0;
}

bool NdbQueryOperationImpl::hasInterpretedCode() const {
  return (m_interpretedCode && m_interpretedCode->m_instructions_length > 0) ||
         (getQueryOperationDef().getInterpretedCode() != nullptr);
}  // NdbQueryOperationImpl::hasInterpretedCode

int NdbQueryOperationImpl::prepareInterpretedCode(
    Uint32Buffer &attrInfo) const {
  const NdbInterpretedCode *interpretedCode =
      (m_interpretedCode && m_interpretedCode->m_instructions_length > 0)
          ? m_interpretedCode
          : getQueryOperationDef().getInterpretedCode();

  // There should be no subroutines in a filter.
  assert(interpretedCode->m_first_sub_instruction_pos == 0);
  assert(interpretedCode->m_instructions_length > 0);
  assert(interpretedCode->m_instructions_length <= 0xffff);

  // Allocate space for program and length field.
  Uint32 *const buffer =
      attrInfo.alloc(1 + interpretedCode->m_instructions_length);
  if (unlikely(buffer == nullptr)) {
    return Err_MemoryAlloc;
  }

  buffer[0] = interpretedCode->m_instructions_length;
  memcpy(buffer + 1, interpretedCode->m_buffer,
         interpretedCode->m_instructions_length * sizeof(Uint32));
  return 0;
}  // NdbQueryOperationImpl::prepareInterpretedCode

Uint32 NdbQueryOperationImpl::getIdOfReceiver() const {
  const NdbWorker &worker = m_queryImpl.m_workers[0];
  return worker.getResultStream(*this).getReceiver().getId();
}

Uint32 NdbQueryOperationImpl::getRowSize() const {
  // Check if row size has been computed yet.
  if (m_rowSize == 0xffffffff) {
    m_rowSize = NdbReceiver::ndbrecord_rowsize(m_ndbRecord, needRangeNo());
  }
  return m_rowSize;
}

Uint32 NdbQueryOperationImpl::getMaxBatchBytes() const {
  // CTE subtree containers and CTE-embedded ops don't need API-side
  // row buffers. Their batch parameters in prepareAttrInfo use fixed
  // defaults. DBSPJ handles their buffer allocation internally.
  if (m_operationDef.getType() == NdbQueryOperationDef::CteSubtree ||
      m_operationDef.isCteEmbedded()) {
    return 0;
  }
  // Check if batch buffer size has been computed yet.
  if (m_maxBatchBytes == 0) {
    Uint32 batchRows = getMaxBatchRows();
    Uint32 batchByteSize = 0;
    Uint32 batchFrags = getQuery().m_fragsPerWorker;

    // Set together with 'm_resultBufferSize'
    assert(m_resultBufferSize == 0);

    const Uint32 rootFragments =
        getQuery().getFragRoutingTable().getFragmentCount();

    if (m_operationDef.isScanOperation()) {
      const Ndb *const ndb = getQuery().getNdbTransaction().getNdb();
      const Uint32 parallelism = rootFragments;
      NdbReceiver::calculate_batch_size(*ndb->theImpl,
                                        parallelism,
                                        batchRows,
                                        batchByteSize,
                                        MAX_PARALLEL_OP_PER_SCAN_SPJ);
      assert(batchRows == getMaxBatchRows());

      /**
       * When LQH reads a scan batch, the size of the batch is limited
       * both to a maximal number of rows and a maximal number of bytes.
       * The latter limit is interpreted such that the batch ends when the
       * limit has been exceeded. Consequently, the buffer must be able to
       * hold max_no_of_bytes plus one extra row. In addition,  when the
       * SPJ block executes a (pushed) child scan operation, it scans a
       * number of fragments (possibly all) in parallel, and divides the
       * row and byte limits by the number of parallel fragments.
       * Consequently, a child scan operation may return max_no_of_bytes,
       * plus one extra row for each fragment.
       */
      if (getParentOperation() != nullptr) {
        batchFrags = rootFragments;
      } else {
        batchFrags = 1;
      }
    }

    AttributeMask readMask;
    if (m_ndbRecord != nullptr) {
      m_ndbRecord->copyMask(readMask.rep.data, m_read_mask);
    }

    const bool withCorrelation = getQueryDef().isScanQuery();

    m_maxBatchBytes = batchByteSize;
    NdbReceiver::result_bufsize(m_ndbRecord, readMask.rep.data, m_firstRecAttr,
                                /*key_size = */ 0, needRangeNo(),
                                withCorrelation, batchFrags, batchRows,
                                m_maxBatchBytes, m_resultBufferSize);

    if (getQuery().m_hasAggregation && getParentOperation() == nullptr) {
      // For aggregate root scans, batch_byte_size controls DBLQH→DBSPJ
      // throughput, not API receive buffer. result_bufsize() caps it to
      // the tiny API buffer (no getValue columns). Restore to the value
      // from calculate_batch_size() so config BatchByteSize takes effect.
      m_maxBatchBytes = batchByteSize;
    }

    // Ensure m_maxBatchBytes is non-zero after calculation to prevent
    // re-entry into this block.  For lookup operations batchByteSize
    // is 0 (no scan batch sizing), but m_resultBufferSize was already
    // computed — re-entry would trigger the assert(m_resultBufferSize==0).
    if (m_maxBatchBytes == 0) {
      m_maxBatchBytes = m_resultBufferSize > 0 ? m_resultBufferSize : 1;
    }
  }

  return m_maxBatchBytes;
}

Uint32 NdbQueryOperationImpl::getResultBufferSize() const {
  (void)getMaxBatchBytes();  // Force calculation if required
  return m_resultBufferSize;
}

/** For debugging.*/
NdbOut &operator<<(NdbOut &out, const NdbQueryOperationImpl &op) {
  out << "[ this: " << &op << "  m_magic: " << op.m_magic;
  out << " op.operationDef.getOpNo()" << op.m_operationDef.getOpNo();
  if (op.getParentOperation()) {
    out << "  m_parent: " << op.getParentOperation();
  }
  for (unsigned int i = 0; i < op.getNoOfChildOperations(); i++) {
    out << "  m_children[" << i << "]: " << &op.getChildOperation(i);
  }
  out << "  m_queryImpl: " << &op.m_queryImpl;
  out << "  m_operationDef: " << &op.m_operationDef;
  out << " m_isRowNull " << op.m_isRowNull;
  out << " ]";
  return out;
}

NdbOut &operator<<(NdbOut &out, const NdbResultStream &stream) {
  out << " received rows: " << stream.m_resultSets[stream.m_recv].getRowCount();
  return out;
}

// Compiler settings require explicit instantiation.
template class Vector<NdbQueryOperationImpl *>;
