/*
   Copyright (c) 2011, 2025, Oracle and/or its affiliates.
   Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.

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

#ifndef NdbQueryBuilderImpl_H
#define NdbQueryBuilderImpl_H

#include <cstring>

/* Query-related error codes. */
#define QRY_REQ_ARG_IS_NULL 4800
#define QRY_TOO_FEW_KEY_VALUES 4801
#define QRY_TOO_MANY_KEY_VALUES 4802
#define QRY_OPERAND_HAS_WRONG_TYPE 4803
#define QRY_CHAR_OPERAND_TRUNCATED 4804
#define QRY_NUM_OPERAND_RANGE 4805
#define QRY_MULTIPLE_PARENTS 4806
#define QRY_UNKNOWN_PARENT 4807
#define QRY_UNRELATED_INDEX 4809
#define QRY_WRONG_INDEX_TYPE 4810
#define QRY_OPERAND_ALREADY_BOUND 4811
#define QRY_DEFINITION_TOO_LARGE 4812
#define QRY_SEQUENTIAL_SCAN_SORTED 4813
#define QRY_RESULT_ROW_ALREADY_DEFINED 4814
#define QRY_HAS_ZERO_OPERATIONS 4815
#define QRY_IN_ERROR_STATE 4816
#define QRY_ILLEGAL_STATE 4817
#define QRY_WRONG_OPERATION_TYPE 4820
#define QRY_SCAN_ORDER_ALREADY_SET 4821
#define QRY_PARAMETER_HAS_WRONG_TYPE 4822
#define QRY_CHAR_PARAMETER_TRUNCATED 4823
#define QRY_MULTIPLE_SCAN_SORTED 4824
#define QRY_BATCH_SIZE_TOO_SMALL 4825
#define QRY_EMPTY_PROJECTION 4826
#define QRY_OJ_NOT_SUPPORTED 4827
// #define QRY_NEST_NOT_SPECIFIED 4828  <<== DEPRECATED
#define QRY_NEST_NOT_SUPPORTED 4829
#define QRY_TABLE_HAVE_NO_FRAGMENTS 4830
#define QRY_BAD_FRAGMENT_DATA 4831

#include <Bitmask.hpp>
#include <Vector.hpp>
#include "NdbIndexScanOperation.hpp"
#include "NdbQueryBuilder.hpp"
#include "ndb_limits.h"

// Forward declared
class Ndb;
class NdbTableImpl;
class NdbIndexImpl;
class NdbColumnImpl;
class NdbQueryBuilderImpl;
class NdbQueryDefImpl;
class NdbQueryOperationDefImpl;
class NdbQueryParamValue;
class NdbParamOperandImpl;
class NdbConstOperandImpl;
class NdbLinkedOperandImpl;

// For debuggging.
// #define TRACE_SERIALIZATION

/** A buffer for holding serialized data.
 *
 *  Data is normally appended to the end of this buffer by several variants
 *  of ::append(). A chunk of memory may also be allocated (at end of buffer)
 *  with ::alloc(). The buffer has a small local storage likely to be sufficient
 *  for most buffer usage. If required it will allocate a buffer extension to
 *  satisfy larger buffer requests.
 *
 * NOTE:
 * When buffer grows, it contents may be relocated ta another memory area.
 * Pointers returned to ::alloc'ed objects or ::addr() request are therefore
 * not valid after another ::append() or ::alloc() has been performed. If
 * pointer persistency is required, use ::getSize() to store the current end
 * of buffer before the persistent object is allocated or appended. You may
 * then later use 'size' as a handle to ::addr() to get the address.
 *
 * NOTE:
 * If memory allocation fails during append / alloc, a 'memoryExhausted'
 * state is set. Further allocation or append will then fail or be ignored.
 * Before using the contents in the Uint32Buffer, always check
 * ::isMemoryExhausted() to validate the contents of your buffer.
 */
class Uint32Buffer {
 public:
  // #define TEST_Uint32Buffer

#if defined(TEST_Uint32Buffer)
  static constexpr Uint32 initSize =
      1;  // Small size to force test of buffer expand.
#else
  static constexpr Uint32 initSize =
      32;  // Initial buffer size, extend on demand but probably sufficient
#endif

  explicit Uint32Buffer()
      : m_array(m_local),
        m_avail(initSize),
        m_size(0),
        m_memoryExhausted(false),
        m_bytesLeft(0) {}

  ~Uint32Buffer() {
    if (unlikely(m_array != m_local)) {
      delete[] m_array;
    }
  }

  /**
   *  Explicit release of buffer to shrink memory footprint.
   */
  void releaseExtend() {
    if (unlikely(m_array != m_local)) {
      delete[] m_array;
    }
    m_array = nullptr;
    m_size = 0;
  }

  /**
   * Allocate a buffer extension at end of this buffer.
   * NOTE: Return NULL if allocation fails and set
   *       isMemoryExhausted. This will also cause further
   *       alloc() / append() to be skipped.
   */
  Uint32 *alloc(Uint32 count) {
    Uint32 reqSize = m_size + count;
    if (unlikely(reqSize >= m_avail)) {
      if (unlikely(m_memoryExhausted)) {
        return nullptr;
      }
#if defined(TEST_Uint32Buffer)
      Uint32 newSize = reqSize;  // -> Always expand on next alloc
#else
      Uint32 newSize = reqSize * 2;
#endif
      //    ndbout << "Uint32Buffer::alloc() Extend buffer from: " << m_avail
      //           << ", to: " << newSize << endl;
      Uint32 *newBuf = new Uint32[newSize];
      if (likely(newBuf != nullptr)) {
        assert(newBuf);
        memcpy(newBuf, m_array, m_size * sizeof(Uint32));
        if (m_array != m_local) {
          delete[] m_array;
        }
        m_array = newBuf;
        m_avail = static_cast<Uint32>(newSize);
      } else {
        m_size = m_avail;
        m_memoryExhausted = true;
        return nullptr;
      }
    }
    Uint32 *extend = &m_array[m_size];
    m_size += static_cast<Uint32>(count);
    return extend;
  }

  /** Put the idx'th element already allocated.
   */
  void put(Uint32 idx, Uint32 value) {
    assert(idx < m_size);
    m_array[idx] = value;
  }

  /** append 'src' word to end of this buffer
   */
  void append(const Uint32 src) {
    m_bytesLeft = 0;
    if (likely(m_size < m_avail)) {
      m_array[m_size++] = src;
    } else {
      Uint32 *dst = alloc(1);
      if (likely(dst != nullptr)) *dst = src;
    }
  }

  /** append 'src' buffer to end of this buffer
   */
  void append(const Uint32Buffer &src) {
    assert(!src.isMemoryExhausted());
    m_bytesLeft = 0;
    Uint32 len = src.getSize();
    if (likely(len > 0)) {
      Uint32 *dst = alloc(len);
      if (likely(dst != nullptr)) {
        memcpy(dst, src.addr(), len * sizeof(Uint32));
      }
    }
  }

  /** append 'src' *bytes* to end of this buffer
   *  Zero pad possibly odd bytes in last Uint32 word
   */
  void appendBytes(const void *src, Uint32 len) {
    if (likely(len > 0)) {
      Uint32 wordCount = static_cast<Uint32>(
          (len + sizeof(Uint32) - 1 - m_bytesLeft) / sizeof(Uint32));
      Uint32 *dst = alloc(wordCount);
      if (likely(dst != nullptr)) {
        // Copy src
        Uint8 *const start = reinterpret_cast<Uint8 *>(dst) - m_bytesLeft;
        memcpy(start, src, len);
        m_bytesLeft = (m_bytesLeft - len) % sizeof(Uint32);
        // Make sure that any trailing bytes in the last word are zero.
        std::memset(start + len, 0, m_bytesLeft);
      }
    }
  }

  /** Skip remaining bytes in m_array[m_size-1], so that a subsequent
   * appendBytes() starts at a word boundary.*/
  void skipRestOfWord() { m_bytesLeft = 0; }

  Uint32 *addr(Uint32 idx = 0) {
    return (likely(!m_memoryExhausted && m_size > idx)) ? &m_array[idx]
                                                        : nullptr;
  }
  const Uint32 *addr(Uint32 idx = 0) const {
    return (likely(!m_memoryExhausted && m_size > idx)) ? &m_array[idx]
                                                        : nullptr;
  }

  /** Get the idx'th element. Make sure there is space for 'count' elements.*/
  Uint32 get(Uint32 idx) const {
    assert(idx < m_size);
    return m_array[idx];
  }

  /** Check for possible memory alloc failure during build. */
  bool isMemoryExhausted() const { return m_memoryExhausted; }

  Uint32 getSize() const { return m_size; }

 private:
  /** Should not be copied, nor assigned.*/
  Uint32Buffer(Uint32Buffer &);
  Uint32Buffer &operator=(Uint32Buffer &);

 private:
  Uint32 m_local[initSize];  // Initial static bufferspace
  Uint32 *m_array;  // Refers m_local initially, or extended large buffer
  Uint32 m_avail;   // Available buffer space
  Uint32 m_size;    // Actual size <= m_avail
  bool m_memoryExhausted;
  /** Number of remaining bytes (0-3) in m_array[m_size-1].*/
  Uint32 m_bytesLeft;
};

class NdbQueryOptionsImpl {
  friend class NdbQueryOptions;
  friend class NdbQueryOperationDefImpl;

 public:
  explicit NdbQueryOptionsImpl()
      : m_matchType(NdbQueryOptions::MatchAll),
        m_scanOrder(NdbQueryOptions::ScanOrdering_void),
        m_parent(nullptr),
        m_firstUpper(nullptr),
        m_firstInner(nullptr),
        m_interpretedCode(nullptr),
        m_parameters(0),
        m_aggProgramBuffer(nullptr),
        m_aggProgramLen(0),
        m_aggNGroupByCols(0),
        m_aggDiskColumns(false),
        m_aggTable(nullptr),
        m_aggGbColumns(nullptr),
        m_linkedProjection(0),
        m_maxRows(0) {}
  NdbQueryOptionsImpl(const NdbQueryOptionsImpl &);
  ~NdbQueryOptionsImpl();

  NdbQueryOptions::ScanOrdering getOrdering() const { return m_scanOrder; }
  Uint32 getMaxRows() const { return m_maxRows; }

  bool hasAggregation() const { return m_aggProgramBuffer != nullptr; }
  const Uint32 *getAggProgramBuffer() const { return m_aggProgramBuffer; }
  Uint32 getAggProgramLen() const { return m_aggProgramLen; }
  Uint32 getAggNGroupByCols() const { return m_aggNGroupByCols; }
  bool getAggDiskColumns() const { return m_aggDiskColumns; }
  const NdbTableImpl *getAggTable() const { return m_aggTable; }
  const NdbDictionary::Column *const *getAggGbColumns() const {
    return m_aggGbColumns;
  }
  const Vector<const NdbLinkedOperandImpl *> &getLinkedProjection() const {
    return m_linkedProjection;
  }
 private:
  NdbQueryOptions::MatchType m_matchType;
  NdbQueryOptions::ScanOrdering m_scanOrder;
  NdbQueryOperationDefImpl *m_parent;
  NdbQueryOperationDefImpl *m_firstUpper;  // First in upper nest
  NdbQueryOperationDefImpl *m_firstInner;  // First in this (inner-)nest
  const NdbInterpretedCode *m_interpretedCode;
  Vector<const NdbQueryOperandImpl *> m_parameters;

  // Aggregation program (deep-copied from NdbAggregator)
  Uint32 *m_aggProgramBuffer;
  Uint32 m_aggProgramLen;
  Uint32 m_aggNGroupByCols;
  bool m_aggDiskColumns;
  const NdbTableImpl *m_aggTable;

  // Column definitions for GROUP BY columns (local and linked).
  // Dynamically allocated array of n_gb_cols pointers, deep-copied
  // from NdbAggregator::gb_columns() during copyAggregation().
  const NdbDictionary::Column **m_aggGbColumns;

  // Linked operands for parent column projection in aggregation
  Vector<const NdbLinkedOperandImpl *> m_linkedProjection;

  // Per-fragment row limit (0 = unlimited). When > 0, DBSPJ closes
  // the fragment scan after this many rows instead of requesting more.
  Uint32 m_maxRows;

  /**
   * Assign NdbInterpretedCode by taking a deep copy of 'src'
   * @return possible error code.
   */
  int copyInterpretedCode(const NdbInterpretedCode &src);

  /**
   * Deep-copy aggregation program from a finalized NdbAggregator.
   * @return possible error code.
   */
  int copyAggregation(const NdbAggregator &src);

  NdbQueryOptionsImpl &operator=(const NdbQueryOptionsImpl &);  // Not impl.
};

////////////////////////////////////////////////
// Implementation of NdbQueryOperation interface
////////////////////////////////////////////////

class NdbQueryOperationDefImpl {
  friend class NdbQueryOperationDef;
  friend class NdbQueryOperationImpl;
  friend class NdbQueryDefImpl;
  friend class NdbQueryImpl;
  friend class NdbQueryBuilder;
  friend class NdbQueryBuilderImpl;

 public:
  struct IndexBound {  // Limiting 'bound ' definition for indexScan
    NdbQueryOperandImpl *low[MAX_ATTRIBUTES_IN_INDEX];
    NdbQueryOperandImpl *high[MAX_ATTRIBUTES_IN_INDEX];
    Uint32 lowKeys, highKeys;
    bool lowIncl, highIncl;
  };

  /* Currently only a single parent is supported */
  Uint32 getNoOfParentOperations() const { return (m_parent) ? 1 : 0; }

  const NdbQueryOperationDefImpl &getParentOperation(Uint32 i
                                                     [[maybe_unused]]) const {
    assert(i == 0 && m_parent != nullptr);
    return *m_parent;
  }

  const NdbQueryOperationDefImpl *getParentOperation() const {
    return m_parent;
  }

  Uint32 getNoOfChildOperations() const { return m_children.size(); }

  const NdbQueryOperationDefImpl &getChildOperation(Uint32 i) const {
    return *m_children[i];
  }

  const NdbQueryOperationDefImpl *getFirstInner() const { return m_firstInner; }

  const NdbQueryOperationDefImpl *getFirstInEmbeddingNest() const {
    assert(m_firstInner == nullptr || m_firstUpper == nullptr);
    if (m_firstInner != nullptr)
      return m_firstInner;
    else if (m_firstUpper != nullptr)
      return m_firstUpper;
    else
      return nullptr;
  }

  const NdbTableImpl &getTable() const { return m_table; }

  const char *getName() const { return m_ident; }

  // Does an ancestor specify a MatchType requiring only a 'firstMatch'?
  // Both 'MatchFirst' and 'MatchNullOnly' are a firstMatch type as it
  // allows us to conclude as soon as a single qualifying row has been found.
  bool hasFirstMatchAncestor() const {
    if (m_parent == nullptr) return false;
    if (m_parent->getMatchType() &
        (NdbQueryOptions::MatchFirst | NdbQueryOptions::MatchNullOnly))
      return true;
    return m_parent->hasFirstMatchAncestor();
  }

  enum NdbQueryOptions::MatchType getMatchType() const {
    return m_options.m_matchType;
  }

  enum NdbQueryOptions::ScanOrdering getOrdering() const {
    return m_options.m_scanOrder;
  }

  const NdbInterpretedCode *getInterpretedCode() const {
    return m_options.m_interpretedCode;
  }

  bool isAggregateLeaf() const { return m_isAggregateLeaf; }

  bool isQueryAggregation() const { return m_queryHasAggregation; }

  bool isCteEmbedded() const { return m_isCteEmbedded; }

  const NdbQueryOptionsImpl &getOptions() const { return m_options; }

  const Vector<const NdbQueryOperandImpl *> &getInterpretedParams() const {
    return m_options.m_parameters;
  }

  // Establish a linked parent <-> child relationship with this operation
  int linkWithParent(NdbQueryOperationDefImpl *parentOp);

  /**
   * Register a linked reference to a column from operation
   * @param[in] column Column to refer.
   * @param[out] error Possible error code.
   * @return position in list of referred columns available from
   * this (parent) operation. Child ops later refer linked
   * columns by its position in this list.
   */
  Uint32 addColumnRef(const NdbColumnImpl *column, int &error);

  /**
   * Register a param operand which is referred by this operation.
   * Param values are supplied pr. operation when code is serialized.
   * @param[in] param Parameter to add.
   * @return Possible error code.
   */
  int addParamRef(const NdbParamOperandImpl *param);

  Uint32 getNoOfParameters() const { return m_params.size(); }

  const NdbParamOperandImpl &getParameter(Uint32 ix) const {
    return *m_params[ix];
  }

  virtual const NdbIndexImpl *getIndex() const { return nullptr; }

  virtual const NdbQueryOperandImpl *const *getKeyOperands() const {
    return nullptr;
  }

  virtual const IndexBound *getBounds() const { return nullptr; }

  /**
   * True if this is a prunable scan and there are NdbQueryParamOperands in the
   * distribution key.
   */
  virtual bool hasParamInPruneKey() const { return false; }

  // Return 'true' if query type is a multi-row scan
  virtual bool isScanOperation() const = 0;

  virtual const NdbQueryOperationDef &getInterface() const = 0;

  /** Make a serialized representation of this operation, corresponding to
   * the struct QueryNode type.
   * @return Possible error code.
   */
  virtual int serializeOperation(const Ndb *ndb,
                                 Uint32Buffer &serializedTree) = 0;

  /** Find the projection that should be sent to the SPJ block. This should
   * contain the attributes needed to instantiate all child operations.
   */
  const Vector<const NdbColumnImpl *> &getSPJProjection() const {
    return m_spjProjection;
  }

  virtual int checkPrunable(const Uint32Buffer & /*keyInfo*/,
                            Uint32 /*shortestBound*/, bool &isPruned,
                            Uint32 & /*hashValue*/) const {
    isPruned = false;
    return 0;
  }

  virtual ~NdbQueryOperationDefImpl();

 protected:
  explicit NdbQueryOperationDefImpl(const NdbTableImpl &table,
                                    const NdbQueryOptionsImpl &options,
                                    const char *ident, Uint32 opNo,
                                    Uint32 internalOpNo, int &error);

 public:
  // Get the ordinal position of this operation within the query def.
  Uint32 getOpNo() const { return m_opNo; }

  // Get id of node as known inside queryTree
  Uint32 getInternalOpNo() const { return m_internalOpNo; }

  // Get type of query operation
  virtual NdbQueryOperationDef::Type getType() const = 0;

  /**
   * Used for telling if parent at depth n has more siblings. (In that case
   * we need to draw a horisontal line leading to that sibling.)
   */
  typedef Bitmask<(NDB_SPJ_MAX_TREE_NODES + 31) / 32> SiblingMask;

  /** Print query tree graph to trace file (using recursion).
   * @param depth Number of ancestor nodes that this node has.
   * @param hasMoreSiblingsMask The n'th bit should be set if the n'th ancestor
   * (counted from the root) has more sibling nodes.
   */
  void printTree(Uint32 depth, SiblingMask hasMoreSiblingsMask) const;

 protected:
  // QueryTree building:
  // Append list of parent nodes to serialized code
  Uint32 appendParentList(Uint32Buffer &serializedDef) const;

  // Append list of columns required by SPJ to instantiate child operations.
  Uint32 appendChildProjection(Uint32Buffer &serializedDef) const;

  Uint32 appendParamConstructor(Uint32Buffer &serializedDef) const;

 protected:
  /** True if enclosing query has been prepared.*/
  bool m_isPrepared;

  /**
   * True if the projection for instantiating child operations contains any
   * disk columns.
   */
  bool m_diskInChildProjection;

  /** True if this operation is the aggregate leaf of a join aggregation query.*/
  bool m_isAggregateLeaf;

  /** True if the enclosing query has aggregation (set before serialization).*/
  bool m_queryHasAggregation;

  /** True if this operation is embedded inside a CTE subtree. */
  bool m_isCteEmbedded;

 private:
  bool isChildOf(const NdbQueryOperationDefImpl *parentOp) const;

  /**
   * Register a linked child referring specified operation
   * @param[in] child Child operation to add.
   * @return Possible error code.
   */
  int addChild(NdbQueryOperationDefImpl *child);

  // Remove a linked child referring specified operation
  void removeChild(const NdbQueryOperationDefImpl *);

 private:
  const NdbTableImpl &m_table;
  const char *const m_ident;  // Optional name specified by application
  const Uint32 m_opNo;        // Index of this operation within operation array
  const Uint32
      m_internalOpNo;  // Operation id when materialized into queryTree.
                       // If op has index, index opNo is 'm_internalOpNo-1'.

  // Optional (or default) options specified when building query:
  // - Scan order which may specify ascending or descending scan order
  // - Match type used for hinting on optimal inner-, outer-, semijoin exec.
  const NdbQueryOptionsImpl m_options;

  // parent pointer & child ptr. vector contains dependencies
  // as defined with linkedValues
  NdbQueryOperationDefImpl *m_parent;
  Vector<NdbQueryOperationDefImpl *> m_children;

  // The (optional) first table in the upper- or the inner nest of
  // this table. Only set if the entire inner-nest is not contained
  // within the tree branch starting with the first inner, or
  // the first upper op-node.
  const NdbQueryOperationDefImpl *const m_firstUpper;
  const NdbQueryOperationDefImpl *const m_firstInner;

  // Params required by this operation
  Vector<const NdbParamOperandImpl *> m_params;

  // Column from this operation required by its child operations
  Vector<const NdbColumnImpl *> m_spjProjection;
};  // class NdbQueryOperationDefImpl

class NdbQueryScanOperationDefImpl : public NdbQueryOperationDefImpl {
 public:
  explicit NdbQueryScanOperationDefImpl(const NdbTableImpl &table,
                                        const NdbQueryOptionsImpl &options,
                                        const char *ident, Uint32 opNo,
                                        Uint32 internalOpNo, int &error);

  bool isScanOperation() const override { return true; }

 protected:
  int serialize(const Ndb *ndb, Uint32Buffer &serializedDef,
                const NdbTableImpl &tableOrIndex);

  // Append pattern for creating complete range bounds to serialized code
  virtual Uint32 appendBoundPattern(Uint32Buffer & /*serializedDef*/) const {
    return 0;
  }

  virtual Uint32 appendPrunePattern(Uint32Buffer & /*serializedDef*/) {
    return 0;
  }

};  // class NdbQueryScanOperationDefImpl

class NdbQueryIndexScanOperationDefImpl : public NdbQueryScanOperationDefImpl {
  friend class NdbQueryBuilder;  // Allow privat access from builder interface

 public:
  const NdbIndexImpl *getIndex() const override { return &m_index; }

  int serializeOperation(const Ndb *ndb, Uint32Buffer &serializedDef) override;

  const NdbQueryIndexScanOperationDef &getInterface() const override {
    return m_interface;
  }

  NdbQueryOperationDef::Type getType() const override {
    return NdbQueryOperationDef::OrderedIndexScan;
  }

  int checkPrunable(const Uint32Buffer &keyInfo, Uint32 shortestBound,
                    bool &isPruned, Uint32 &hashValue) const override;

  const IndexBound *getBounds() const override { return &m_bound; }

  bool hasParamInPruneKey() const override { return m_paramInPruneKey; }

 protected:
  // Append pattern for creating complete range bounds to serialized code
  Uint32 appendBoundPattern(Uint32Buffer &serializedDef) const override;

  Uint32 appendPrunePattern(Uint32Buffer &serializedDef) override;

 private:
  explicit NdbQueryIndexScanOperationDefImpl(const NdbIndexImpl &index,
                                             const NdbTableImpl &table,
                                             const NdbQueryIndexBound *bound,
                                             const NdbQueryOptionsImpl &options,
                                             const char *ident, Uint32 opNo,
                                             Uint32 internalOpNo, int &error);

  // Append pattern for creating a single bound value to serialized code
  Uint32 appendBoundValue(Uint32Buffer &serializedDef,
                          NdbIndexScanOperation::BoundType type,
                          const NdbQueryOperandImpl *value,
                          int &paramCnt) const;

 private:
  NdbQueryIndexScanOperationDef m_interface;
  const NdbIndexImpl &m_index;

  /** True if there is a set of bounds.*/
  IndexBound m_bound;

  /**
   * True if scan is prunable and there are NdbQueryParamOperands in the
   * distribution key.
   */
  bool m_paramInPruneKey;
};  // class NdbQueryIndexScanOperationDefImpl

class NdbQueryDefImpl {
  friend class NdbQueryDef;

 public:
  /** CTE definition info for the KeyInfo agg section. */
  struct CteDefInfo {
    Uint32 cteId;
    Uint32 tableId;
    Uint32 schemaVersion;
    Uint64 depMask;
    Uint32 flags;
    Vector<Uint32> aggProgram;
  };

  explicit NdbQueryDefImpl(const Ndb *ndb,
                           const Vector<NdbQueryOperationDefImpl *> &operations,
                           const Vector<NdbQueryOperandImpl *> &operands,
                           const Vector<CteDefInfo> &cteDefs,
                           int &error);
  ~NdbQueryDefImpl();

  // A query is treated as scan-type when:
  //   (a) the main query root is a scan, OR
  //   (b) the query contains some CTE
  // Case (b) forces SCAN_TABREQ so CTE SETUP / aggStateKeys /
  // RT_CTE_PHASE machinery runs.  DBSPJ short-circuits the main
  // query on all but rootFragId == 0 for CTE_LOOKUP_REQ and
  // rootFragId < numDataNodes for CTE_SCAN_REQ to avoid duplicating
  // the result across fragments.
  //
  // Real-table PK/UniqueIndex main roots without CTEs stay as TCKEYREQ
  bool isScanQuery() const {
    /* Queries with CTEs always use the scan protocol (SCAN_TABREQ)
     * because CTE materialization requires multi-phase coordination
     * by DBTC, even when the main query root is a lookup (lookupCte). */
    if (m_cteDefs.size() > 0) return true;
    return m_operations[0]->isScanOperation();
  }

  NdbQueryDef::QueryType getQueryType() const;

  Uint32 getNoOfOperations() const { return m_operations.size(); }

  // Get a specific NdbQueryOperationDef by ident specified
  // when the NdbQueryOperationDef was created.
  const NdbQueryOperationDefImpl &getQueryOperation(Uint32 index) const {
    return *m_operations[index];
  }

  const NdbQueryOperationDefImpl *getQueryOperation(const char *ident) const;

  const NdbQueryDef &getInterface() const { return m_interface; }

  /** Get serialized representation of query definition.*/
  Uint32Buffer &getSerialized() { return m_serializedDef; }

  /** Get serialized representation of query definition.*/
  const Uint32Buffer &getSerialized() const { return m_serializedDef; }

  bool hasAggregation() const { return m_hasAggregation; }
  Uint32 getAggregateLeafOpNo() const { return m_aggregateLeafOpNos[0]; }
  Uint32 getNumAggregateLeaves() const { return m_aggregateLeafOpNos.size(); }
  Uint32 getAggregateLeafOpNo(Uint32 i) const { return m_aggregateLeafOpNos[i]; }

 private:
  NdbQueryDef m_interface;

  Vector<NdbQueryOperationDefImpl *> m_operations;
  Vector<NdbQueryOperandImpl *> m_operands;
  Uint32Buffer m_serializedDef;

  Vector<CteDefInfo> m_cteDefs;

 public:
  Uint32 getNumCtes() const { return m_cteDefs.size(); }
  const CteDefInfo &getCteDef(Uint32 i) const { return m_cteDefs[i]; }

 private:
  bool m_hasAggregation;
  Vector<Uint32> m_aggregateLeafOpNos;
};  // class NdbQueryDefImpl

class NdbQueryBuilderImpl {
  friend class NdbQueryBuilder;

 public:
  ~NdbQueryBuilderImpl();
  explicit NdbQueryBuilderImpl();

  const NdbQueryDefImpl *prepare(const Ndb *ndb);

  const NdbError &getNdbError() const;

  void setErrorCode(int aErrorCode);

 private:
  bool hasError() const { return m_hasError; }

  /**
   * Add an operand to m_operands. Set an error code if operand
   * is null or if adding it to m_operands fails.
   * @param[in] operand to add (may be NULL).
   * @return Operand interface (or NULL if there was an error.)
   */
  NdbQueryOperand *addOperand(NdbQueryOperandImpl *operand);

  /**
   * Take ownership of specified object: From now on it is the
   * responsibility of this NdbQueryBuilderImpl to manage the
   * lifetime of the object. If takeOwnership() fails, the
   * specified object is deleted before it returns.
   *
   * @param[in] operand operand to take ownership for (may be NULL).
   * @return 0 if ok, else there has been an 'Err_MemoryAlloc'
   */
  int takeOwnership(NdbQueryOperandImpl *operand);
  int takeOwnership(NdbQueryOperationDefImpl *);

  bool contains(const NdbQueryOperationDefImpl *);

  // Get internal operation number of the next operation.
  Uint32 getNextInternalOpNo() const {
    return m_operations.size() == 0
               ? 0
               : m_operations[m_operations.size() - 1]->getInternalOpNo() + 1;
  }

  NdbQueryBuilder m_interface;
  // Allow update error from const methods
  mutable NdbError m_error;

  Vector<NdbQueryOperationDefImpl *> m_operations;
  Vector<NdbQueryOperandImpl *> m_operands;
  Uint32 m_paramCnt;
  /** True if there was an error that prevents further use of this object.*/
  bool m_hasError;

  /** CTE definitions (transferred to NdbQueryDefImpl in prepare()). */
  Vector<NdbQueryDefImpl::CteDefInfo> m_cteDefs;

  /** CTE subtree tracking for beginCteSubtree()/endCteSubtree(). */
  bool m_inCteSubtree;
  Uint32 m_cteSubtreeStartOpIdx;  // Index in m_operations where subtree starts
  Uint32 m_currentCteId;
  Uint32 m_completedCteSubtrees;  // Count of completed CTE subtrees
};  // class NdbQueryBuilderImpl

//////////////////////////////////////////////
// Implementation of NdbQueryOperand interface
//////////////////////////////////////////////

// Baseclass for the QueryOperand implementation
class NdbQueryOperandImpl {
  friend class NdbQueryBuilderImpl;

 public:
  /** The type of an operand. This corresponds to the set of subclasses
   * of NdbQueryOperandImpl.
   */
  enum Kind { Linked, Param, Const };

  const NdbColumnImpl *getColumn() const { return m_column; }

  virtual int bindOperand(const NdbColumnImpl &column,
                          NdbQueryOperationDefImpl & /*operation*/) {
    if (m_column && m_column != &column)
      // Already bounded to a different column
      return QRY_OPERAND_ALREADY_BOUND;
    m_column = &column;
    return 0;
  }

  Kind getKind() const { return m_kind; }

  virtual NdbQueryOperand &getInterface() = 0;

 protected:
  friend NdbQueryBuilderImpl::~NdbQueryBuilderImpl();
  friend NdbQueryDefImpl::~NdbQueryDefImpl();

  virtual ~NdbQueryOperandImpl() {}

  NdbQueryOperandImpl(Kind kind) : m_column(nullptr), m_kind(kind) {}

 protected:
  const NdbColumnImpl *m_column;  // Initial NULL, assigned w/ bindOperand()

  /** This is used to tell the type of an NdbQueryOperand. This allow safe
   * downcasting to a subclass.
   */
  const Kind m_kind;
};  // class NdbQueryOperandImpl

class NdbLinkedOperandImpl : public NdbQueryOperandImpl {
  friend class NdbQueryBuilder;  // Allow privat access from builder interface

 public:
  const NdbQueryOperationDefImpl &getParentOperation() const {
    return m_parentOperation;
  }

  // 'LinkedSrc' is index into parent op's spj-projection list where
  // the referred column value is available
  Uint32 getLinkedColumnIx() const { return m_parentColumnIx; }

  const NdbColumnImpl &getParentColumn() const {
    return *m_parentOperation.getSPJProjection()[m_parentColumnIx];
  }

  NdbQueryOperand &getInterface() override { return m_interface; }

  int bindOperand(const NdbColumnImpl &column,
                  NdbQueryOperationDefImpl &operation) override;

 private:
  NdbLinkedOperandImpl(NdbQueryOperationDefImpl &parent, Uint32 columnIx)
      : NdbQueryOperandImpl(Linked),
        m_interface(*this),
        m_parentOperation(parent),
        m_parentColumnIx(columnIx) {}

  NdbLinkedOperand m_interface;
  NdbQueryOperationDefImpl &m_parentOperation;
  const Uint32 m_parentColumnIx;
};  // class NdbLinkedOperandImpl

class NdbParamOperandImpl : public NdbQueryOperandImpl {
  friend class NdbQueryBuilder;  // Allow privat access from builder interface

 public:
  const char *getName() const { return m_name; }

  Uint32 getParamIx() const { return m_paramIx; }

  NdbQueryOperand &getInterface() override { return m_interface; }

  int bindOperand(const NdbColumnImpl &column,
                  NdbQueryOperationDefImpl &operation) override;

 private:
  NdbParamOperandImpl(const char *name, Uint32 paramIx)
      : NdbQueryOperandImpl(Param),
        m_interface(*this),
        m_name(name),
        m_paramIx(paramIx) {}

  NdbParamOperand m_interface;
  const char *const m_name;  // Optional parameter name or NULL
  const Uint32 m_paramIx;
};  // class NdbParamOperandImpl

class NdbConstOperandImpl : public NdbQueryOperandImpl {
  friend class NdbQueryBuilder;  // Allow privat access from builder interface
 public:
  Uint32 getSizeInBytes() const { return m_converted.len; }
  const void *getAddr() const {
    return likely(m_converted.buffer == nullptr) ? &m_converted.val
                                                 : m_converted.buffer;
  }

  NdbQueryOperand &getInterface() override { return m_interface; }

  int bindOperand(const NdbColumnImpl &column,
                  NdbQueryOperationDefImpl &operation) override;

 protected:
  NdbConstOperandImpl()
      : NdbQueryOperandImpl(Const), m_converted(), m_interface(*this) {}

#define UNDEFINED_CONVERSION \
  { return QRY_OPERAND_HAS_WRONG_TYPE; }

  virtual int convertUint8() UNDEFINED_CONVERSION
      virtual int convertInt8() UNDEFINED_CONVERSION
      virtual int convertUint16() UNDEFINED_CONVERSION
      virtual int convertInt16() UNDEFINED_CONVERSION
      virtual int convertUint24() UNDEFINED_CONVERSION
      virtual int convertInt24() UNDEFINED_CONVERSION
      virtual int convertUint32() UNDEFINED_CONVERSION
      virtual int convertInt32() UNDEFINED_CONVERSION
      virtual int convertUint64() UNDEFINED_CONVERSION
      virtual int convertInt64() UNDEFINED_CONVERSION
      virtual int convertFloat() UNDEFINED_CONVERSION
      virtual int convertDouble() UNDEFINED_CONVERSION

      virtual int convertUDec() UNDEFINED_CONVERSION
      virtual int convertDec() UNDEFINED_CONVERSION

      virtual int convertBit() UNDEFINED_CONVERSION
      virtual int convertChar() UNDEFINED_CONVERSION
      virtual int convertVChar() UNDEFINED_CONVERSION
      virtual int convertLVChar() UNDEFINED_CONVERSION
      virtual int convertBin() UNDEFINED_CONVERSION
      virtual int convertVBin() UNDEFINED_CONVERSION
      virtual int convertLVBin() UNDEFINED_CONVERSION

      virtual int convertDate() UNDEFINED_CONVERSION
      virtual int convertDatetime() UNDEFINED_CONVERSION
      virtual int convertTime() UNDEFINED_CONVERSION
      virtual int convertYear() UNDEFINED_CONVERSION
      virtual int convertTimestamp() UNDEFINED_CONVERSION

      virtual int convert2ColumnType();

  /** Values converted to datatype format as expected by bound column
   * (available through ::getColumn())
   */
  class ConvertedValue {
   public:
    ConvertedValue() : len(0), buffer(nullptr) {}
    ~ConvertedValue() {
      if (buffer) delete[] ((char *)buffer);
    }

    char *getCharBuffer(Uint32 size) {
      char *dst = val.shortChar;
      if (unlikely(size > sizeof(val.shortChar))) {
        dst = new char[size];
        buffer = dst;
      }
      len = size;
      return dst;
    }

    static constexpr Uint32 maxShortChar = 32;

    union {
      Uint8 uint8;
      Int8 int8;
      Uint16 uint16;
      Int16 int16;
      Uint32 uint32;
      Int32 int32;
      Uint64 uint64;
      Int64 int64;

      double dbl;
      float flt;

      char shortChar[maxShortChar];
    } val;

    Uint32 len;
    void *buffer;  // Optional; storage for converted value
  } m_converted;

 private:
  NdbConstOperand m_interface;
};  // class NdbConstOperandImpl

#endif
