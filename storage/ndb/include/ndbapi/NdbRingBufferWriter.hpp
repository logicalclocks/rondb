/*
   Copyright (c) 2025, 2026, Hopsworks and/or its affiliates.

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

#ifndef NdbRingBufferWriter_H
#define NdbRingBufferWriter_H

#include <ndb_types.h>
#include <NdbDictionary.hpp>

class Ndb;
class NdbTransaction;
class NdbRecord;
class NdbOperation;

/**
 * @brief Helper for inserting rows into ring buffer tables via NDB API.
 *
 * NdbRingBufferWriter encapsulates the ring buffer INSERT protocol that
 * is normally performed by the MySQL handler (ha_ndbcluster.cc).  It
 * manages the internal ring_idx and ring_meta columns automatically,
 * so the caller only needs to provide user column values.
 *
 * Usage:
 * @code
 *   const NdbDictionary::Table *table = dict->getTable("my_ring_table");
 *   const NdbRecord *record = table->getDefaultRecord();
 *   NdbTransaction *trans = ndb->startTransaction();
 *
 *   NdbRingBufferWriter writer(table, record, trans);
 *
 *   // Fill row buffer with user column values (NdbRecord layout)
 *   // Do NOT set ring_idx or ring_meta — the writer manages those.
 *   writer.addRow(rowBuffer, userMask);
 *   writer.addRow(rowBuffer2, userMask);  // batches if same PK prefix
 *
 *   writer.flush();         // writes meta row + execute(NoCommit)
 *   trans->execute(Commit); // caller commits
 *   ndb->closeTransaction(trans);
 * @endcode
 *
 * The userMask is a byte array indexed by attribute ID (same format as
 * NdbTransaction::insertTuple).  The bits for ring_idx and ring_meta
 * columns must NOT be set — the writer adds them internally.
 *
 * For tables with BLOB/TEXT columns, addRow() returns the NdbOperation*
 * so the caller can obtain blob handles via op->getBlobHandle(attrId).
 */
class NdbRingBufferWriter {
 public:
  /**
   * Construct a writer for the given ring buffer table.
   *
   * @param table     NdbDictionary::Table (must be a ring buffer table)
   * @param ndbRecord NdbRecord for the table (use table->getDefaultRecord())
   * @param trans     Active NdbTransaction (must already be started)
   *
   * Check getErrorCode() after construction — non-zero means the table
   * is not a ring buffer table or metadata could not be cached.
   */
  NdbRingBufferWriter(const NdbDictionary::Table *table,
                      const NdbRecord *ndbRecord, NdbTransaction *trans);

  /** Destructor.  Does NOT auto-flush — caller must call flush() explicitly. */
  ~NdbRingBufferWriter();

  /**
   * Queue a row for insertion into the ring buffer.
   *
   * @param rowBuffer  Row in NdbRecord layout with user columns filled in.
   *                   The writer copies the data internally.
   * @param userMask   Column mask (byte array, indexed by attrId) for the
   *                   columns the user is providing.  Must NOT include
   *                   ring_idx or ring_meta bits.
   * @return Pointer to the queued NdbOperation on success (for blob handle
   *         access), or nullptr on error.
   *
   * For rows with the same PK prefix (all PK columns except ring_idx),
   * the writer batches them: meta is read once and data writes are queued
   * without intermediate execute calls.  When the PK prefix changes,
   * the writer flushes the previous batch internally.
   */
  const NdbOperation *addRow(const char *rowBuffer,
                             const unsigned char *userMask);

  /**
   * Flush any pending batch (writes meta row, executes NoCommit).
   * Must be called after the last addRow() and before commit.
   *
   * @return 0 on success, -1 on error.
   */
  int flush();

  /** Get the NDB error code of the last failed operation. */
  int getErrorCode() const { return m_error_code; }

  /** Get the NDB error message of the last failed operation. */
  const char *getErrorMessage() const { return m_error_message; }

 private:
  // Non-copyable
  NdbRingBufferWriter(const NdbRingBufferWriter &) = delete;
  NdbRingBufferWriter &operator=(const NdbRingBufferWriter &) = delete;

  /*
   * Ring buffer meta row format (32 bytes):
   *   Offset  Size  Field
   *   0       2     version (1)
   *   2       2     reserved_0
   *   4       4     next_pos (1-based, wraps at ring_size)
   *   8       4     count (0 to ring_size)
   *   12      4     reserved_1
   *   16      8     total_inserts (monotonic)
   *   24      8     reserved_2
   */
  static const Uint32 RING_META_VERSION = 1;
  static const Uint32 RING_META_SIZE = 32;

  struct Ring_meta {
    Uint16 version;
    Uint16 reserved_0;
    Uint32 next_pos;
    Uint32 count;
    Uint32 reserved_1;
    Uint64 total_inserts;
    Uint64 reserved_2;

    void pack(unsigned char *buf) const;
    void unpack(const unsigned char *buf);
    void init_first_insert();
    void advance(Uint32 ring_size);
  };

  // Per-column info cached at construction
  struct ColumnInfo {
    Uint32 attr_id;
    Uint32 offset;
    Uint32 max_size;
    Uint32 nullbit_byte_offset;
    Uint32 nullbit_bit_in_byte;
    Uint32 flags;  // NdbRecord::ColFlags
  };

  // Internal methods
  int initColumnMetadata();
  int readMetaRow(const char *rowBuffer);
  const NdbOperation *writeDataRow(const char *rowBuffer,
                                   const unsigned char *userMask);
  int writeMetaRow();
  bool pkPrefixMatches(const char *row1, const char *row2) const;
  void setRingIdxInBuffer(char *buf, Uint32 value) const;
  void setRingMetaNullInBuffer(char *buf) const;
  void setRingMetaValueInBuffer(char *buf,
                                const unsigned char *packed) const;
  void clearRingMetaNullInBuffer(char *buf) const;
  void zeroNotNullColumnsInBuffer(char *buf) const;
  void buildDataMask(const unsigned char *userMask,
                     unsigned char *outMask) const;
  void setError(int code, const char *msg);

  // Table metadata (cached at construction)
  const NdbDictionary::Table *m_table;
  const NdbRecord *m_ndb_record;
  NdbTransaction *m_trans;

  Uint32 m_ring_buffer_size;
  Uint32 m_ring_idx_col_no;
  Uint32 m_ring_meta_col_no;

  // Ring column info from NdbRecord
  ColumnInfo m_ring_idx_info;
  ColumnInfo m_ring_meta_info;

  // PK prefix columns (all PK columns except ring_idx)
  ColumnInfo *m_pk_prefix_cols;
  Uint32 m_num_pk_prefix_cols;

  // NOT NULL non-blob non-PK columns (for meta row zero-fill)
  ColumnInfo *m_notnull_cols;
  Uint32 m_num_notnull_cols;

  // Pre-built meta column mask (byte array indexed by attrId)
  unsigned char *m_meta_mask;
  Uint32 m_mask_byte_size;

  // Row buffer sizes
  Uint32 m_row_size;

  // Internal buffers
  char *m_meta_row_buffer;      // Scratch for meta row operations
  char *m_data_row_buffer;      // Scratch for data row operations
  char *m_key_row_buffer;       // Scratch for key (meta read)
  char *m_pk_prefix_buffer;     // Cached PK prefix for batch comparison
  unsigned char *m_data_mask;   // Reusable scratch for writeDataRow mask

  // Batch state
  bool m_batch_active;
  bool m_batch_meta_existed;
  Ring_meta m_batch_meta;

  // Error state
  int m_error_code;
  char m_error_message[256];
};

#endif  // NdbRingBufferWriter_H
