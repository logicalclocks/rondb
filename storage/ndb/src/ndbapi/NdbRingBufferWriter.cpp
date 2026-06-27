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

#include <NdbRingBufferWriter.hpp>

#include <cstring>

#include <NdbOperation.hpp>
#include <NdbTransaction.hpp>

#include "API.hpp"

// ---------------------------------------------------------------
// Ring_meta serialization
// ---------------------------------------------------------------

void NdbRingBufferWriter::Ring_meta::pack(unsigned char *buf) const {
  int2store(buf + 0, version);
  int2store(buf + 2, reserved_0);
  int4store(buf + 4, next_pos);
  int4store(buf + 8, count);
  int4store(buf + 12, reserved_1);
  int8store(buf + 16, total_inserts);
  int8store(buf + 24, reserved_2);
}

void NdbRingBufferWriter::Ring_meta::unpack(const unsigned char *buf) {
  version = uint2korr(buf + 0);
  reserved_0 = uint2korr(buf + 2);
  next_pos = uint4korr(buf + 4);
  count = uint4korr(buf + 8);
  reserved_1 = uint4korr(buf + 12);
  total_inserts = uint8korr(buf + 16);
  reserved_2 = uint8korr(buf + 24);
}

void NdbRingBufferWriter::Ring_meta::init_first_insert() {
  version = RING_META_VERSION;
  reserved_0 = 0;
  next_pos = 1;  // First data slot; advance() called by writeDataRow
  count = 0;
  reserved_1 = 0;
  total_inserts = 0;
  reserved_2 = 0;
}

void NdbRingBufferWriter::Ring_meta::advance(Uint32 ring_size) {
  next_pos = (next_pos % ring_size) + 1;
  if (count < ring_size) count++;
  total_inserts++;
}

// ---------------------------------------------------------------
// Helper to look up a column in NdbRecord by attrId
// ---------------------------------------------------------------

static const NdbRecord::Attr *findAttrByAttrId(const NdbRecord *rec,
                                                Uint32 attrId) {
  if (attrId < rec->m_attrId_indexes_length) {
    int idx = rec->m_attrId_indexes[attrId];
    if (idx >= 0) {
      return &rec->columns[idx];
    }
  }
  return nullptr;
}

// ---------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------

NdbRingBufferWriter::NdbRingBufferWriter(const NdbDictionary::Table *table,
                                         const NdbRecord *ndbRecord,
                                         NdbTransaction *trans)
    : m_table(table),
      m_ndb_record(ndbRecord),
      m_trans(trans),
      m_ring_buffer_size(0),
      m_ring_idx_col_no(0),
      m_ring_meta_col_no(0),
      m_pk_prefix_cols(nullptr),
      m_num_pk_prefix_cols(0),
      m_notnull_cols(nullptr),
      m_num_notnull_cols(0),
      m_meta_mask(nullptr),
      m_mask_byte_size(0),
      m_row_size(0),
      m_meta_row_buffer(nullptr),
      m_data_row_buffer(nullptr),
      m_key_row_buffer(nullptr),
      m_pk_prefix_buffer(nullptr),
      m_data_mask(nullptr),
      m_batch_active(false),
      m_batch_meta_existed(false),
      m_error_code(0) {
  memset(&m_ring_idx_info, 0, sizeof(m_ring_idx_info));
  memset(&m_ring_meta_info, 0, sizeof(m_ring_meta_info));
  memset(&m_batch_meta, 0, sizeof(m_batch_meta));
  m_error_message[0] = '\0';

  if (!table || !ndbRecord || !trans) {
    setError(4000, "NdbRingBufferWriter: null argument");
    return;
  }

  if (!table->isRingBuffer()) {
    setError(4000, "NdbRingBufferWriter: table is not a ring buffer table");
    return;
  }

  m_ring_buffer_size = table->getRingBufferSize();
  m_ring_idx_col_no = table->getRingIdxColumnNo();
  m_ring_meta_col_no = table->getRingMetaColumnNo();
  m_row_size = ndbRecord->m_row_size;

  if (initColumnMetadata() != 0) {
    return;  // error already set
  }

  // Allocate internal buffers
  m_meta_row_buffer = new char[m_row_size];
  m_data_row_buffer = new char[m_row_size];
  m_key_row_buffer = new char[m_row_size];
  m_pk_prefix_buffer = new char[m_row_size];
  m_data_mask = new unsigned char[m_mask_byte_size];
  memset(m_meta_row_buffer, 0, m_row_size);
  memset(m_data_row_buffer, 0, m_row_size);
  memset(m_key_row_buffer, 0, m_row_size);
  memset(m_pk_prefix_buffer, 0, m_row_size);
}

NdbRingBufferWriter::~NdbRingBufferWriter() {
  delete[] m_pk_prefix_cols;
  delete[] m_notnull_cols;
  delete[] m_meta_mask;
  delete[] m_meta_row_buffer;
  delete[] m_data_row_buffer;
  delete[] m_key_row_buffer;
  delete[] m_pk_prefix_buffer;
  delete[] m_data_mask;
}

// ---------------------------------------------------------------
// Column metadata initialization
// ---------------------------------------------------------------

int NdbRingBufferWriter::initColumnMetadata() {
  const int num_cols = m_table->getNoOfColumns();

  // Find ring_idx and ring_meta in NdbRecord
  const NdbDictionary::Column *ring_idx_col =
      m_table->getColumn(m_ring_idx_col_no);
  const NdbDictionary::Column *ring_meta_col =
      m_table->getColumn(m_ring_meta_col_no);

  if (!ring_idx_col || !ring_meta_col) {
    setError(4000, "NdbRingBufferWriter: ring columns not found in table");
    return -1;
  }

  const Uint32 ring_idx_attr_id = ring_idx_col->getAttrId();
  const Uint32 ring_meta_attr_id = ring_meta_col->getAttrId();

  // Look up ring_idx in NdbRecord
  const NdbRecord::Attr *idx_attr =
      findAttrByAttrId(m_ndb_record, ring_idx_attr_id);
  if (!idx_attr) {
    setError(4000,
             "NdbRingBufferWriter: ring_idx column not found in NdbRecord");
    return -1;
  }
  m_ring_idx_info.attr_id = idx_attr->attrId;
  m_ring_idx_info.offset = idx_attr->offset;
  m_ring_idx_info.max_size = idx_attr->maxSize;
  m_ring_idx_info.nullbit_byte_offset = idx_attr->nullbit_byte_offset;
  m_ring_idx_info.nullbit_bit_in_byte = idx_attr->nullbit_bit_in_byte;
  m_ring_idx_info.flags = idx_attr->flags;

  // Look up ring_meta in NdbRecord
  const NdbRecord::Attr *meta_attr =
      findAttrByAttrId(m_ndb_record, ring_meta_attr_id);
  if (!meta_attr) {
    setError(4000,
             "NdbRingBufferWriter: ring_meta column not found in NdbRecord");
    return -1;
  }
  m_ring_meta_info.attr_id = meta_attr->attrId;
  m_ring_meta_info.offset = meta_attr->offset;
  m_ring_meta_info.max_size = meta_attr->maxSize;
  m_ring_meta_info.nullbit_byte_offset = meta_attr->nullbit_byte_offset;
  m_ring_meta_info.nullbit_bit_in_byte = meta_attr->nullbit_bit_in_byte;
  m_ring_meta_info.flags = meta_attr->flags;

  // Collect PK prefix columns (all PK columns except ring_idx)
  // and NOT NULL non-blob non-ring columns
  // First pass: count them
  Uint32 pk_prefix_count = 0;
  Uint32 notnull_count = 0;

  for (int i = 0; i < num_cols; i++) {
    const NdbDictionary::Column *col = m_table->getColumn(i);
    if (!col) continue;
    const Uint32 attr_id = col->getAttrId();

    if (col->getPrimaryKey() && attr_id != ring_idx_attr_id) {
      pk_prefix_count++;
    }

    if (attr_id != ring_idx_attr_id && attr_id != ring_meta_attr_id &&
        !col->getPrimaryKey() && !col->getNullable()) {
      NdbDictionary::Column::Type ctype = col->getType();
      if (ctype != NdbDictionary::Column::Blob &&
          ctype != NdbDictionary::Column::Text) {
        notnull_count++;
      }
    }
  }

  m_num_pk_prefix_cols = pk_prefix_count;
  m_pk_prefix_cols = new ColumnInfo[pk_prefix_count > 0 ? pk_prefix_count : 1];

  m_num_notnull_cols = notnull_count;
  m_notnull_cols = new ColumnInfo[notnull_count > 0 ? notnull_count : 1];

  // Second pass: populate
  Uint32 pk_idx = 0;
  Uint32 nn_idx = 0;

  for (int i = 0; i < num_cols; i++) {
    const NdbDictionary::Column *col = m_table->getColumn(i);
    if (!col) continue;
    const Uint32 attr_id = col->getAttrId();
    const NdbRecord::Attr *rec_attr =
        findAttrByAttrId(m_ndb_record, attr_id);
    if (!rec_attr) continue;

    if (col->getPrimaryKey() && attr_id != ring_idx_attr_id) {
      ColumnInfo &ci = m_pk_prefix_cols[pk_idx++];
      ci.attr_id = rec_attr->attrId;
      ci.offset = rec_attr->offset;
      ci.max_size = rec_attr->maxSize;
      ci.nullbit_byte_offset = rec_attr->nullbit_byte_offset;
      ci.nullbit_bit_in_byte = rec_attr->nullbit_bit_in_byte;
      ci.flags = rec_attr->flags;
    }

    if (attr_id != ring_idx_attr_id && attr_id != ring_meta_attr_id &&
        !col->getPrimaryKey() && !col->getNullable()) {
      NdbDictionary::Column::Type ctype = col->getType();
      if (ctype != NdbDictionary::Column::Blob &&
          ctype != NdbDictionary::Column::Text) {
        ColumnInfo &ci = m_notnull_cols[nn_idx++];
        ci.attr_id = rec_attr->attrId;
        ci.offset = rec_attr->offset;
        ci.max_size = rec_attr->maxSize;
        ci.nullbit_byte_offset = rec_attr->nullbit_byte_offset;
        ci.nullbit_bit_in_byte = rec_attr->nullbit_bit_in_byte;
        ci.flags = rec_attr->flags;
      }
    }
  }

  // Build pre-computed meta column mask
  // Mask format: byte array indexed by attrId, bit (attrId & 7) in byte
  // (attrId >> 3)
  Uint32 max_attr_id = 0;
  for (Uint32 i = 0; i < m_ndb_record->noOfColumns; i++) {
    if (m_ndb_record->columns[i].attrId > max_attr_id) {
      max_attr_id = m_ndb_record->columns[i].attrId;
    }
  }
  m_mask_byte_size = (max_attr_id / 8) + 1;
  m_meta_mask = new unsigned char[m_mask_byte_size];
  memset(m_meta_mask, 0, m_mask_byte_size);

  // Include all PK columns (including ring_idx) in meta mask
  for (int i = 0; i < num_cols; i++) {
    const NdbDictionary::Column *col = m_table->getColumn(i);
    if (col && col->getPrimaryKey()) {
      Uint32 aid = col->getAttrId();
      m_meta_mask[aid >> 3] |= (1 << (aid & 7));
    }
  }

  // Include ring_meta column
  m_meta_mask[ring_meta_attr_id >> 3] |= (1 << (ring_meta_attr_id & 7));

  // Include NOT NULL non-blob non-PK columns
  for (Uint32 i = 0; i < m_num_notnull_cols; i++) {
    Uint32 aid = m_notnull_cols[i].attr_id;
    m_meta_mask[aid >> 3] |= (1 << (aid & 7));
  }

  return 0;
}

// ---------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------

void NdbRingBufferWriter::setError(int code, const char *msg) {
  m_error_code = code;
  if (msg) {
    strncpy(m_error_message, msg, sizeof(m_error_message) - 1);
    m_error_message[sizeof(m_error_message) - 1] = '\0';
  } else {
    m_error_message[0] = '\0';
  }
}

// ---------------------------------------------------------------
// Buffer manipulation helpers
// ---------------------------------------------------------------

void NdbRingBufferWriter::setRingIdxInBuffer(char *buf, Uint32 value) const {
  // ring_idx is an INT UNSIGNED (4 bytes, little-endian)
  int4store(reinterpret_cast<unsigned char *>(buf + m_ring_idx_info.offset),
            value);
}

void NdbRingBufferWriter::setRingMetaNullInBuffer(char *buf) const {
  // Set the null bit for ring_meta column
  if (m_ring_meta_info.flags & NdbRecord::IsNullable) {
    buf[m_ring_meta_info.nullbit_byte_offset] |=
        (1 << m_ring_meta_info.nullbit_bit_in_byte);
  }
}

void NdbRingBufferWriter::clearRingMetaNullInBuffer(char *buf) const {
  // Clear the null bit for ring_meta column
  if (m_ring_meta_info.flags & NdbRecord::IsNullable) {
    buf[m_ring_meta_info.nullbit_byte_offset] &=
        ~(1 << m_ring_meta_info.nullbit_bit_in_byte);
  }
}

void NdbRingBufferWriter::setRingMetaValueInBuffer(
    char *buf, const unsigned char *packed) const {
  // Clear null bit
  clearRingMetaNullInBuffer(buf);

  // Write VARBINARY value: length prefix + data
  unsigned char *p =
      reinterpret_cast<unsigned char *>(buf + m_ring_meta_info.offset);
  if (m_ring_meta_info.flags & NdbRecord::IsVar1ByteLen) {
    // 1-byte length prefix (VARBINARY with max <= 255)
    p[0] = (unsigned char)RING_META_SIZE;
    memcpy(p + 1, packed, RING_META_SIZE);
  } else if (m_ring_meta_info.flags & NdbRecord::IsVar2ByteLen) {
    // 2-byte length prefix (VARBINARY with max > 255)
    int2store(p, RING_META_SIZE);
    memcpy(p + 2, packed, RING_META_SIZE);
  } else {
    // Fixed-size: just write the data
    memcpy(p, packed, RING_META_SIZE);
  }
}

void NdbRingBufferWriter::zeroNotNullColumnsInBuffer(char *buf) const {
  for (Uint32 i = 0; i < m_num_notnull_cols; i++) {
    const ColumnInfo &ci = m_notnull_cols[i];
    if (ci.flags & NdbRecord::IsVar1ByteLen) {
      // VARBINARY/VARCHAR: set length to 0
      buf[ci.offset] = 0;
    } else if (ci.flags & NdbRecord::IsVar2ByteLen) {
      int2store(reinterpret_cast<unsigned char *>(buf + ci.offset), 0);
    } else {
      memset(buf + ci.offset, 0, ci.max_size);
    }
  }
}

bool NdbRingBufferWriter::pkPrefixMatches(const char *row1,
                                          const char *row2) const {
  for (Uint32 i = 0; i < m_num_pk_prefix_cols; i++) {
    const ColumnInfo &ci = m_pk_prefix_cols[i];
    Uint32 cmp_size = ci.max_size;

    // For variable-length columns, compare the full storage area
    // (length prefix + data) to handle different lengths correctly
    if (ci.flags & NdbRecord::IsVar1ByteLen) {
      Uint32 len1 = 1 + (unsigned char)row1[ci.offset];
      Uint32 len2 = 1 + (unsigned char)row2[ci.offset];
      if (len1 != len2) return false;
      cmp_size = len1;
    } else if (ci.flags & NdbRecord::IsVar2ByteLen) {
      Uint32 len1 =
          2 + uint2korr(reinterpret_cast<const unsigned char *>(
                  row1 + ci.offset));
      Uint32 len2 =
          2 + uint2korr(reinterpret_cast<const unsigned char *>(
                  row2 + ci.offset));
      if (len1 != len2) return false;
      cmp_size = len1;
    }

    if (memcmp(row1 + ci.offset, row2 + ci.offset, cmp_size) != 0) {
      return false;
    }
  }
  return true;
}

void NdbRingBufferWriter::buildDataMask(const unsigned char *userMask,
                                        unsigned char *outMask) const {
  // Start with user's mask
  memcpy(outMask, userMask, m_mask_byte_size);

  // Add ring_idx bit
  Uint32 idx_aid = m_ring_idx_info.attr_id;
  outMask[idx_aid >> 3] |= (1 << (idx_aid & 7));

  // Add ring_meta bit (data row sets it to null, but it must be in the mask)
  Uint32 meta_aid = m_ring_meta_info.attr_id;
  outMask[meta_aid >> 3] |= (1 << (meta_aid & 7));
}

// ---------------------------------------------------------------
// readMetaRow - read the meta row (ring_idx=0) with exclusive lock
// ---------------------------------------------------------------

int NdbRingBufferWriter::readMetaRow(const char *rowBuffer) {
  // Prepare key buffer: copy user row to get PK prefix, set ring_idx=0
  memcpy(m_key_row_buffer, rowBuffer, m_row_size);
  setRingIdxInBuffer(m_key_row_buffer, 0);

  // Prepare result buffer
  memset(m_meta_row_buffer, 0, m_row_size);

  NdbOperation::OperationOptions read_opts;
  memset(&read_opts, 0, sizeof(read_opts));
  read_opts.optionsPresent =
      NdbOperation::OperationOptions::OO_RING_BUFFER_OP |
      NdbOperation::OperationOptions::OO_ABORTOPTION;
  read_opts.abortOption = NdbOperation::AO_IgnoreError;

  const NdbOperation *read_op = m_trans->readTuple(
      m_ndb_record, m_key_row_buffer, m_ndb_record, m_meta_row_buffer,
      NdbOperation::LM_Exclusive, nullptr, &read_opts,
      sizeof(NdbOperation::OperationOptions));

  if (!read_op) {
    const NdbError &err = m_trans->getNdbError();
    setError(err.code, err.message);
    return -1;
  }

  m_trans->execute(NdbTransaction::NoCommit,
                   NdbOperation::DefaultAbortOption);

  /*
   * Check the operation-level error, not the transaction error.
   * With AO_IgnoreError, the transaction may report the 626 as a
   * transaction-level error, but the operation still carries the
   * per-operation status.  Error 626 means "tuple not found" which
   * is the expected case for the first insert on a PK prefix.
   */
  const NdbError &read_err = read_op->getNdbError();
  if (read_err.code == 0) {
    // Meta row found - unpack it
    m_batch_meta_existed = true;

    // Check if ring_meta is null
    bool meta_is_null = false;
    if (m_ring_meta_info.flags & NdbRecord::IsNullable) {
      meta_is_null =
          (m_meta_row_buffer[m_ring_meta_info.nullbit_byte_offset] &
           (1 << m_ring_meta_info.nullbit_bit_in_byte)) != 0;
    }

    if (meta_is_null) {
      // Meta row exists but ring_meta is null - re-init
      m_batch_meta.init_first_insert();
    } else {
      // Read the packed meta value
      const unsigned char *p = reinterpret_cast<const unsigned char *>(
          m_meta_row_buffer + m_ring_meta_info.offset);
      Uint32 data_len = 0;
      const unsigned char *data_ptr = nullptr;

      if (m_ring_meta_info.flags & NdbRecord::IsVar1ByteLen) {
        data_len = p[0];
        data_ptr = p + 1;
      } else if (m_ring_meta_info.flags & NdbRecord::IsVar2ByteLen) {
        data_len = uint2korr(p);
        data_ptr = p + 2;
      } else {
        data_len = m_ring_meta_info.max_size;
        data_ptr = p;
      }

      if (data_len < RING_META_SIZE) {
        // Corrupted - re-init
        m_batch_meta.init_first_insert();
      } else {
        m_batch_meta.unpack(data_ptr);

        // Grow adjustment: after ALTER TABLE increases ring_size,
        // next_pos may point to an occupied slot
        bool ring_full =
            (m_batch_meta.count >= m_ring_buffer_size);
        if (!ring_full && m_batch_meta.next_pos <= m_batch_meta.count) {
          m_batch_meta.next_pos = m_batch_meta.count + 1;
        }
      }
    }
  } else if (read_err.code == 626) {
    // Meta row not found - first insert for this PK prefix.
    // The 626 propagates to theError.code via setOperationErrorCode().
    // Clear it and release the completed read so subsequent writeTuple
    // calls on this transaction are not rejected.
    m_trans->theCommitStatus = NdbTransaction::Started;
    m_trans->theError.code = 0;
    m_trans->releaseCompletedOpsAndQueries();
    m_batch_meta_existed = false;
    m_batch_meta.init_first_insert();
  } else {
    setError(read_err.code, read_err.message);
    return -1;
  }

  return 0;
}

// ---------------------------------------------------------------
// writeDataRow - queue a writeTuple for one data row
// ---------------------------------------------------------------

const NdbOperation *NdbRingBufferWriter::writeDataRow(
    const char *rowBuffer, const unsigned char *userMask) {
  // Compute the data slot from current meta state
  Uint32 data_slot = m_batch_meta.next_pos;

  // Prepare data row buffer: copy user data, set ring_idx and ring_meta
  memcpy(m_data_row_buffer, rowBuffer, m_row_size);
  setRingIdxInBuffer(m_data_row_buffer, data_slot);
  setRingMetaNullInBuffer(m_data_row_buffer);  // data row has null ring_meta

  // Build data mask: user columns + ring_idx + ring_meta
  buildDataMask(userMask, m_data_mask);

  NdbOperation::OperationOptions write_opts;
  memset(&write_opts, 0, sizeof(write_opts));
  write_opts.optionsPresent =
      NdbOperation::OperationOptions::OO_RING_BUFFER_OP;

  // buildSignalsNdbRecord copies mask/buffer at call time, so
  // m_data_row_buffer and m_data_mask are safe to reuse after this returns.
  const NdbOperation *data_op = m_trans->writeTuple(
      m_ndb_record, m_data_row_buffer, m_ndb_record, m_data_row_buffer,
      m_data_mask, &write_opts, sizeof(NdbOperation::OperationOptions));

  if (!data_op) {
    const NdbError &err = m_trans->getNdbError();
    setError(err.code, err.message);
    return nullptr;
  }

  // Advance meta state for next insert
  m_batch_meta.advance(m_ring_buffer_size);

  return data_op;
}

// ---------------------------------------------------------------
// writeMetaRow - insert or update the meta row
// ---------------------------------------------------------------

int NdbRingBufferWriter::writeMetaRow() {
  // Build the meta row buffer
  memset(m_meta_row_buffer, 0, m_row_size);

  // Copy PK prefix from the cached prefix buffer
  for (Uint32 i = 0; i < m_num_pk_prefix_cols; i++) {
    const ColumnInfo &ci = m_pk_prefix_cols[i];
    memcpy(m_meta_row_buffer + ci.offset, m_pk_prefix_buffer + ci.offset,
           ci.max_size);
  }

  // Set ring_idx = 0
  setRingIdxInBuffer(m_meta_row_buffer, 0);

  // Pack and set ring_meta value
  unsigned char meta_packed[RING_META_SIZE];
  m_batch_meta.pack(meta_packed);
  setRingMetaValueInBuffer(m_meta_row_buffer, meta_packed);

  // Zero NOT NULL non-blob user columns in meta buffer
  zeroNotNullColumnsInBuffer(m_meta_row_buffer);

  NdbOperation::OperationOptions meta_opts;
  memset(&meta_opts, 0, sizeof(meta_opts));
  meta_opts.optionsPresent =
      NdbOperation::OperationOptions::OO_RING_BUFFER_OP;

  const NdbOperation *meta_op;
  if (!m_batch_meta_existed) {
    meta_op = m_trans->insertTuple(
        m_ndb_record, m_meta_row_buffer, m_ndb_record, m_meta_row_buffer,
        m_meta_mask, &meta_opts, sizeof(NdbOperation::OperationOptions));
  } else {
    meta_op = m_trans->updateTuple(
        m_ndb_record, m_meta_row_buffer, m_ndb_record, m_meta_row_buffer,
        m_meta_mask, &meta_opts, sizeof(NdbOperation::OperationOptions));
  }

  if (!meta_op) {
    const NdbError &err = m_trans->getNdbError();
    setError(err.code, err.message);
    return -1;
  }

  if (m_trans->execute(NdbTransaction::NoCommit) != 0) {
    const NdbError &err = m_trans->getNdbError();
    setError(err.code, err.message);
    return -1;
  }

  return 0;
}

// ---------------------------------------------------------------
// addRow - main entry point
// ---------------------------------------------------------------

const NdbOperation *NdbRingBufferWriter::addRow(
    const char *rowBuffer, const unsigned char *userMask) {
  if (m_error_code != 0) {
    return nullptr;  // constructor failed
  }

  if (!rowBuffer || !userMask) {
    setError(4000, "NdbRingBufferWriter::addRow: null argument");
    return nullptr;
  }

  // Debug: clear any stale error from previous operations
  m_error_code = 0;
  m_error_message[0] = '\0';

  // Path A: batch hit - same PK prefix as current batch
  if (m_batch_active) {
    if (pkPrefixMatches(rowBuffer, m_pk_prefix_buffer)) {
      // Advance meta in memory and queue data write (no execute)
      const NdbOperation *op = writeDataRow(rowBuffer, userMask);
      return op;
    }

    // PK prefix changed - flush old batch
    if (flush() != 0) {
      return nullptr;
    }
  }

  // Path B: new batch - read meta row, queue first data write
  if (readMetaRow(rowBuffer) != 0) {
    return nullptr;
  }

  // Cache PK prefix for subsequent batch-hit comparisons
  memcpy(m_pk_prefix_buffer, rowBuffer, m_row_size);

  // Queue the data write
  const NdbOperation *op = writeDataRow(rowBuffer, userMask);
  if (!op) {
    return nullptr;
  }

  // Mark batch as active
  m_batch_active = true;

  return op;
}

// ---------------------------------------------------------------
// flush - finalize pending batch
// ---------------------------------------------------------------

int NdbRingBufferWriter::flush() {
  if (!m_batch_active) {
    return 0;  // nothing to flush
  }

  int ret = writeMetaRow();
  m_batch_active = false;
  return ret;
}
