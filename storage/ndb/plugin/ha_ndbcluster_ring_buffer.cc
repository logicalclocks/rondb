/* Copyright (c) 2022, 2025, Hopsworks and/or its affiliates.

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
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/*
  Ring Buffer Table — extracted from ha_ndbcluster.cc (see
  ha_ndbcluster_ring_buffer.h). This TU owns the Ring_meta on-disk layout,
  the DELETE WHERE Item-tree walker, and the two ring-buffer member function
  bodies of ha_ndbcluster. Everything else (call-site hooks in write/update/
  delete/scan/create paths) stays in ha_ndbcluster.cc.
*/

#include "storage/ndb/plugin/ha_ndbcluster_ring_buffer.h"
#include "storage/ndb/plugin/ha_ndbcluster.h"

#include <cstdint>
#include <cstring>
#include <string>

#include "my_dbug.h"
#include "mysql/strings/m_ctype.h"
#include "sql/item.h"
#include "sql/item_cmpfunc.h"
#include "sql/key.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/table.h"
#include "storage/ndb/include/ndbapi/NdbApi.hpp"
#include "storage/ndb/plugin/ndb_modifiers.h"
#include "storage/ndb/plugin/ndb_table_map.h"
#include "storage/ndb/plugin/ndb_thd_ndb.h"

/*
  Plugin-internal helper defined in ha_ndbcluster.cc. Declared here so the
  ring-buffer member function bodies that were moved out of ha_ndbcluster.cc
  can still call it. Kept out of any header to limit symbol exposure — only
  this TU needs the cross-file reference.
*/
int execute_no_commit(Thd_ndb *thd_ndb, NdbTransaction *trans,
                      bool ignore_no_key, uint *ignore_count = nullptr);

/**
  Ring buffer meta row format (32 bytes at ring_idx=0, ring_meta column):
  Offset  Size  Field
  0       2     version (1)
  2       2     reserved_0
  4       4     next_pos (1-based, wraps at ring_size)
  8       4     count (0 to ring_size)
  12      4     reserved_1
  16      8     total_inserts (monotonic)
  24      8     reserved_2
*/
static const Uint32 RING_META_VERSION = 1;
static const Uint32 RING_META_SIZE = ndb_ring_buffer::META_SIZE;

namespace {

struct Ring_meta {
  Uint16 version;
  Uint16 reserved_0;
  Uint32 next_pos;
  Uint32 count;
  Uint32 reserved_1;
  Uint64 total_inserts;
  Uint64 reserved_2;

  void pack(uchar *buf) const {
    int2store(buf + 0, version);
    int2store(buf + 2, reserved_0);
    int4store(buf + 4, next_pos);
    int4store(buf + 8, count);
    int4store(buf + 12, reserved_1);
    int8store(buf + 16, total_inserts);
    int8store(buf + 24, reserved_2);
  }

  void unpack(const uchar *buf) {
    version = uint2korr(buf + 0);
    reserved_0 = uint2korr(buf + 2);
    next_pos = uint4korr(buf + 4);
    count = uint4korr(buf + 8);
    reserved_1 = uint4korr(buf + 12);
    total_inserts = uint8korr(buf + 16);
    reserved_2 = uint8korr(buf + 24);
  }

  void init_first_insert(Uint32 ring_size) {
    version = RING_META_VERSION;
    reserved_0 = 0;
    next_pos = 1;  /* First insert goes to slot 1 */
    count = 0;
    reserved_1 = 0;
    total_inserts = 0;
    reserved_2 = 0;
    advance(ring_size);  /* Sets next_pos, count=1, total_inserts=1 */
  }

  void advance(Uint32 ring_size) {
    next_pos = (next_pos % ring_size) + 1;
    if (count < ring_size) count++;
    total_inserts++;
  }
};

/**
 * Check if a condition Item only references PK-prefix columns
 * (all PK columns except ring_idx). Recursively walks the Item tree.
 *
 * This validates that a DELETE WHERE clause on a ring-buffer table
 * will only delete complete rings. Any field reference to ring_idx
 * or a non-PK column causes rejection. Accepts any operator (=, IN,
 * >, <, >=, <=, BETWEEN, etc.) — the constraint is on which columns
 * are referenced, not which operators are used.
 */
bool check_ring_buffer_delete_condition(const TABLE *table,
                                        uint ring_idx_field_index,
                                        const KEY *pk_info,
                                        const Item *item) {
  switch (item->type()) {
    case Item::FIELD_ITEM: {
      const Item_field *f = down_cast<const Item_field *>(item);
      if (f->field->table != table) return false;
      if (f->field->field_index() == ring_idx_field_index) return false;
      for (uint i = 0; i < pk_info->user_defined_key_parts; i++) {
        if (pk_info->key_part[i].field->field_index() ==
            f->field->field_index())
          return true;
      }
      return false; /* non-PK column */
    }
    case Item::FUNC_ITEM: {
      const Item_func *func = down_cast<const Item_func *>(item);
      for (uint i = 0; i < func->argument_count(); i++) {
        const Item *arg = func->arguments()[i];
        if (!check_ring_buffer_delete_condition(table, ring_idx_field_index,
                                                pk_info, arg))
          return false;
      }
      return true;
    }
    case Item::COND_ITEM: {
      const Item_cond *cond = down_cast<const Item_cond *>(item);
      List<Item> *args = const_cast<Item_cond *>(cond)->argument_list();
      List_iterator<Item> it(*args);
      Item *arg;
      while ((arg = it++)) {
        if (!check_ring_buffer_delete_condition(table, ring_idx_field_index,
                                                pk_info, arg))
          return false;
      }
      return true;
    }
    default:
      if (item->const_item() || item->type() == Item::PARAM_ITEM) return true;
      return false;
  }
}

}  // anonymous namespace

namespace ndb_ring_buffer {

bool delete_where_allowed(const TABLE *table, unsigned ring_idx_field_index,
                          const Item *cond) {
  if (cond == nullptr) return true; /* bare DELETE: clears entire table, safe */
  const KEY *pk_info = table->key_info + table->s->primary_key;
  return check_ring_buffer_delete_condition(table, ring_idx_field_index,
                                            pk_info, cond);
}

bool show_meta_active(THD *thd, bool is_ring_buffer, bool delete_allowed) {
  if (thdvar_show_meta(thd)) return true;
  if (delete_allowed) return true;
  if (is_ring_buffer && thd_sql_command(thd) == SQLCOM_ALTER_TABLE) return true;
  return false;
}

const char *parse_spec(const NDB_Modifier *mod, Spec *out) {
  std::string rb_comment(mod->m_val_str.str, mod->m_val_str.len);

  /* "off" keyword — caller owns the policy of what "off" means. */
  if (!my_strcasecmp(system_charset_info, mod->m_val_str.str, "off")) {
    out->is_off = true;
    return nullptr;
  }

  /*
    Grammar: SIZE  |  SIZE@idx_col@meta_col
    When the @col@col suffix is omitted, default to ring_idx / ring_meta.
  */
  std::string size_str;
  std::size_t pos1 = rb_comment.find('@');
  if (pos1 == std::string::npos) {
    size_str = rb_comment;
    out->idx_col_name = "ring_idx";
    out->meta_col_name = "ring_meta";
  } else {
    static const char *kFormatErr =
        "MAX_ROWS_PER_PK format: 'SIZE' or 'SIZE@idx_col@meta_col'";
    if (pos1 == 0) return kFormatErr;
    std::size_t pos2 = rb_comment.find('@', pos1 + 1);
    if (pos2 == std::string::npos || pos2 == pos1 + 1) return kFormatErr;
    if (rb_comment.find('@', pos2 + 1) != std::string::npos) return kFormatErr;
    size_str = rb_comment.substr(0, pos1);
    out->idx_col_name = rb_comment.substr(pos1 + 1, pos2 - pos1 - 1);
    out->meta_col_name = rb_comment.substr(pos2 + 1);
    if (out->meta_col_name.empty()) return kFormatErr;
  }

  static const char *kSizeRangeErr =
      "MAX_ROWS_PER_PK size must be >= 1 and <= 2147483647";
  if (size_str.empty()) return kSizeRangeErr;
  for (std::size_t i = 0; i < size_str.length(); i++) {
    if (size_str.at(i) < '0' || size_str.at(i) > '9') {
      return "Invalid MAX_ROWS_PER_PK format, size must be a positive integer";
    }
  }
  if (size_str.length() > 10) return kSizeRangeErr;
  std::int64_t size_raw = std::stoll(size_str);
  if (size_raw < 1 || size_raw > 0x7FFFFFFF) return kSizeRangeErr;
  out->size = static_cast<std::uint32_t>(size_raw);
  return nullptr;
}

const char *validate_columns_mysql(const TABLE *table, const Spec &spec) {
  /* ring_idx column: INT with DEFAULT, last column of PK. */
  bool found_idx = false;
  for (uint i = 0; i < table->s->fields; i++) {
    Field *const field = table->field[i];
    if (!my_strcasecmp(system_charset_info, field->field_name,
                       spec.idx_col_name.c_str())) {
      if (field->real_type() != MYSQL_TYPE_LONG)
        return "Ring index column must be INT type";
      if (field->is_flag_set(NO_DEFAULT_VALUE_FLAG))
        return "Ring index column must have DEFAULT (e.g. 0)";
      found_idx = true;
      break;
    }
  }
  if (!found_idx) return "Ring index column not found in table";

  KEY *pk = &table->s->key_info[0];
  uint last_pk_idx = pk->user_defined_key_parts - 1;
  if (my_strcasecmp(system_charset_info,
                    pk->key_part[last_pk_idx].field->field_name,
                    spec.idx_col_name.c_str())) {
    return "Ring index column must be the last column of the PRIMARY KEY";
  }

  /* ring_meta column: VARBINARY with length >= META_SIZE, nullable. */
  bool found_meta = false;
  for (uint i = 0; i < table->s->fields; i++) {
    Field *const field = table->field[i];
    if (!my_strcasecmp(system_charset_info, field->field_name,
                       spec.meta_col_name.c_str())) {
      if (field->real_type() != MYSQL_TYPE_VARCHAR ||
          field->charset() != &my_charset_bin)
        return "Ring meta column must be VARBINARY type";
      if (field->field_length < META_SIZE)
        return "Ring meta column must be VARBINARY with length >= 32";
      if (!field->is_nullable()) return "Ring meta column must be nullable";
      found_meta = true;
      break;
    }
  }
  if (!found_meta) return "Ring meta column not found in table";

  /* No NOT NULL BLOB/TEXT user columns (meta rows cannot set zero-defaults
     for BLOB types → NDB error 839 at runtime). */
  for (uint i = 0; i < table->s->fields; i++) {
    Field *const field = table->field[i];
    if (!my_strcasecmp(system_charset_info, field->field_name,
                       spec.idx_col_name.c_str()) ||
        !my_strcasecmp(system_charset_info, field->field_name,
                       spec.meta_col_name.c_str())) {
      continue;
    }
    if (!field->is_nullable()) {
      enum_field_types ft = field->real_type();
      if (ft == MYSQL_TYPE_BLOB || ft == MYSQL_TYPE_TINY_BLOB ||
          ft == MYSQL_TYPE_MEDIUM_BLOB || ft == MYSQL_TYPE_LONG_BLOB) {
        return "Ring buffer tables cannot have NOT NULL BLOB/TEXT columns";
      }
    }
  }
  return nullptr;
}

const char *apply_columns_ndb(NdbDictionary::Table *new_tab,
                              const Spec &spec) {
  NdbDictionary::Column *idx_col = new_tab->getColumn(spec.idx_col_name.c_str());
  NdbDictionary::Column *meta_col =
      new_tab->getColumn(spec.meta_col_name.c_str());
  if (idx_col == nullptr || meta_col == nullptr)
    return "Ring buffer column not found in table";
  new_tab->setRingBufferSize(spec.size);
  new_tab->setRingIdxColumnNo(idx_col->getColumnNo());
  new_tab->setRingMetaColumnNo(meta_col->getColumnNo());
  return nullptr;
}

int check_index_columns(THD *thd, const NdbDictionary::Table *ndbtab,
                        const KEY *key_info, bool is_primary_key_index) {
  if (!ndbtab->isRingBuffer() || is_primary_key_index) return 0;

  const NdbDictionary::Column *ring_idx_col =
      ndbtab->getColumn(ndbtab->getRingIdxColumnNo());
  const NdbDictionary::Column *ring_meta_col =
      ndbtab->getColumn(ndbtab->getRingMetaColumnNo());
  const KEY_PART_INFO *key_part = key_info->key_part;
  const KEY_PART_INFO *end = key_part + key_info->user_defined_key_parts;
  for (; key_part != end; key_part++) {
    const char *col_name = key_part->field->field_name;
    if ((ring_idx_col && strcmp(col_name, ring_idx_col->getName()) == 0) ||
        (ring_meta_col && strcmp(col_name, ring_meta_col->getName()) == 0)) {
      if (thd_sql_command(thd) == SQLCOM_ALTER_TABLE ||
          thd_sql_command(thd) == SQLCOM_CREATE_INDEX) {
        /* Inplace ALTER: set error directly; HA_ERR_GENERIC tells
           print_error() to skip (error already reported). */
        my_printf_error(ER_ILLEGAL_HA_CREATE_OPTION,
                        "Cannot create index on ring buffer internal "
                        "column '%s'",
                        MYF(0), col_name);
        return HA_ERR_GENERIC;
      }
      /* CREATE TABLE: push warning; create_helper wraps it with
         ER_CANT_CREATE_TABLE. */
      push_warning_printf(thd, Sql_condition::SL_WARNING,
                          ER_ILLEGAL_HA_CREATE_OPTION,
                          "Cannot create index on ring buffer internal "
                          "column '%s'",
                          col_name);
      return HA_ERR_UNSUPPORTED;
    }
  }
  return 0;
}

}  // namespace ndb_ring_buffer

/**
  Flush the pending ring-buffer meta write for the current batch.

  Called when a PK prefix changes mid-batch or from end_bulk_insert().
  Writes the cached meta state to NDB using record[1] (which holds the
  PK prefix from the meta read) and executes.
*/
int ha_ndbcluster::flush_ring_buffer_batch() {
  DBUG_TRACE;
  if (!m_rb_batch_active) return 0;

  Thd_ndb *thd_ndb = m_thd_ndb;
  NdbTransaction *trans = thd_ndb->trans;
  assert(trans);

  const Uint32 ring_idx_col_no = m_table->getRingIdxColumnNo();
  const Uint32 ring_meta_col_no = m_table->getRingMetaColumnNo();
  Field *ring_idx_field = table->field[ring_idx_col_no];
  Field *ring_meta_field = table->field[ring_meta_col_no];

  const NdbRecord *key_rec =
      m_index[table_share->primary_key].ndb_unique_record_row;

  /*
   * Ensure ring columns are in write_set/read_set.
   * They may have been cleared by write_row() cleanup if we're called
   * from end_bulk_insert().
   *
   * We intentionally do NOT clear these bits on exit — the caller is
   * responsible for cleanup.  When called from write_row(), the cleanup
   * label clears them; when called from end_bulk_insert(), the caller
   * clears them explicitly after this function returns.
   */
  bitmap_set_bit(table->write_set, ring_idx_field->field_index());
  bitmap_set_bit(table->write_set, ring_meta_field->field_index());
  bitmap_set_bit(table->read_set, ring_idx_field->field_index());
  bitmap_set_bit(table->read_set, ring_meta_field->field_index());

  /* Pack cached meta state */
  Ring_meta meta;
  meta.version = RING_META_VERSION;
  meta.reserved_0 = 0;
  meta.next_pos = m_rb_batch_next_pos;
  meta.count = m_rb_batch_count;
  meta.reserved_1 = 0;
  meta.total_inserts = m_rb_batch_total_inserts;
  meta.reserved_2 = 0;

  uchar meta_packed[RING_META_SIZE];
  meta.pack(meta_packed);

  /* Prepare record[1] for meta write */
  uchar *meta_rec = table->record[1];
  ptrdiff_t row_offset = meta_rec - table->record[0];

  ring_idx_field->move_field_offset(row_offset);
  ring_idx_field->store(0, true);
  ring_idx_field->move_field_offset(-row_offset);

  ring_meta_field->move_field_offset(row_offset);
  ring_meta_field->set_notnull();
  ring_meta_field->store((const char *)meta_packed, RING_META_SIZE,
                         &my_charset_bin);
  ring_meta_field->move_field_offset(-row_offset);

  /* Build meta column mask: PK columns + ring_meta + NOT NULL user columns */
  const Uint32 bitmapSz = (NDB_MAX_ATTRIBUTES_IN_TABLE + 31) / 32;
  uint32 metaMaskSpace[bitmapSz];
  MY_BITMAP metaMask;
  bitmap_init(&metaMask, metaMaskSpace, table->s->fields);

  KEY *pk_info = table->key_info + table_share->primary_key;
  for (uint i = 0; i < pk_info->user_defined_key_parts; i++) {
    bitmap_set_bit(&metaMask, pk_info->key_part[i].field->field_index());
  }
  bitmap_set_bit(&metaMask, ring_meta_field->field_index());

  /*
   * Include NOT NULL user columns with zero-defaults so that
   * DBTUP's checkNullAttributes() does not reject the meta row.
   * Skip BLOB/TEXT columns (handled by NdbBlob separately).
   */
  ptrdiff_t nn_offset = meta_rec - table->record[0];
  for (uint i = 0; i < table->s->fields; i++) {
    Field *f = table->field[i];
    if (!bitmap_is_set(&metaMask, i) && !f->is_nullable()) {
      enum_field_types ft = f->real_type();
      if (ft == MYSQL_TYPE_BLOB || ft == MYSQL_TYPE_TINY_BLOB ||
          ft == MYSQL_TYPE_MEDIUM_BLOB || ft == MYSQL_TYPE_LONG_BLOB) {
        continue;
      }
      f->move_field_offset(nn_offset);
      f->reset();
      f->move_field_offset(-nn_offset);
      bitmap_set_bit(&metaMask, i);
    }
  }

  uchar *meta_mask = m_table_map->get_column_mask(&metaMask);

  NdbOperation::OperationOptions meta_opts;
  memset(&meta_opts, 0, sizeof(meta_opts));
  meta_opts.optionsPresent =
      NdbOperation::OperationOptions::OO_RING_BUFFER_OP;

  const NdbOperation *meta_op;
  if (!m_rb_batch_meta_existed) {
    meta_op = trans->insertTuple(key_rec, (const char *)meta_rec, m_ndb_record,
                                 (char *)meta_rec, meta_mask, &meta_opts,
                                 sizeof(NdbOperation::OperationOptions));
  } else {
    meta_op = trans->updateTuple(key_rec, (const char *)meta_rec, m_ndb_record,
                                 (char *)meta_rec, meta_mask, &meta_opts,
                                 sizeof(NdbOperation::OperationOptions));
  }

  if (!meta_op) {
    m_rb_batch_active = false;
    return ndb_err(trans);
  }

  if (execute_no_commit(thd_ndb, trans, false) != 0) {
    m_rb_batch_active = false;
    return ndb_err(trans);
  }

  m_rb_batch_active = false;
  return 0;
}

/**
  Insert one record into a ring-buffer NDB table.

  Handles meta row management (ring_idx=0) and assigns ring_idx
  automatically. Sets OO_RING_BUFFER_OP on all write operations.

  For bulk INSERTs (m_rows_to_insert > 1), caches meta state in memory
  and batches data writes to reduce NDB round-trips from 2N to 2 per
  PK-prefix group.

  Note on affected rows: each ring buffer INSERT always modifies 2 NDB rows
  (meta row + data row), but the MySQL handler framework counts each
  successful write_row() call as 1 affected row. The client therefore always
  sees "1 row affected". Changing this would require overriding the SQL
  layer's counting, which is non-trivial. This may need revisiting if users
  expect the affected rows count to reflect the actual NDB operations.
*/
int ha_ndbcluster::ndb_ring_buffer_write_row(uchar *record) {
  DBUG_TRACE;
  THD *thd = table->in_use;
  Thd_ndb *thd_ndb = m_thd_ndb;
  int error = 0;

  const Uint32 ring_buffer_size = m_table->getRingBufferSize();
  const Uint32 ring_idx_col_no = m_table->getRingIdxColumnNo();
  const Uint32 ring_meta_col_no = m_table->getRingMetaColumnNo();

  /* Get MySQL Field objects for the ring columns */
  Field *ring_idx_field = table->field[ring_idx_col_no];
  Field *ring_meta_field = table->field[ring_meta_col_no];

  /* Check if table has BLOB/TEXT columns that need special handling */
  const bool uses_blobs = uses_blob_value(table->write_set);

  /*
   * Block REPLACE and INSERT ON DUPLICATE KEY UPDATE.
   * These don't make sense for ring-buffer tables because
   * the user doesn't control ring_idx (the key component).
   */
  if (thd->lex->sql_command == SQLCOM_REPLACE ||
      thd->lex->sql_command == SQLCOM_REPLACE_SELECT) {
    my_error(ER_ILLEGAL_HA, MYF(0),
             "REPLACE is not allowed on ring-buffer tables");
    return HA_ERR_UNSUPPORTED;
  }
  if (thd->lex->duplicates == DUP_UPDATE) {
    my_error(ER_ILLEGAL_HA, MYF(0),
             "INSERT ON DUPLICATE KEY UPDATE is not allowed on "
             "ring-buffer tables");
    return HA_ERR_UNSUPPORTED;
  }

  /*
   * Block user-specified ring_idx or ring_meta in INSERT.
   * These columns are system-managed.
   */
  if (bitmap_is_set(table->write_set, ring_idx_field->field_index())) {
    my_error(ER_ILLEGAL_HA, MYF(0),
             "Cannot specify ring_idx column in INSERT on ring-buffer table");
    return HA_ERR_UNSUPPORTED;
  }
  if (bitmap_is_set(table->write_set, ring_meta_field->field_index())) {
    my_error(ER_ILLEGAL_HA, MYF(0),
             "Cannot specify ring_meta column in INSERT on ring-buffer table");
    return HA_ERR_UNSUPPORTED;
  }

  /*
   * Add ring columns to write_set — we are writing them as part of
   * ring management. The user-specified checks above already verified
   * these bits were NOT set, so we own them from here on.
   */
  bitmap_set_bit(table->write_set, ring_idx_field->field_index());
  bitmap_set_bit(table->write_set, ring_meta_field->field_index());
  /* Also add to read_set — we read ring_meta from the meta row to unpack it */
  bitmap_set_bit(table->read_set, ring_idx_field->field_index());
  bitmap_set_bit(table->read_set, ring_meta_field->field_index());

  /* Save user's original ring_idx value and track cleanup state */
  const ptrdiff_t ring_idx_offset = ring_idx_field->offset(table->record[0]);
  uchar saved_ring_idx[8];
  memcpy(saved_ring_idx, record + ring_idx_offset,
         ring_idx_field->pack_length());
  int ret = 0;

  /* Ensure transaction exists */
  NdbTransaction *trans = thd_ndb->trans;
  const NdbRecord *key_rec =
      m_index[table_share->primary_key].ndb_unique_record_row;
  if (!trans) {
    ring_idx_field->store(0, true);
    if (unlikely(!(trans = start_transaction_row(key_rec, record, error)))) {
      ret = error;
      goto cleanup;
    }
  }

  /*
   * Path A: Check if we have a cached batch for the same PK prefix.
   * If so, compute the next slot in memory and queue the data write
   * without a meta read or execute.
   */
  if (m_rb_batch_active) {
    bool prefix_match = true;
    KEY *pk_info = table->key_info + table_share->primary_key;
    for (uint i = 0; i < pk_info->user_defined_key_parts; i++) {
      Field *kp_field = pk_info->key_part[i].field;
      if (kp_field->field_index() == ring_idx_col_no) continue;
      ptrdiff_t off = kp_field->offset(table->record[0]);
      if (memcmp(record + off, table->record[1] + off,
                 kp_field->pack_length()) != 0) {
        prefix_match = false;
        break;
      }
    }

    if (prefix_match) {
      /* Batch hit: advance cached meta state in memory */
      Uint32 data_slot = m_rb_batch_next_pos;
      bool ring_full = (m_rb_batch_count >= ring_buffer_size);
      m_rb_batch_next_pos = (m_rb_batch_next_pos % ring_buffer_size) + 1;
      if (m_rb_batch_count < ring_buffer_size) m_rb_batch_count++;
      m_rb_batch_total_inserts++;

      /* Queue data write at ring_idx=data_slot */
      ring_idx_field->store(data_slot, true);
      ring_meta_field->set_null();

      uchar *data_mask = m_table_map->get_column_mask(table->write_set);

      NdbOperation::OperationOptions write_opts;
      memset(&write_opts, 0, sizeof(write_opts));
      write_opts.optionsPresent =
          NdbOperation::OperationOptions::OO_RING_BUFFER_OP;

      const NdbOperation *data_op = trans->writeTuple(
          key_rec, (const char *)record, m_ndb_record, (char *)record,
          data_mask, &write_opts, sizeof(NdbOperation::OperationOptions));

      if (!data_op) {
        ret = ndb_err(trans);
        goto cleanup;
      }

      /* Set BLOB/TEXT column values if present */
      if (uses_blobs) {
        my_bitmap_map *old_map =
            dbug_tmp_use_all_columns(table, table->read_set);
        uint blob_count = 0;
        int blob_res = set_blob_values(data_op, record - table->record[0],
                                       table->write_set, &blob_count, true);
        dbug_tmp_restore_column_map(table->read_set, old_map);
        if (blob_res != 0) {
          ret = blob_res;
          goto cleanup;
        }
        thd_ndb->m_unsent_blob_ops = true;
      }

      ha_statistic_increment(&System_status_var::ha_write_count);
      m_trans_table_stats->update_uncommitted_rows(ring_full ? 0 : 1);

      /*
       * Check if NDB operation buffer is getting full.  If so, flush
       * the batch (meta write + execute) so that the next row re-enters
       * via Path B with a fresh meta read.  This bounds memory usage
       * the same way normal table bulk inserts do.
       */
      if (thd_ndb->add_row_check_if_batch_full(m_bytes_per_write)) {
        ret = flush_ring_buffer_batch();
        if (ret != 0) goto cleanup;
      }

      goto cleanup;
    }

    /* Prefix mismatch: flush old batch before reading new meta */
    ret = flush_ring_buffer_batch();
    if (ret != 0) goto cleanup;
  }

  {
    /*
     * Step 1: Read meta row (ring_idx=0) with exclusive lock.
     * Use table->record[1] as the result buffer.
     */
    uchar *meta_result = table->record[1];
    memcpy(meta_result, record, table->s->reclength);

    ring_idx_field->store(0, true);

    NdbOperation::OperationOptions read_opts;
    memset(&read_opts, 0, sizeof(read_opts));
    read_opts.optionsPresent =
        NdbOperation::OperationOptions::OO_RING_BUFFER_OP;

    const NdbOperation *read_op = trans->readTuple(
        key_rec, (const char *)record, m_ndb_record, (char *)meta_result,
        NdbOperation::LM_Exclusive, nullptr, &read_opts,
        sizeof(NdbOperation::OperationOptions));

    if (!read_op) {
      ret = ndb_err(trans);
      goto cleanup;
    }

    if (execute_no_commit(thd_ndb, trans, true /* ignore_no_key */) != 0) {
      ret = ndb_err(trans);
      goto cleanup;
    }

    /*
     * Step 2: Check if meta row was found. Error 626 = row not found.
     */
    const NdbError &read_err = read_op->getNdbError();
    const bool meta_exists = (read_err.code == 0);
    const bool meta_not_found = (read_err.code == 626);

    if (!meta_exists && !meta_not_found) {
      ret = ndb_err(trans);
      goto cleanup;
    }

    Ring_meta meta;
    Uint32 data_slot;
    bool ring_full = false;

    if (meta_not_found) {
      meta.init_first_insert(ring_buffer_size);
      data_slot = 1;
    } else {
      /* Unpack ring_meta from meta_result (record[1]) */
      Field *meta_field_in_result = table->field[ring_meta_col_no];
      ptrdiff_t row_offset = meta_result - table->record[0];
      meta_field_in_result->move_field_offset(row_offset);

      if (meta_field_in_result->is_null()) {
        meta.init_first_insert(ring_buffer_size);
        data_slot = 1;
      } else {
        String meta_str;
        meta_field_in_result->val_str(&meta_str);
        if (meta_str.length() < RING_META_SIZE) {
          meta.init_first_insert(ring_buffer_size);
          data_slot = 1;
        } else {
          meta.unpack((const uchar *)meta_str.ptr());
          ring_full = (meta.count >= ring_buffer_size);
          /*
           * Grow adjustment: after ALTER TABLE increases ring_size,
           * next_pos may point to an occupied slot (it wrapped at
           * the old smaller size). Redirect to first empty slot.
           */
          if (!ring_full && meta.next_pos <= meta.count) {
            meta.next_pos = meta.count + 1;
          }
          data_slot = meta.next_pos;
          meta.advance(ring_buffer_size);
        }
      }

      meta_field_in_result->move_field_offset(-row_offset);
    }

    /*
     * Update row count statistics.
     *
     * ha_write_count: always +1 (one handler write call).
     *
     * uncommitted_rows: reflects the net change in number of rows in the
     * table, used by stats.records for SHOW TABLE STATUS and the optimizer.
     *   - First insert for PK prefix: +2 (new meta row + new data row)
     *   - Ring not full: +1 (meta row updated, new data row added)
     *   - Ring full (overwrite): +0 (meta row updated, data row overwritten)
     * An alternative is to always pass +1 (one user-visible INSERT) like
     * the normal ndb_write_row() path, but we choose accuracy here since
     * the meta row is a real row occupying storage.
     */
    ha_statistic_increment(&System_status_var::ha_write_count);
    {
      int row_delta;
      if (meta_not_found) {
        row_delta = 2;  /* new meta row + new data row */
      } else if (!ring_full) {
        row_delta = 1;  /* meta updated, new data row */
      } else {
        row_delta = 0;  /* meta updated, data row overwritten */
      }
      m_trans_table_stats->update_uncommitted_rows(row_delta);
    }

    if (m_rows_to_insert > 1) {
      /*
       * Path B: Bulk mode — queue data write only, defer meta write.
       * Cache meta state for subsequent same-prefix rows (Path A).
       */
      ring_idx_field->store(data_slot, true);
      ring_meta_field->set_null();

      uchar *data_mask = m_table_map->get_column_mask(table->write_set);

      NdbOperation::OperationOptions write_opts;
      memset(&write_opts, 0, sizeof(write_opts));
      write_opts.optionsPresent =
          NdbOperation::OperationOptions::OO_RING_BUFFER_OP;

      const NdbOperation *data_op = trans->writeTuple(
          key_rec, (const char *)record, m_ndb_record, (char *)record,
          data_mask, &write_opts, sizeof(NdbOperation::OperationOptions));

      if (!data_op) {
        ret = ndb_err(trans);
        goto cleanup;
      }

      /* Set BLOB/TEXT column values if present */
      if (uses_blobs) {
        my_bitmap_map *old_map =
            dbug_tmp_use_all_columns(table, table->read_set);
        uint blob_count = 0;
        int blob_res = set_blob_values(data_op, record - table->record[0],
                                       table->write_set, &blob_count, true);
        dbug_tmp_restore_column_map(table->read_set, old_map);
        if (blob_res != 0) {
          ret = blob_res;
          goto cleanup;
        }
        thd_ndb->m_unsent_blob_ops = true;
      }

      /* Ensure record[1] has ring_idx=0 for meta write at flush */
      ptrdiff_t rec1_offset = table->record[1] - table->record[0];
      ring_idx_field->move_field_offset(rec1_offset);
      ring_idx_field->store(0, true);
      ring_idx_field->move_field_offset(-rec1_offset);

      /* Cache batch state */
      m_rb_batch_active = true;
      m_rb_batch_meta_existed = !meta_not_found;
      m_rb_batch_next_pos = meta.next_pos;
      m_rb_batch_count = meta.count;
      m_rb_batch_total_inserts = meta.total_inserts;

      /*
       * Account for both the data row and the deferred meta row write.
       * The meta row is smaller (PK + ring_meta), but we use
       * m_bytes_per_write as a conservative upper bound to ensure the
       * batch-full check doesn't let us exceed NDB's hard limits.
       */
      thd_ndb->m_unsent_bytes += 2 * m_bytes_per_write;
    } else {
      /*
       * Path C: Single-row mode — queue data write + meta write, execute.
       * This is the original behavior, unchanged.
       */
      ring_idx_field->store(data_slot, true);
      ring_meta_field->set_null();

      uchar *data_mask = m_table_map->get_column_mask(table->write_set);

      NdbOperation::OperationOptions write_opts;
      memset(&write_opts, 0, sizeof(write_opts));
      write_opts.optionsPresent =
          NdbOperation::OperationOptions::OO_RING_BUFFER_OP;

      const NdbOperation *data_op = trans->writeTuple(
          key_rec, (const char *)record, m_ndb_record, (char *)record,
          data_mask, &write_opts, sizeof(NdbOperation::OperationOptions));

      if (!data_op) {
        ret = ndb_err(trans);
        goto cleanup;
      }

      /* Set BLOB/TEXT column values if present */
      if (uses_blobs) {
        my_bitmap_map *old_map =
            dbug_tmp_use_all_columns(table, table->read_set);
        uint blob_count = 0;
        int blob_res = set_blob_values(data_op, record - table->record[0],
                                       table->write_set, &blob_count, false);
        dbug_tmp_restore_column_map(table->read_set, old_map);
        if (blob_res != 0) {
          ret = blob_res;
          goto cleanup;
        }
      }

      /*
       * Insert or update meta row.
       * Use record[1] as a separate buffer so we don't corrupt user
       * column values in record[0] (which the data_op still references).
       */
      uchar meta_packed[RING_META_SIZE];
      meta.pack(meta_packed);

      uchar *meta_rec = table->record[1];
      memcpy(meta_rec, record, table->s->reclength);
      ptrdiff_t row_offset = meta_rec - table->record[0];

      ring_idx_field->move_field_offset(row_offset);
      ring_idx_field->store(0, true);
      ring_idx_field->move_field_offset(-row_offset);

      ring_meta_field->move_field_offset(row_offset);
      ring_meta_field->set_notnull();
      ring_meta_field->store((const char *)meta_packed, RING_META_SIZE,
                             &my_charset_bin);
      ring_meta_field->move_field_offset(-row_offset);

      /* Meta mask: PK columns + ring_meta + NOT NULL user columns */
      const Uint32 bitmapSz = (NDB_MAX_ATTRIBUTES_IN_TABLE + 31) / 32;
      uint32 metaMaskSpace[bitmapSz];
      MY_BITMAP metaMask;
      bitmap_init(&metaMask, metaMaskSpace, table->s->fields);

      KEY *pk_info = table->key_info + table_share->primary_key;
      for (uint i = 0; i < pk_info->user_defined_key_parts; i++) {
        bitmap_set_bit(&metaMask, pk_info->key_part[i].field->field_index());
      }
      bitmap_set_bit(&metaMask, ring_meta_field->field_index());

      /*
       * Include NOT NULL user columns with zero-defaults so that
       * DBTUP's checkNullAttributes() does not reject the meta row.
       * Skip BLOB/TEXT columns (handled by NdbBlob separately).
       */
      for (uint i = 0; i < table->s->fields; i++) {
        Field *f = table->field[i];
        if (!bitmap_is_set(&metaMask, i) && !f->is_nullable()) {
          enum_field_types ft = f->real_type();
          if (ft == MYSQL_TYPE_BLOB || ft == MYSQL_TYPE_TINY_BLOB ||
              ft == MYSQL_TYPE_MEDIUM_BLOB || ft == MYSQL_TYPE_LONG_BLOB) {
            continue;
          }
          f->move_field_offset(row_offset);
          f->reset();
          f->move_field_offset(-row_offset);
          bitmap_set_bit(&metaMask, i);
        }
      }

      uchar *meta_mask = m_table_map->get_column_mask(&metaMask);

      NdbOperation::OperationOptions meta_opts;
      memset(&meta_opts, 0, sizeof(meta_opts));
      meta_opts.optionsPresent =
          NdbOperation::OperationOptions::OO_RING_BUFFER_OP;

      const NdbOperation *meta_op;
      if (meta_not_found) {
        meta_op =
            trans->insertTuple(key_rec, (const char *)meta_rec, m_ndb_record,
                               (char *)meta_rec, meta_mask, &meta_opts,
                               sizeof(NdbOperation::OperationOptions));
      } else {
        meta_op =
            trans->updateTuple(key_rec, (const char *)meta_rec, m_ndb_record,
                               (char *)meta_rec, meta_mask, &meta_opts,
                               sizeof(NdbOperation::OperationOptions));
      }

      if (!meta_op) {
        ret = ndb_err(trans);
        goto cleanup;
      }

      /* Execute data write + meta insert/update together */
      if (execute_no_commit(thd_ndb, trans, false) != 0) {
        ret = ndb_err(trans);
        goto cleanup;
      }
    }
  }

cleanup:
  /* Restore record state and write_set */
  memcpy(record + ring_idx_offset, saved_ring_idx,
         ring_idx_field->pack_length());
  bitmap_clear_bit(table->write_set, ring_idx_field->field_index());
  bitmap_clear_bit(table->write_set, ring_meta_field->field_index());
  bitmap_clear_bit(table->read_set, ring_idx_field->field_index());
  bitmap_clear_bit(table->read_set, ring_meta_field->field_index());
  return ret;
}
