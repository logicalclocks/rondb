/*
   Copyright (c) 2025, 2026, Hopsworks and/or its affiliates.

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
 * Comprehensive test for NdbRingBufferWriter.
 *
 * Usage:
 *   ndb_ndbapi_ring_buffer_test <mysql_socket> <connectstring>
 *
 * Creates ring buffer tables via MySQL, then exercises
 * NdbRingBufferWriter through the NDB API.
 */

#ifdef _WIN32
#include <winsock2.h>
#endif
#include <mysql.h>
#include <NdbApi.hpp>
// Internal header for NdbRecord buffer access in test code
#include "NdbRecord.hpp"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <atomic>

// ---------------------------------------------------------------
// Test infrastructure
// ---------------------------------------------------------------

static int g_tests_passed = 0;
static int g_tests_failed = 0;

#define TEST_ASSERT(cond, msg)                                                \
  do {                                                                        \
    if (!(cond)) {                                                            \
      std::cerr << "  FAIL: " << msg << " [" << __FILE__ << ":" << __LINE__   \
                << "]" << std::endl;                                          \
      g_tests_failed++;                                                       \
      return false;                                                           \
    }                                                                         \
  } while (0)

#define TEST_PASS(name)                           \
  do {                                            \
    std::cout << "  PASS: " << name << std::endl; \
    g_tests_passed++;                             \
  } while (0)

static void mysql_exec(MYSQL *mysql, const char *sql) {
  if (mysql_query(mysql, sql) != 0) {
    std::cerr << "SQL ERROR: " << mysql_error(mysql) << "\n  SQL: " << sql
              << std::endl;
    exit(EXIT_FAILURE);
  }
}

// ---------------------------------------------------------------
// NdbRecord buffer helpers - charset-safe varchar encoding
// ---------------------------------------------------------------

static const NdbRecord::Attr *findAttr(const NdbRecord *rec,
                                       const NdbDictionary::Table *table,
                                       const char *col_name) {
  const NdbDictionary::Column *col = table->getColumn(col_name);
  if (!col) return nullptr;
  Uint32 aid = col->getAttrId();
  if (aid < rec->m_attrId_indexes_length) {
    int idx = rec->m_attrId_indexes[aid];
    if (idx >= 0) return &rec->columns[idx];
  }
  return nullptr;
}

static void setInt32(char *buf, const NdbRecord::Attr *attr, Int32 val) {
  int4store(reinterpret_cast<unsigned char *>(buf + attr->offset), val);
}

static void setVarchar(char *buf, const NdbRecord::Attr *attr,
                       const char *str) {
  Uint32 len = strlen(str);
  unsigned char *p = reinterpret_cast<unsigned char *>(buf + attr->offset);
  if (attr->flags & NdbRecord::IsVar1ByteLen) {
    p[0] = (unsigned char)len;
    memcpy(p + 1, str, len);
  } else if (attr->flags & NdbRecord::IsVar2ByteLen) {
    int2store(p, len);
    memcpy(p + 2, str, len);
  } else {
    memcpy(p, str, len);
  }
}

static void setVarbinary(char *buf, const NdbRecord::Attr *attr,
                         const unsigned char *data, Uint32 len) {
  unsigned char *p = reinterpret_cast<unsigned char *>(buf + attr->offset);
  if (attr->flags & NdbRecord::IsVar1ByteLen) {
    p[0] = (unsigned char)len;
    memcpy(p + 1, data, len);
  } else if (attr->flags & NdbRecord::IsVar2ByteLen) {
    int2store(p, len);
    memcpy(p + 2, data, len);
  } else {
    memcpy(p, data, len);
  }
}

static void setNull(char *buf, const NdbRecord::Attr *attr) {
  if (attr->flags & NdbRecord::IsNullable) {
    buf[attr->nullbit_byte_offset] |= (1 << attr->nullbit_bit_in_byte);
  }
}

static void clearNull(char *buf, const NdbRecord::Attr *attr) {
  if (attr->flags & NdbRecord::IsNullable) {
    buf[attr->nullbit_byte_offset] &= ~(1 << attr->nullbit_bit_in_byte);
  }
}

/*
 * Build a user column mask for named columns.
 */
static void buildMask(const NdbDictionary::Table *table,
                      unsigned char *mask, Uint32 mask_size,
                      const char *const *col_names, int num_cols) {
  memset(mask, 0, mask_size);
  for (int i = 0; i < num_cols; i++) {
    const NdbDictionary::Column *c = table->getColumn(col_names[i]);
    if (!c) continue;
    Uint32 aid = c->getAttrId();
    mask[aid >> 3] |= (1 << (aid & 7));
  }
}

/*
 * Helper struct to hold record + attr pointers for the basic test schema.
 */
struct BasicRecordHelper {
  const NdbRecord *record;
  Uint32 row_size;
  const NdbRecord::Attr *cid_attr;
  const NdbRecord::Attr *data_attr;
  const NdbRecord::Attr *rmeta_attr;
  // TTL column of the TTL ring test tables (nullptr on the others). A
  // TTL ring insert must set the TTL column (4359): fillRow() sets it to
  // NULL (never expires) and newUserMask() includes it.
  const NdbRecord::Attr *ts_attr;
  Uint32 mask_size;

  bool init(const NdbDictionary::Table *table) {
    record = table->getDefaultRecord();
    if (!record) return false;
    row_size = record->m_row_size;
    cid_attr = findAttr(record, table, "client_id");
    data_attr = findAttr(record, table, "event_data");
    rmeta_attr = findAttr(record, table, "ring_meta");
    ts_attr = findAttr(record, table, "ts");
    Uint32 max_attr = 0;
    for (Uint32 i = 0; i < record->noOfColumns; i++)
      if (record->columns[i].attrId > max_attr)
        max_attr = record->columns[i].attrId;
    mask_size = (max_attr / 8) + 1;
    return cid_attr && data_attr && rmeta_attr;
  }

  char *newRow() const {
    char *buf = new char[row_size];
    memset(buf, 0, row_size);
    return buf;
  }

  void fillRow(char *buf, Int32 client_id, const char *data) const {
    memset(buf, 0, row_size);
    setInt32(buf, cid_attr, client_id);
    setNull(buf, rmeta_attr);
    clearNull(buf, data_attr);
    setVarchar(buf, data_attr, data);
    if (ts_attr) setNull(buf, ts_attr);
  }

  unsigned char *newUserMask(const NdbDictionary::Table *table) const {
    unsigned char *mask = new unsigned char[mask_size];
    const char *cols[] = {"client_id", "event_data", "ts"};
    buildMask(table, mask, mask_size, cols, ts_attr ? 3 : 2);
    return mask;
  }
};

/*
 * SQL helper: read data rows (ring_idx > 0).
 */
struct DataRow {
  int client_id;
  int ring_idx;
  std::string data;
};

static std::vector<DataRow> readDataRows(MYSQL *mysql, const char *tbl,
                                         int cid = -1) {
  std::vector<DataRow> rows;
  char sql[512];
  if (cid >= 0)
    snprintf(sql, sizeof(sql),
             "SELECT client_id, ring_idx, event_data FROM test.%s "
             "WHERE client_id=%d AND ring_idx>0 ORDER BY ring_idx",
             tbl, cid);
  else
    snprintf(sql, sizeof(sql),
             "SELECT client_id, ring_idx, event_data FROM test.%s "
             "WHERE ring_idx>0 ORDER BY client_id, ring_idx",
             tbl);
  mysql_exec(mysql, sql);
  MYSQL_RES *res = mysql_store_result(mysql);
  if (!res) return rows;
  MYSQL_ROW r;
  while ((r = mysql_fetch_row(res))) {
    DataRow dr;
    dr.client_id = atoi(r[0]);
    dr.ring_idx = atoi(r[1]);
    dr.data = r[2] ? r[2] : "";
    rows.push_back(dr);
  }
  mysql_free_result(res);
  return rows;
}

static int countAllRows(MYSQL *mysql, const char *tbl, int cid = -1) {
  mysql_exec(mysql, "SET ndb_ring_buffer_show_meta=1");
  char sql[256];
  if (cid >= 0)
    snprintf(sql, sizeof(sql),
             "SELECT COUNT(*) FROM test.%s WHERE client_id=%d", tbl, cid);
  else
    snprintf(sql, sizeof(sql), "SELECT COUNT(*) FROM test.%s", tbl);
  mysql_exec(mysql, sql);
  MYSQL_RES *res = mysql_store_result(mysql);
  MYSQL_ROW r = mysql_fetch_row(res);
  int count = atoi(r[0]);
  mysql_free_result(res);
  mysql_exec(mysql, "SET ndb_ring_buffer_show_meta=0");
  return count;
}

/*
 * SQL helper: read HEX(ring_meta) of the meta row for one PK prefix.
 * Returns empty string if the meta row is missing or ring_meta is NULL.
 */
static std::string readMetaHex(MYSQL *mysql, const char *tbl, int cid) {
  mysql_exec(mysql, "SET ndb_ring_buffer_show_meta=1");
  char sql[256];
  snprintf(sql, sizeof(sql),
           "SELECT HEX(ring_meta) FROM test.%s "
           "WHERE client_id=%d AND ring_idx=0",
           tbl, cid);
  mysql_exec(mysql, sql);
  MYSQL_RES *res = mysql_store_result(mysql);
  std::string hex;
  if (res) {
    MYSQL_ROW r = mysql_fetch_row(res);
    if (r && r[0]) hex = r[0];
    mysql_free_result(res);
  }
  mysql_exec(mysql, "SET ndb_ring_buffer_show_meta=0");
  return hex;
}

/*
 * Parse a little-endian unsigned field out of a HEX(ring_meta) string.
 * byte_off/nbytes follow the packed Ring_meta layout (version @0/2,
 * next_pos @4/4, count @8/4, total_inserts @16/8).
 */
static Uint64 metaFieldLE(const std::string &hex, int byte_off, int nbytes) {
  Uint64 val = 0;
  for (int j = 0; j < nbytes; j++) {
    int pos = (byte_off + j) * 2;
    if (pos + 1 >= (int)hex.size()) break;
    unsigned int byte = 0;
    sscanf(hex.c_str() + pos, "%2x", &byte);
    val |= ((Uint64)byte) << (8 * j);
  }
  return val;
}

static const char *CREATE_BASIC =
    "CREATE TABLE test.%s ("
    "  client_id INT NOT NULL,"
    "  ring_idx INT NOT NULL DEFAULT 0,"
    "  ring_meta VARBINARY(64),"
    "  event_data VARCHAR(100),"
    "  PRIMARY KEY (client_id, ring_idx)"
    ") ENGINE=NDB,"
    "  COMMENT='NDB_TABLE=MAX_ROWS_PER_PK=%d@ring_idx@ring_meta'";

// ---------------------------------------------------------------
// Test cases
// ---------------------------------------------------------------

/*
 * Test 1: Single row insert.
 */
static bool test_single_insert(Ndb *ndb, MYSQL *mysql) {
  std::cout << "[Test 1] Single row insert" << std::endl;

  mysql_exec(mysql, "DROP TABLE IF EXISTS test.rb_t1");
  char ddl[1024];
  snprintf(ddl, sizeof(ddl), CREATE_BASIC, "rb_t1", 5);
  mysql_exec(mysql, ddl);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("rb_t1");
  const NdbDictionary::Table *table = dict->getTable("rb_t1");
  TEST_ASSERT(table != nullptr, "getTable");
  TEST_ASSERT(table->isRingBuffer(), "should be ring buffer");
  TEST_ASSERT(table->getRingBufferSize() == 5, "ring_size=5");

  BasicRecordHelper h;
  TEST_ASSERT(h.init(table), "init record helper");

  char *rowbuf = h.newRow();
  h.fillRow(rowbuf, 42, "hello");

  unsigned char *mask = h.newUserMask(table);

  NdbTransaction *trans = ndb->startTransaction(table);
  TEST_ASSERT(trans != nullptr, "startTransaction");

  {
    NdbRingBufferWriter writer(table, h.record, trans);
    TEST_ASSERT(writer.getErrorCode() == 0,
                std::string("writer init: ") + writer.getErrorMessage());

    const NdbOperation *op = writer.addRow(rowbuf, mask);
    TEST_ASSERT(op != nullptr,
                std::string("addRow: ") + writer.getErrorMessage());

    TEST_ASSERT(writer.flush() == 0,
                std::string("flush: ") + writer.getErrorMessage());
  }

  TEST_ASSERT(trans->execute(NdbTransaction::Commit) == 0, "commit");
  ndb->closeTransaction(trans);

  delete[] rowbuf;
  delete[] mask;

  // Verify
  auto rows = readDataRows(mysql, "rb_t1", 42);
  TEST_ASSERT(rows.size() == 1, "expected 1 row");
  TEST_ASSERT(rows[0].ring_idx == 1, "ring_idx should be 1");
  TEST_ASSERT(rows[0].data == "hello", "data should be 'hello'");

  int total = countAllRows(mysql, "rb_t1", 42);
  TEST_ASSERT(total == 2, "expected 2 total (meta+data)");

  mysql_exec(mysql, "DROP TABLE test.rb_t1");
  TEST_PASS("Single row insert");
  return true;
}

/*
 * Test 2: Fill ring and verify wraparound.
 * 7 rows into ring_size=5 - only last 5 survive.
 */
static bool test_fill_ring(Ndb *ndb, MYSQL *mysql) {
  std::cout << "[Test 2] Fill ring + wraparound" << std::endl;

  mysql_exec(mysql, "DROP TABLE IF EXISTS test.rb_t2");
  char ddl[1024];
  snprintf(ddl, sizeof(ddl), CREATE_BASIC, "rb_t2", 5);
  mysql_exec(mysql, ddl);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("rb_t2");
  const NdbDictionary::Table *table = dict->getTable("rb_t2");
  TEST_ASSERT(table != nullptr, "getTable");

  BasicRecordHelper h;
  TEST_ASSERT(h.init(table), "init record helper");
  char *rowbuf = h.newRow();
  unsigned char *mask = h.newUserMask(table);

  NdbTransaction *trans = ndb->startTransaction(table);
  TEST_ASSERT(trans != nullptr, "startTransaction");

  {
    NdbRingBufferWriter writer(table, h.record, trans);
    TEST_ASSERT(writer.getErrorCode() == 0, "writer init");

    for (int i = 0; i < 7; i++) {
      char buf[32];
      snprintf(buf, sizeof(buf), "row_%d", i);
      h.fillRow(rowbuf, 1, buf);

      const NdbOperation *op = writer.addRow(rowbuf, mask);
      TEST_ASSERT(op != nullptr,
                  std::string("addRow ") + std::to_string(i));
    }
    TEST_ASSERT(writer.flush() == 0, "flush");
  }

  TEST_ASSERT(trans->execute(NdbTransaction::Commit) == 0, "commit");
  ndb->closeTransaction(trans);

  delete[] rowbuf;
  delete[] mask;

  auto rows = readDataRows(mysql, "rb_t2", 1);
  TEST_ASSERT(rows.size() == 5,
              "expected 5 data rows, got " + std::to_string(rows.size()));

  // row_0 and row_1 should be overwritten; row_2..row_6 should survive
  bool found_row_2 = false, found_row_6 = false;
  bool found_row_0 = false;
  for (const auto &r : rows) {
    if (r.data == "row_2") found_row_2 = true;
    if (r.data == "row_6") found_row_6 = true;
    if (r.data == "row_0") found_row_0 = true;
  }
  TEST_ASSERT(found_row_2, "row_2 should survive");
  TEST_ASSERT(found_row_6, "row_6 should survive");
  TEST_ASSERT(!found_row_0, "row_0 should be overwritten");

  int total = countAllRows(mysql, "rb_t2", 1);
  TEST_ASSERT(total == 6, "expected 6 total (meta+5 data)");

  mysql_exec(mysql, "DROP TABLE test.rb_t2");
  TEST_PASS("Fill ring + wraparound");
  return true;
}

/*
 * Test 3: Multiple PK prefixes with interleaved inserts.
 */
static bool test_multiple_prefixes(Ndb *ndb, MYSQL *mysql) {
  std::cout << "[Test 3] Multiple PK prefixes" << std::endl;

  mysql_exec(mysql, "DROP TABLE IF EXISTS test.rb_t3");
  char ddl[1024];
  snprintf(ddl, sizeof(ddl), CREATE_BASIC, "rb_t3", 3);
  mysql_exec(mysql, ddl);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("rb_t3");
  const NdbDictionary::Table *table = dict->getTable("rb_t3");
  TEST_ASSERT(table != nullptr, "getTable");

  BasicRecordHelper h;
  TEST_ASSERT(h.init(table), "init record helper");
  char *rowbuf = h.newRow();
  unsigned char *mask = h.newUserMask(table);

  // Inserts: cid=10 x2, cid=20 x2, cid=10 x2  (prefix changes twice)
  struct Ins {
    int cid;
    const char *data;
  };
  Ins inserts[] = {{10, "A1"}, {10, "A2"}, {20, "B1"},
                   {20, "B2"}, {10, "A3"}, {10, "A4"}};

  NdbTransaction *trans = ndb->startTransaction(table);
  TEST_ASSERT(trans != nullptr, "startTransaction");

  {
    NdbRingBufferWriter writer(table, h.record, trans);
    TEST_ASSERT(writer.getErrorCode() == 0, "writer init");

    for (const auto &ins : inserts) {
      h.fillRow(rowbuf, ins.cid, ins.data);
      const NdbOperation *op = writer.addRow(rowbuf, mask);
      TEST_ASSERT(op != nullptr,
                  std::string("addRow ") + ins.data + ": " +
                      writer.getErrorMessage());
    }
    TEST_ASSERT(writer.flush() == 0, "flush");
  }

  TEST_ASSERT(trans->execute(NdbTransaction::Commit) == 0, "commit");
  ndb->closeTransaction(trans);

  delete[] rowbuf;
  delete[] mask;

  // cid=10: 4 inserts into ring_size=3, 3 should survive
  auto rows_10 = readDataRows(mysql, "rb_t3", 10);
  TEST_ASSERT(rows_10.size() == 3,
              "cid=10: expected 3, got " + std::to_string(rows_10.size()));

  // cid=20: 2 inserts into ring_size=3, 2 should survive
  auto rows_20 = readDataRows(mysql, "rb_t3", 20);
  TEST_ASSERT(rows_20.size() == 2,
              "cid=20: expected 2, got " + std::to_string(rows_20.size()));

  mysql_exec(mysql, "DROP TABLE test.rb_t3");
  TEST_PASS("Multiple PK prefixes");
  return true;
}

/*
 * Test 4: Batch insert - 10 rows, same PK prefix, ring_size=10.
 */
static bool test_batch_insert(Ndb *ndb, MYSQL *mysql) {
  std::cout << "[Test 4] Batch insert (same PK prefix)" << std::endl;

  mysql_exec(mysql, "DROP TABLE IF EXISTS test.rb_t4");
  char ddl[1024];
  snprintf(ddl, sizeof(ddl), CREATE_BASIC, "rb_t4", 10);
  mysql_exec(mysql, ddl);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("rb_t4");
  const NdbDictionary::Table *table = dict->getTable("rb_t4");
  TEST_ASSERT(table != nullptr, "getTable");

  BasicRecordHelper h;
  TEST_ASSERT(h.init(table), "init record helper");
  char *rowbuf = h.newRow();
  unsigned char *mask = h.newUserMask(table);

  NdbTransaction *trans = ndb->startTransaction(table);
  TEST_ASSERT(trans != nullptr, "startTransaction");

  {
    NdbRingBufferWriter writer(table, h.record, trans);
    for (int i = 0; i < 10; i++) {
      char buf[32];
      snprintf(buf, sizeof(buf), "batch_%d", i);
      h.fillRow(rowbuf, 100, buf);
      TEST_ASSERT(writer.addRow(rowbuf, mask) != nullptr,
                  std::string("addRow ") + buf);
    }
    TEST_ASSERT(writer.flush() == 0, "flush");
  }

  TEST_ASSERT(trans->execute(NdbTransaction::Commit) == 0, "commit");
  ndb->closeTransaction(trans);

  delete[] rowbuf;
  delete[] mask;

  auto rows = readDataRows(mysql, "rb_t4", 100);
  TEST_ASSERT(rows.size() == 10,
              "expected 10, got " + std::to_string(rows.size()));

  for (int i = 0; i < 10; i++) {
    char expected[32];
    snprintf(expected, sizeof(expected), "batch_%d", i);
    bool found = false;
    for (const auto &r : rows) {
      if (r.data == expected) {
        found = true;
        break;
      }
    }
    TEST_ASSERT(found, std::string("missing ") + expected);
  }

  mysql_exec(mysql, "DROP TABLE test.rb_t4");
  TEST_PASS("Batch insert (same PK prefix)");
  return true;
}

/*
 * Test 5: NOT NULL user columns.
 */
static bool test_notnull_column(Ndb *ndb, MYSQL *mysql) {
  std::cout << "[Test 5] NOT NULL user column" << std::endl;

  mysql_exec(mysql, "DROP TABLE IF EXISTS test.rb_t5");
  mysql_exec(mysql,
             "CREATE TABLE test.rb_t5 ("
             "  client_id INT NOT NULL,"
             "  ring_idx INT NOT NULL DEFAULT 0,"
             "  ring_meta VARBINARY(64),"
             "  name VARCHAR(50) NOT NULL,"
             "  score INT NOT NULL,"
             "  PRIMARY KEY (client_id, ring_idx)"
             ") ENGINE=NDB,"
             "  COMMENT='NDB_TABLE=MAX_ROWS_PER_PK=3@ring_idx@ring_meta'");

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("rb_t5");
  const NdbDictionary::Table *table = dict->getTable("rb_t5");
  TEST_ASSERT(table != nullptr, "getTable");

  const NdbRecord *record = table->getDefaultRecord();
  TEST_ASSERT(record != nullptr, "getDefaultRecord");

  const NdbRecord::Attr *cid_attr = findAttr(record, table, "client_id");
  const NdbRecord::Attr *name_attr = findAttr(record, table, "name");
  const NdbRecord::Attr *score_attr = findAttr(record, table, "score");
  const NdbRecord::Attr *rmeta_attr = findAttr(record, table, "ring_meta");
  TEST_ASSERT(cid_attr && name_attr && score_attr && rmeta_attr, "find attrs");

  Uint32 row_size = record->m_row_size;
  char *rowbuf = new char[row_size];
  memset(rowbuf, 0, row_size);
  setInt32(rowbuf, cid_attr, 1);
  setNull(rowbuf, rmeta_attr);
  setVarchar(rowbuf, name_attr, "alice");
  int4store(reinterpret_cast<unsigned char *>(rowbuf + score_attr->offset), 99);

  // Build mask for user columns: client_id, name, score
  Uint32 max_attr = 0;
  for (Uint32 i = 0; i < record->noOfColumns; i++)
    if (record->columns[i].attrId > max_attr)
      max_attr = record->columns[i].attrId;
  Uint32 mask_size = (max_attr / 8) + 1;
  unsigned char *mask = new unsigned char[mask_size];
  const char *cols[] = {"client_id", "name", "score"};
  buildMask(table, mask, mask_size, cols, 3);

  NdbTransaction *trans = ndb->startTransaction(table);
  TEST_ASSERT(trans != nullptr, "startTransaction");

  {
    NdbRingBufferWriter writer(table, record, trans);
    TEST_ASSERT(writer.getErrorCode() == 0,
                std::string("writer init: ") + writer.getErrorMessage());
    TEST_ASSERT(writer.addRow(rowbuf, mask) != nullptr,
                std::string("addRow: ") + writer.getErrorMessage());
    TEST_ASSERT(writer.flush() == 0,
                std::string("flush: ") + writer.getErrorMessage());
  }

  TEST_ASSERT(trans->execute(NdbTransaction::Commit) == 0, "commit");
  ndb->closeTransaction(trans);

  delete[] rowbuf;
  delete[] mask;

  // Verify
  mysql_exec(mysql,
             "SELECT name, score FROM test.rb_t5 "
             "WHERE client_id=1 AND ring_idx>0");
  MYSQL_RES *res = mysql_store_result(mysql);
  MYSQL_ROW sqlrow = mysql_fetch_row(res);
  TEST_ASSERT(sqlrow != nullptr, "expected 1 row");
  TEST_ASSERT(std::string(sqlrow[0]) == "alice", "name=alice");
  TEST_ASSERT(std::string(sqlrow[1]) == "99", "score=99");
  mysql_free_result(res);

  mysql_exec(mysql, "DROP TABLE test.rb_t5");
  TEST_PASS("NOT NULL user column");
  return true;
}

/*
 * Test 6: ring_size=1 - every insert overwrites the single slot.
 */
static bool test_ring_size_1(Ndb *ndb, MYSQL *mysql) {
  std::cout << "[Test 6] Ring size = 1" << std::endl;

  mysql_exec(mysql, "DROP TABLE IF EXISTS test.rb_t6");
  char ddl[1024];
  snprintf(ddl, sizeof(ddl), CREATE_BASIC, "rb_t6", 1);
  mysql_exec(mysql, ddl);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("rb_t6");
  const NdbDictionary::Table *table = dict->getTable("rb_t6");
  TEST_ASSERT(table != nullptr, "getTable");

  BasicRecordHelper h;
  TEST_ASSERT(h.init(table), "init record helper");
  char *rowbuf = h.newRow();
  unsigned char *mask = h.newUserMask(table);

  NdbTransaction *trans = ndb->startTransaction(table);
  TEST_ASSERT(trans != nullptr, "startTransaction");

  {
    NdbRingBufferWriter writer(table, h.record, trans);
    const char *vals[] = {"first", "second", "third"};
    for (int i = 0; i < 3; i++) {
      h.fillRow(rowbuf, 1, vals[i]);
      TEST_ASSERT(writer.addRow(rowbuf, mask) != nullptr,
                  std::string("addRow ") + vals[i]);
    }
    TEST_ASSERT(writer.flush() == 0, "flush");
  }

  TEST_ASSERT(trans->execute(NdbTransaction::Commit) == 0, "commit");
  ndb->closeTransaction(trans);

  delete[] rowbuf;
  delete[] mask;

  auto rows = readDataRows(mysql, "rb_t6", 1);
  TEST_ASSERT(rows.size() == 1,
              "expected 1 row, got " + std::to_string(rows.size()));
  TEST_ASSERT(rows[0].data == "third",
              "expected 'third', got '" + rows[0].data + "'");

  mysql_exec(mysql, "DROP TABLE test.rb_t6");
  TEST_PASS("Ring size = 1");
  return true;
}

/*
 * Test 7: Non-ring-buffer table - writer should reject.
 */
static bool test_error_non_ring_table(Ndb *ndb, MYSQL *mysql) {
  std::cout << "[Test 7] Error: non-ring-buffer table" << std::endl;

  mysql_exec(mysql, "DROP TABLE IF EXISTS test.rb_t7");
  mysql_exec(mysql,
             "CREATE TABLE test.rb_t7 ("
             "  id INT NOT NULL PRIMARY KEY,"
             "  data VARCHAR(100)"
             ") ENGINE=NDB");

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("rb_t7");
  const NdbDictionary::Table *table = dict->getTable("rb_t7");
  TEST_ASSERT(table != nullptr, "getTable");
  TEST_ASSERT(!table->isRingBuffer(), "should NOT be ring buffer");

  const NdbRecord *record = table->getDefaultRecord();

  NdbTransaction *trans = ndb->startTransaction(table);
  TEST_ASSERT(trans != nullptr, "startTransaction");

  {
    NdbRingBufferWriter writer(table, record, trans);
    TEST_ASSERT(writer.getErrorCode() != 0,
                "should fail for non-ring table");
  }

  ndb->closeTransaction(trans);
  mysql_exec(mysql, "DROP TABLE test.rb_t7");
  TEST_PASS("Error: non-ring-buffer table");
  return true;
}

/*
 * Test 8: Multiple transactions - verify meta row carries state.
 */
static bool test_multi_transaction(Ndb *ndb, MYSQL *mysql) {
  std::cout << "[Test 8] Multiple transactions" << std::endl;

  mysql_exec(mysql, "DROP TABLE IF EXISTS test.rb_t8");
  char ddl[1024];
  snprintf(ddl, sizeof(ddl), CREATE_BASIC, "rb_t8", 5);
  mysql_exec(mysql, ddl);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("rb_t8");
  const NdbDictionary::Table *table = dict->getTable("rb_t8");
  TEST_ASSERT(table != nullptr, "getTable");

  BasicRecordHelper h;
  TEST_ASSERT(h.init(table), "init record helper");
  char *rowbuf = h.newRow();
  unsigned char *mask = h.newUserMask(table);

  // Transaction 1: insert 2 rows
  {
    NdbTransaction *trans = ndb->startTransaction(table);
    TEST_ASSERT(trans != nullptr, "startTransaction 1");

    NdbRingBufferWriter writer(table, h.record, trans);
    for (int i = 0; i < 2; i++) {
      char buf[32];
      snprintf(buf, sizeof(buf), "tx1_%d", i);
      h.fillRow(rowbuf, 1, buf);
      TEST_ASSERT(writer.addRow(rowbuf, mask) != nullptr, "addRow tx1");
    }
    TEST_ASSERT(writer.flush() == 0, "flush tx1");
    TEST_ASSERT(trans->execute(NdbTransaction::Commit) == 0, "commit tx1");
    ndb->closeTransaction(trans);
  }

  auto rows1 = readDataRows(mysql, "rb_t8", 1);
  TEST_ASSERT(rows1.size() == 2,
              "after tx1: expected 2, got " + std::to_string(rows1.size()));

  // Transaction 2: insert 2 more rows (should go to slots 3, 4)
  {
    NdbTransaction *trans = ndb->startTransaction(table);
    TEST_ASSERT(trans != nullptr, "startTransaction 2");

    NdbRingBufferWriter writer(table, h.record, trans);
    for (int i = 0; i < 2; i++) {
      char buf[32];
      snprintf(buf, sizeof(buf), "tx2_%d", i);
      h.fillRow(rowbuf, 1, buf);
      TEST_ASSERT(writer.addRow(rowbuf, mask) != nullptr, "addRow tx2");
    }
    TEST_ASSERT(writer.flush() == 0, "flush tx2");
    TEST_ASSERT(trans->execute(NdbTransaction::Commit) == 0, "commit tx2");
    ndb->closeTransaction(trans);
  }

  auto rows2 = readDataRows(mysql, "rb_t8", 1);
  TEST_ASSERT(rows2.size() == 4,
              "after tx2: expected 4, got " + std::to_string(rows2.size()));

  // Transaction 3: insert 3 more to trigger wrap (total 7 into ring_size=5)
  {
    NdbTransaction *trans = ndb->startTransaction(table);
    TEST_ASSERT(trans != nullptr, "startTransaction 3");

    NdbRingBufferWriter writer(table, h.record, trans);
    for (int i = 0; i < 3; i++) {
      char buf[32];
      snprintf(buf, sizeof(buf), "tx3_%d", i);
      h.fillRow(rowbuf, 1, buf);
      TEST_ASSERT(writer.addRow(rowbuf, mask) != nullptr, "addRow tx3");
    }
    TEST_ASSERT(writer.flush() == 0, "flush tx3");
    TEST_ASSERT(trans->execute(NdbTransaction::Commit) == 0, "commit tx3");
    ndb->closeTransaction(trans);
  }

  auto rows3 = readDataRows(mysql, "rb_t8", 1);
  TEST_ASSERT(rows3.size() == 5,
              "after tx3: expected 5, got " + std::to_string(rows3.size()));

  // Verify that tx1 data was overwritten and tx3 data survives
  bool found_tx3_2 = false;
  bool found_tx1_0 = false;
  for (const auto &r : rows3) {
    if (r.data == "tx3_2") found_tx3_2 = true;
    if (r.data == "tx1_0") found_tx1_0 = true;
  }
  TEST_ASSERT(found_tx3_2, "tx3_2 should survive");
  TEST_ASSERT(!found_tx1_0, "tx1_0 should be overwritten");

  delete[] rowbuf;
  delete[] mask;

  mysql_exec(mysql, "DROP TABLE test.rb_t8");
  TEST_PASS("Multiple transactions");
  return true;
}

/*
 * Test 9: BLOB/TEXT columns - insert via NdbRingBufferWriter + NdbBlob.
 * Verifies that large TEXT data survives ring wrapping.
 */
static bool test_blob_text(Ndb *ndb, MYSQL *mysql) {
  std::cout << "[Test 9] BLOB/TEXT columns" << std::endl;

  mysql_exec(mysql, "DROP TABLE IF EXISTS test.rb_t9");
  mysql_exec(mysql,
             "CREATE TABLE test.rb_t9 ("
             "  client_id INT NOT NULL,"
             "  ring_idx INT NOT NULL DEFAULT 0,"
             "  ring_meta VARBINARY(64),"
             "  content TEXT,"
             "  PRIMARY KEY (client_id, ring_idx)"
             ") ENGINE=NDB,"
             "  COMMENT='NDB_TABLE=MAX_ROWS_PER_PK=3@ring_idx@ring_meta'");

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("rb_t9");
  const NdbDictionary::Table *table = dict->getTable("rb_t9");
  TEST_ASSERT(table != nullptr, "getTable");
  TEST_ASSERT(table->isRingBuffer(), "should be ring buffer");

  const NdbRecord *record = table->getDefaultRecord();
  TEST_ASSERT(record != nullptr, "getDefaultRecord");

  const NdbRecord::Attr *cid_attr = findAttr(record, table, "client_id");
  const NdbRecord::Attr *rmeta_attr = findAttr(record, table, "ring_meta");
  TEST_ASSERT(cid_attr && rmeta_attr, "find attrs");

  Uint32 row_size = record->m_row_size;
  char *rowbuf = new char[row_size];

  // Build mask for user columns: client_id, content
  Uint32 max_attr = 0;
  for (Uint32 i = 0; i < record->noOfColumns; i++)
    if (record->columns[i].attrId > max_attr)
      max_attr = record->columns[i].attrId;
  Uint32 mask_size = (max_attr / 8) + 1;
  unsigned char *mask = new unsigned char[mask_size];
  const char *cols[] = {"client_id", "content"};
  buildMask(table, mask, mask_size, cols, 2);

  // Insert 5 rows into ring_size=3 - only last 3 should survive
  const char *texts[] = {"alpha_text", "bravo_text", "charlie_text",
                         "delta_text", "echo_text"};

  NdbTransaction *trans = ndb->startTransaction(table);
  TEST_ASSERT(trans != nullptr, "startTransaction");

  {
    NdbRingBufferWriter writer(table, record, trans);
    TEST_ASSERT(writer.getErrorCode() == 0,
                std::string("writer init: ") + writer.getErrorMessage());

    for (int i = 0; i < 5; i++) {
      memset(rowbuf, 0, row_size);
      setInt32(rowbuf, cid_attr, 1);
      setNull(rowbuf, rmeta_attr);

      const NdbOperation *op = writer.addRow(rowbuf, mask);
      TEST_ASSERT(op != nullptr,
                  std::string("addRow ") + std::to_string(i) + ": " +
                      writer.getErrorMessage());

      // Set TEXT via blob handle
      NdbBlob *blob = op->getBlobHandle("content");
      TEST_ASSERT(blob != nullptr, "getBlobHandle");
      TEST_ASSERT(blob->setValue(texts[i], strlen(texts[i])) == 0,
                  "blob setValue");
    }
    TEST_ASSERT(writer.flush() == 0,
                std::string("flush: ") + writer.getErrorMessage());
  }

  TEST_ASSERT(trans->execute(NdbTransaction::Commit) == 0, "commit");
  ndb->closeTransaction(trans);

  delete[] rowbuf;
  delete[] mask;

  // Verify: only charlie, delta, echo should survive
  mysql_exec(mysql,
             "SELECT content FROM test.rb_t9 "
             "WHERE client_id=1 AND ring_idx>0 ORDER BY ring_idx");
  MYSQL_RES *res = mysql_store_result(mysql);
  int nrows = (int)mysql_num_rows(res);
  TEST_ASSERT(nrows == 3, "expected 3 rows, got " + std::to_string(nrows));

  bool found_charlie = false, found_delta = false, found_echo = false;
  bool found_alpha = false;
  MYSQL_ROW r;
  while ((r = mysql_fetch_row(res))) {
    std::string val = r[0] ? r[0] : "";
    if (val == "charlie_text") found_charlie = true;
    if (val == "delta_text") found_delta = true;
    if (val == "echo_text") found_echo = true;
    if (val == "alpha_text") found_alpha = true;
  }
  mysql_free_result(res);

  TEST_ASSERT(found_charlie, "charlie_text should survive");
  TEST_ASSERT(found_delta, "delta_text should survive");
  TEST_ASSERT(found_echo, "echo_text should survive");
  TEST_ASSERT(!found_alpha, "alpha_text should be overwritten");

  mysql_exec(mysql, "DROP TABLE test.rb_t9");
  TEST_PASS("BLOB/TEXT columns");
  return true;
}

/*
 * Test 10: Delete + re-INSERT cycle.
 * Delete all rows for a PK prefix via SQL, then insert fresh via NDB API.
 * Verifies meta row is cleaned up and a fresh ring starts.
 */
static bool test_delete_reinsert(Ndb *ndb, MYSQL *mysql) {
  std::cout << "[Test 10] Delete + re-INSERT cycle" << std::endl;

  mysql_exec(mysql, "DROP TABLE IF EXISTS test.rb_t10");
  char ddl[1024];
  snprintf(ddl, sizeof(ddl), CREATE_BASIC, "rb_t10", 5);
  mysql_exec(mysql, ddl);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("rb_t10");
  const NdbDictionary::Table *table = dict->getTable("rb_t10");
  TEST_ASSERT(table != nullptr, "getTable");

  BasicRecordHelper h;
  TEST_ASSERT(h.init(table), "init record helper");
  char *rowbuf = h.newRow();
  unsigned char *mask = h.newUserMask(table);

  // Phase 1: Insert 4 rows via NdbRingBufferWriter
  {
    NdbTransaction *trans = ndb->startTransaction(table);
    TEST_ASSERT(trans != nullptr, "startTransaction phase1");
    NdbRingBufferWriter writer(table, h.record, trans);
    TEST_ASSERT(writer.getErrorCode() == 0, "writer init phase1");
    for (int i = 0; i < 4; i++) {
      char buf[32];
      snprintf(buf, sizeof(buf), "orig_%d", i);
      h.fillRow(rowbuf, 1, buf);
      TEST_ASSERT(writer.addRow(rowbuf, mask) != nullptr, "addRow phase1");
    }
    TEST_ASSERT(writer.flush() == 0, "flush phase1");
    TEST_ASSERT(trans->execute(NdbTransaction::Commit) == 0, "commit phase1");
    ndb->closeTransaction(trans);
  }

  auto rows1 = readDataRows(mysql, "rb_t10", 1);
  TEST_ASSERT(rows1.size() == 4,
              "phase1: expected 4, got " + std::to_string(rows1.size()));

  // Phase 2: Delete all rows for client_id=1 via SQL
  mysql_exec(mysql, "DELETE FROM test.rb_t10 WHERE client_id=1");

  auto rows2 = readDataRows(mysql, "rb_t10", 1);
  TEST_ASSERT(rows2.size() == 0,
              "after delete: expected 0, got " + std::to_string(rows2.size()));

  // Verify meta row is also gone
  int total_after_delete = countAllRows(mysql, "rb_t10", 1);
  TEST_ASSERT(total_after_delete == 0,
              "after delete: expected 0 total, got " +
                  std::to_string(total_after_delete));

  // Phase 3: Re-insert 2 rows - should start fresh ring
  {
    NdbTransaction *trans = ndb->startTransaction(table);
    TEST_ASSERT(trans != nullptr, "startTransaction phase3");
    NdbRingBufferWriter writer(table, h.record, trans);
    TEST_ASSERT(writer.getErrorCode() == 0, "writer init phase3");
    for (int i = 0; i < 2; i++) {
      char buf[32];
      snprintf(buf, sizeof(buf), "fresh_%d", i);
      h.fillRow(rowbuf, 1, buf);
      TEST_ASSERT(writer.addRow(rowbuf, mask) != nullptr, "addRow phase3");
    }
    TEST_ASSERT(writer.flush() == 0, "flush phase3");
    TEST_ASSERT(trans->execute(NdbTransaction::Commit) == 0, "commit phase3");
    ndb->closeTransaction(trans);
  }

  auto rows3 = readDataRows(mysql, "rb_t10", 1);
  TEST_ASSERT(rows3.size() == 2,
              "phase3: expected 2, got " + std::to_string(rows3.size()));

  // Fresh ring should start at ring_idx=1 again
  TEST_ASSERT(rows3[0].ring_idx == 1, "first fresh row at ring_idx=1");
  TEST_ASSERT(rows3[1].ring_idx == 2, "second fresh row at ring_idx=2");
  TEST_ASSERT(rows3[0].data == "fresh_0", "data should be fresh_0");
  TEST_ASSERT(rows3[1].data == "fresh_1", "data should be fresh_1");

  // Total should be 3 (meta + 2 data)
  int total = countAllRows(mysql, "rb_t10", 1);
  TEST_ASSERT(total == 3,
              "phase3: expected 3 total, got " + std::to_string(total));

  delete[] rowbuf;
  delete[] mask;

  mysql_exec(mysql, "DROP TABLE test.rb_t10");
  TEST_PASS("Delete + re-INSERT cycle");
  return true;
}

/*
 * Test 11: Rollback - verify meta row is reverted.
 * Insert 2 rows + commit, then insert 3 more + rollback.
 * Verify only first 2 rows survive.
 * Then insert 1 more + commit - should continue from ring_idx=3.
 */
static bool test_rollback(Ndb *ndb, MYSQL *mysql) {
  std::cout << "[Test 11] Rollback" << std::endl;

  mysql_exec(mysql, "DROP TABLE IF EXISTS test.rb_t11");
  char ddl[1024];
  snprintf(ddl, sizeof(ddl), CREATE_BASIC, "rb_t11", 5);
  mysql_exec(mysql, ddl);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("rb_t11");
  const NdbDictionary::Table *table = dict->getTable("rb_t11");
  TEST_ASSERT(table != nullptr, "getTable");

  BasicRecordHelper h;
  TEST_ASSERT(h.init(table), "init record helper");
  char *rowbuf = h.newRow();
  unsigned char *mask = h.newUserMask(table);

  // Transaction 1: Insert 2 rows, commit
  {
    NdbTransaction *trans = ndb->startTransaction(table);
    TEST_ASSERT(trans != nullptr, "startTransaction tx1");
    NdbRingBufferWriter writer(table, h.record, trans);
    for (int i = 0; i < 2; i++) {
      char buf[32];
      snprintf(buf, sizeof(buf), "committed_%d", i);
      h.fillRow(rowbuf, 1, buf);
      TEST_ASSERT(writer.addRow(rowbuf, mask) != nullptr, "addRow tx1");
    }
    TEST_ASSERT(writer.flush() == 0, "flush tx1");
    TEST_ASSERT(trans->execute(NdbTransaction::Commit) == 0, "commit tx1");
    ndb->closeTransaction(trans);
  }

  auto rows1 = readDataRows(mysql, "rb_t11", 1);
  TEST_ASSERT(rows1.size() == 2,
              "after tx1: expected 2, got " + std::to_string(rows1.size()));

  // Transaction 2: Insert 3 rows, ROLLBACK
  {
    NdbTransaction *trans = ndb->startTransaction(table);
    TEST_ASSERT(trans != nullptr, "startTransaction tx2");
    NdbRingBufferWriter writer(table, h.record, trans);
    for (int i = 0; i < 3; i++) {
      char buf[32];
      snprintf(buf, sizeof(buf), "rolled_back_%d", i);
      h.fillRow(rowbuf, 1, buf);
      TEST_ASSERT(writer.addRow(rowbuf, mask) != nullptr, "addRow tx2");
    }
    TEST_ASSERT(writer.flush() == 0, "flush tx2");
    // Rollback instead of commit
    trans->execute(NdbTransaction::Rollback);
    ndb->closeTransaction(trans);
  }

  // Verify: only the 2 committed rows should exist
  auto rows2 = readDataRows(mysql, "rb_t11", 1);
  TEST_ASSERT(rows2.size() == 2,
              "after rollback: expected 2, got " +
                  std::to_string(rows2.size()));

  bool found_rolled_back = false;
  for (const auto &r : rows2) {
    if (r.data.find("rolled_back") != std::string::npos)
      found_rolled_back = true;
  }
  TEST_ASSERT(!found_rolled_back, "rolled-back data should not exist");

  // Transaction 3: Insert 1 more row, commit - should go to ring_idx=3
  {
    NdbTransaction *trans = ndb->startTransaction(table);
    TEST_ASSERT(trans != nullptr, "startTransaction tx3");
    NdbRingBufferWriter writer(table, h.record, trans);
    h.fillRow(rowbuf, 1, "after_rollback");
    TEST_ASSERT(writer.addRow(rowbuf, mask) != nullptr, "addRow tx3");
    TEST_ASSERT(writer.flush() == 0, "flush tx3");
    TEST_ASSERT(trans->execute(NdbTransaction::Commit) == 0, "commit tx3");
    ndb->closeTransaction(trans);
  }

  auto rows3 = readDataRows(mysql, "rb_t11", 1);
  TEST_ASSERT(rows3.size() == 3,
              "after tx3: expected 3, got " + std::to_string(rows3.size()));

  // The new row should be at ring_idx=3 (continuing from the committed state)
  bool found_after = false;
  for (const auto &r : rows3) {
    if (r.data == "after_rollback") {
      TEST_ASSERT(r.ring_idx == 3,
                  "after_rollback should be at ring_idx=3, got " +
                      std::to_string(r.ring_idx));
      found_after = true;
    }
  }
  TEST_ASSERT(found_after, "after_rollback row should exist");

  delete[] rowbuf;
  delete[] mask;

  mysql_exec(mysql, "DROP TABLE test.rb_t11");
  TEST_PASS("Rollback");
  return true;
}

/*
 * Test 12: Multiple wraps in a single batch.
 * 10 rows into ring_size=3 in one transaction - tests aggressive wrapping.
 * Only last 3 rows should survive.
 */
static bool test_multi_wrap_batch(Ndb *ndb, MYSQL *mysql) {
  std::cout << "[Test 12] Multiple wraps in single batch" << std::endl;

  mysql_exec(mysql, "DROP TABLE IF EXISTS test.rb_t12");
  char ddl[1024];
  snprintf(ddl, sizeof(ddl), CREATE_BASIC, "rb_t12", 3);
  mysql_exec(mysql, ddl);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("rb_t12");
  const NdbDictionary::Table *table = dict->getTable("rb_t12");
  TEST_ASSERT(table != nullptr, "getTable");

  BasicRecordHelper h;
  TEST_ASSERT(h.init(table), "init record helper");
  char *rowbuf = h.newRow();
  unsigned char *mask = h.newUserMask(table);

  NdbTransaction *trans = ndb->startTransaction(table);
  TEST_ASSERT(trans != nullptr, "startTransaction");

  {
    NdbRingBufferWriter writer(table, h.record, trans);
    TEST_ASSERT(writer.getErrorCode() == 0, "writer init");

    for (int i = 0; i < 10; i++) {
      char buf[32];
      snprintf(buf, sizeof(buf), "wrap_%d", i);
      h.fillRow(rowbuf, 1, buf);
      TEST_ASSERT(writer.addRow(rowbuf, mask) != nullptr,
                  std::string("addRow ") + buf);
    }
    TEST_ASSERT(writer.flush() == 0, "flush");
  }

  TEST_ASSERT(trans->execute(NdbTransaction::Commit) == 0, "commit");
  ndb->closeTransaction(trans);

  delete[] rowbuf;
  delete[] mask;

  auto rows = readDataRows(mysql, "rb_t12", 1);
  TEST_ASSERT(rows.size() == 3,
              "expected 3, got " + std::to_string(rows.size()));

  // 10 inserts into size 3: wraps at 3, 6, 9.
  // Slots: wrap_7->idx1, wrap_8->idx2, wrap_9->idx3
  bool found_7 = false, found_8 = false, found_9 = false;
  bool found_0 = false;
  for (const auto &r : rows) {
    if (r.data == "wrap_7") found_7 = true;
    if (r.data == "wrap_8") found_8 = true;
    if (r.data == "wrap_9") found_9 = true;
    if (r.data == "wrap_0") found_0 = true;
  }
  TEST_ASSERT(found_7, "wrap_7 should survive");
  TEST_ASSERT(found_8, "wrap_8 should survive");
  TEST_ASSERT(found_9, "wrap_9 should survive");
  TEST_ASSERT(!found_0, "wrap_0 should be overwritten");

  int total = countAllRows(mysql, "rb_t12", 1);
  TEST_ASSERT(total == 4, "expected 4 total (meta+3 data)");

  mysql_exec(mysql, "DROP TABLE test.rb_t12");
  TEST_PASS("Multiple wraps in single batch");
  return true;
}

/*
 * Test 13: Multi-column PK prefix.
 * Table: (region INT, client_id INT, ring_idx INT, ...)
 * Verifies that prefix change detection works with >1 prefix column
 * and each (region, client_id) combo has an independent ring.
 */
static bool test_multi_col_pk_prefix(Ndb *ndb, MYSQL *mysql) {
  std::cout << "[Test 13] Multi-column PK prefix" << std::endl;

  mysql_exec(mysql, "DROP TABLE IF EXISTS test.rb_t13");
  mysql_exec(mysql,
             "CREATE TABLE test.rb_t13 ("
             "  region INT NOT NULL,"
             "  client_id INT NOT NULL,"
             "  ring_idx INT NOT NULL DEFAULT 0,"
             "  ring_meta VARBINARY(64),"
             "  event_data VARCHAR(100),"
             "  PRIMARY KEY (region, client_id, ring_idx)"
             ") ENGINE=NDB,"
             "  COMMENT='NDB_TABLE=MAX_ROWS_PER_PK=3@ring_idx@ring_meta'");

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("rb_t13");
  const NdbDictionary::Table *table = dict->getTable("rb_t13");
  TEST_ASSERT(table != nullptr, "getTable");
  TEST_ASSERT(table->isRingBuffer(), "should be ring buffer");
  TEST_ASSERT(table->getRingBufferSize() == 3, "ring_size=3");

  const NdbRecord *record = table->getDefaultRecord();
  TEST_ASSERT(record != nullptr, "getDefaultRecord");

  const NdbRecord::Attr *region_attr = findAttr(record, table, "region");
  const NdbRecord::Attr *cid_attr = findAttr(record, table, "client_id");
  const NdbRecord::Attr *data_attr = findAttr(record, table, "event_data");
  const NdbRecord::Attr *rmeta_attr = findAttr(record, table, "ring_meta");
  TEST_ASSERT(region_attr && cid_attr && data_attr && rmeta_attr, "find attrs");

  Uint32 row_size = record->m_row_size;
  char *rowbuf = new char[row_size];

  Uint32 max_attr = 0;
  for (Uint32 i = 0; i < record->noOfColumns; i++)
    if (record->columns[i].attrId > max_attr)
      max_attr = record->columns[i].attrId;
  Uint32 mask_size = (max_attr / 8) + 1;
  unsigned char *mask = new unsigned char[mask_size];
  const char *cols[] = {"region", "client_id", "event_data"};
  buildMask(table, mask, mask_size, cols, 3);

  // Insert interleaved rows for different (region, client_id) combos.
  // (1,100) x4, (1,200) x2, (2,100) x2
  // (1,100) should wrap (4 > ring_size=3), others should not.
  struct Ins {
    int region;
    int cid;
    const char *data;
  };
  Ins inserts[] = {
      {1, 100, "r1c100_A"}, {1, 100, "r1c100_B"}, {1, 100, "r1c100_C"},
      {1, 100, "r1c100_D"},  // 4th -> wraps, overwrites A
      {1, 200, "r1c200_A"}, {1, 200, "r1c200_B"},
      {2, 100, "r2c100_A"}, {2, 100, "r2c100_B"},
  };

  NdbTransaction *trans = ndb->startTransaction(table);
  TEST_ASSERT(trans != nullptr, "startTransaction");

  {
    NdbRingBufferWriter writer(table, record, trans);
    TEST_ASSERT(writer.getErrorCode() == 0, "writer init");

    for (const auto &ins : inserts) {
      memset(rowbuf, 0, row_size);
      setInt32(rowbuf, region_attr, ins.region);
      setInt32(rowbuf, cid_attr, ins.cid);
      setNull(rowbuf, rmeta_attr);
      clearNull(rowbuf, data_attr);
      setVarchar(rowbuf, data_attr, ins.data);

      const NdbOperation *op = writer.addRow(rowbuf, mask);
      TEST_ASSERT(op != nullptr,
                  std::string("addRow ") + ins.data + ": " +
                      writer.getErrorMessage());
    }
    TEST_ASSERT(writer.flush() == 0,
                std::string("flush: ") + writer.getErrorMessage());
  }

  TEST_ASSERT(trans->execute(NdbTransaction::Commit) == 0, "commit");
  ndb->closeTransaction(trans);

  delete[] rowbuf;
  delete[] mask;

  // Verify (1,100): 4 inserts into size 3 -> 3 survive, A overwritten
  mysql_exec(mysql,
             "SELECT event_data FROM test.rb_t13 "
             "WHERE region=1 AND client_id=100 AND ring_idx>0 "
             "ORDER BY ring_idx");
  MYSQL_RES *res = mysql_store_result(mysql);
  TEST_ASSERT(mysql_num_rows(res) == 3, "(1,100): expected 3 rows");
  bool found_A = false, found_D = false;
  MYSQL_ROW r;
  while ((r = mysql_fetch_row(res))) {
    std::string val = r[0] ? r[0] : "";
    if (val == "r1c100_A") found_A = true;
    if (val == "r1c100_D") found_D = true;
  }
  mysql_free_result(res);
  TEST_ASSERT(!found_A, "(1,100): A should be overwritten");
  TEST_ASSERT(found_D, "(1,100): D should survive");

  // Verify (1,200): 2 inserts -> 2 rows
  mysql_exec(mysql,
             "SELECT event_data FROM test.rb_t13 "
             "WHERE region=1 AND client_id=200 AND ring_idx>0 "
             "ORDER BY ring_idx");
  res = mysql_store_result(mysql);
  TEST_ASSERT(mysql_num_rows(res) == 2, "(1,200): expected 2 rows");
  mysql_free_result(res);

  // Verify (2,100): 2 inserts -> 2 rows
  mysql_exec(mysql,
             "SELECT event_data FROM test.rb_t13 "
             "WHERE region=2 AND client_id=100 AND ring_idx>0 "
             "ORDER BY ring_idx");
  res = mysql_store_result(mysql);
  TEST_ASSERT(mysql_num_rows(res) == 2, "(2,100): expected 2 rows");
  mysql_free_result(res);

  mysql_exec(mysql, "DROP TABLE test.rb_t13");
  TEST_PASS("Multi-column PK prefix");
  return true;
}

/*
 * Helper struct for unique-index test schema:
 *   (id INT, ring_idx INT, ring_meta VARBINARY(64),
 *    code VARCHAR(20), val INT, PK(id,ring_idx), UNIQUE(code))
 */
struct UniqueRecordHelper {
  const NdbRecord *record;
  Uint32 row_size;
  const NdbRecord::Attr *id_attr;
  const NdbRecord::Attr *code_attr;
  const NdbRecord::Attr *val_attr;
  const NdbRecord::Attr *rmeta_attr;
  Uint32 mask_size;

  bool init(const NdbDictionary::Table *table) {
    record = table->getDefaultRecord();
    if (!record) return false;
    row_size = record->m_row_size;
    id_attr = findAttr(record, table, "id");
    code_attr = findAttr(record, table, "code");
    val_attr = findAttr(record, table, "val");
    rmeta_attr = findAttr(record, table, "ring_meta");
    Uint32 max_attr = 0;
    for (Uint32 i = 0; i < record->noOfColumns; i++)
      if (record->columns[i].attrId > max_attr)
        max_attr = record->columns[i].attrId;
    mask_size = (max_attr / 8) + 1;
    return id_attr && code_attr && val_attr && rmeta_attr;
  }

  char *newRow() const {
    char *buf = new char[row_size];
    memset(buf, 0, row_size);
    return buf;
  }

  void fillRow(char *buf, Int32 id, const char *code, Int32 val) const {
    memset(buf, 0, row_size);
    setInt32(buf, id_attr, id);
    setNull(buf, rmeta_attr);
    clearNull(buf, code_attr);
    setVarchar(buf, code_attr, code);
    clearNull(buf, val_attr);
    int4store(reinterpret_cast<unsigned char *>(buf + val_attr->offset), val);
  }

  unsigned char *newUserMask(const NdbDictionary::Table *table) const {
    unsigned char *mask = new unsigned char[mask_size];
    const char *cols[] = {"id", "code", "val"};
    buildMask(table, mask, mask_size, cols, 3);
    return mask;
  }
};

static const char *CREATE_UNIQUE =
    "CREATE TABLE test.%s ("
    "  id INT NOT NULL,"
    "  ring_idx INT NOT NULL DEFAULT 0,"
    "  ring_meta VARBINARY(64),"
    "  code VARCHAR(20),"
    "  val INT,"
    "  PRIMARY KEY (id, ring_idx),"
    "  UNIQUE INDEX idx_code (code)"
    ") ENGINE=NDB,"
    "  COMMENT='NDB_TABLE=MAX_ROWS_PER_PK=%d@ring_idx@ring_meta'";

/*
 * SQL helper: read data rows from unique-index test tables.
 */
struct UniqueDataRow {
  int id;
  int ring_idx;
  std::string code;
  int val;
};

static std::vector<UniqueDataRow> readUniqueRows(MYSQL *mysql, const char *tbl,
                                                  int id = -1) {
  std::vector<UniqueDataRow> rows;
  char sql[512];
  if (id >= 0)
    snprintf(sql, sizeof(sql),
             "SELECT id, ring_idx, code, val FROM test.%s "
             "WHERE id=%d ORDER BY ring_idx",
             tbl, id);
  else
    snprintf(sql, sizeof(sql),
             "SELECT id, ring_idx, code, val FROM test.%s "
             "ORDER BY id, ring_idx",
             tbl);
  mysql_exec(mysql, sql);
  MYSQL_RES *res = mysql_store_result(mysql);
  if (!res) return rows;
  MYSQL_ROW r;
  while ((r = mysql_fetch_row(res))) {
    UniqueDataRow dr;
    dr.id = atoi(r[0]);
    dr.ring_idx = atoi(r[1]);
    dr.code = r[2] ? r[2] : "";
    dr.val = r[3] ? atoi(r[3]) : 0;
    rows.push_back(dr);
  }
  mysql_free_result(res);
  return rows;
}

/*
 * Test 14: Duplicate unique value within same PK prefix.
 * Two inserts for the same PK prefix carry the same unique value.
 * The commit must fail with ER_DUP_ENTRY (NDB error 893).
 */
static bool test_unique_dup_same_prefix(Ndb *ndb, MYSQL *mysql) {
  std::cout << "[Test 14] Unique dup - same PK prefix" << std::endl;

  mysql_exec(mysql, "DROP TABLE IF EXISTS test.rb_t14");
  char ddl[1024];
  snprintf(ddl, sizeof(ddl), CREATE_UNIQUE, "rb_t14", 3);
  mysql_exec(mysql, ddl);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("rb_t14");
  const NdbDictionary::Table *table = dict->getTable("rb_t14");
  TEST_ASSERT(table != nullptr, "getTable");

  UniqueRecordHelper h;
  TEST_ASSERT(h.init(table), "init record helper");
  char *rowbuf = h.newRow();
  unsigned char *mask = h.newUserMask(table);

  // Insert two rows: AAA then BBB - should succeed
  {
    NdbTransaction *trans = ndb->startTransaction(table);
    TEST_ASSERT(trans != nullptr, "startTransaction tx1");
    NdbRingBufferWriter writer(table, h.record, trans);
    TEST_ASSERT(writer.getErrorCode() == 0, "writer init tx1");

    h.fillRow(rowbuf, 1, "AAA", 10);
    TEST_ASSERT(writer.addRow(rowbuf, mask) != nullptr, "addRow AAA");
    h.fillRow(rowbuf, 1, "BBB", 20);
    TEST_ASSERT(writer.addRow(rowbuf, mask) != nullptr, "addRow BBB");
    TEST_ASSERT(writer.flush() == 0, "flush tx1");
    TEST_ASSERT(trans->execute(NdbTransaction::Commit) == 0, "commit tx1");
    ndb->closeTransaction(trans);
  }

  auto rows1 = readUniqueRows(mysql, "rb_t14", 1);
  TEST_ASSERT(rows1.size() == 2, "after tx1: expected 2 rows");

  // Insert duplicate 'AAA' for same PK prefix - must fail
  {
    NdbTransaction *trans = ndb->startTransaction(table);
    TEST_ASSERT(trans != nullptr, "startTransaction tx2");
    NdbRingBufferWriter writer(table, h.record, trans);
    TEST_ASSERT(writer.getErrorCode() == 0, "writer init tx2");

    h.fillRow(rowbuf, 1, "AAA", 30);
    const NdbOperation *op = writer.addRow(rowbuf, mask);
    // addRow may succeed (ops just queued), failure at flush/commit
    if (op != nullptr) {
      int flush_rc = writer.flush();
      if (flush_rc == 0) {
        // Flush succeeded, error should come at commit
        int exec_rc = trans->execute(NdbTransaction::Commit);
        TEST_ASSERT(exec_rc != 0, "commit should fail with dup key");
      }
      // Else flush failed - also acceptable
    }
    ndb->closeTransaction(trans);
  }

  // Also test cross-prefix duplicate: id=2 with code='BBB'
  {
    NdbTransaction *trans = ndb->startTransaction(table);
    TEST_ASSERT(trans != nullptr, "startTransaction tx3");
    NdbRingBufferWriter writer(table, h.record, trans);
    TEST_ASSERT(writer.getErrorCode() == 0, "writer init tx3");

    h.fillRow(rowbuf, 2, "BBB", 40);
    const NdbOperation *op = writer.addRow(rowbuf, mask);
    if (op != nullptr) {
      int flush_rc = writer.flush();
      if (flush_rc == 0) {
        int exec_rc = trans->execute(NdbTransaction::Commit);
        TEST_ASSERT(exec_rc != 0, "cross-prefix dup should fail");
      }
    }
    ndb->closeTransaction(trans);
  }

  // Verify original rows are intact
  auto rows2 = readUniqueRows(mysql, "rb_t14", 1);
  TEST_ASSERT(rows2.size() == 2, "original rows intact");
  TEST_ASSERT(rows2[0].code == "AAA" && rows2[1].code == "BBB",
              "original data intact");

  delete[] rowbuf;
  delete[] mask;
  mysql_exec(mysql, "DROP TABLE test.rb_t14");
  TEST_PASS("Unique dup - same PK prefix");
  return true;
}

/*
 * Test 15: Wrap-time collision with active slot's unique value.
 * Fill the ring, wrap once (OK), then try wrapping with a value
 * that collides with a still-active slot.  Verify the error, then
 * retry with a non-duplicate value.
 */
static bool test_unique_wrap_collision(Ndb *ndb, MYSQL *mysql) {
  std::cout << "[Test 15] Unique wrap collision" << std::endl;

  mysql_exec(mysql, "DROP TABLE IF EXISTS test.rb_t15");
  char ddl[1024];
  snprintf(ddl, sizeof(ddl), CREATE_UNIQUE, "rb_t15", 3);
  mysql_exec(mysql, ddl);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("rb_t15");
  const NdbDictionary::Table *table = dict->getTable("rb_t15");
  TEST_ASSERT(table != nullptr, "getTable");

  UniqueRecordHelper h;
  TEST_ASSERT(h.init(table), "init record helper");
  char *rowbuf = h.newRow();
  unsigned char *mask = h.newUserMask(table);

  // Fill ring (size=3): slot1=AAA, slot2=BBB, slot3=CCC
  {
    NdbTransaction *trans = ndb->startTransaction(table);
    TEST_ASSERT(trans != nullptr, "startTransaction fill");
    NdbRingBufferWriter writer(table, h.record, trans);
    TEST_ASSERT(writer.getErrorCode() == 0, "writer init fill");

    h.fillRow(rowbuf, 1, "AAA", 10);
    TEST_ASSERT(writer.addRow(rowbuf, mask) != nullptr, "addRow AAA");
    h.fillRow(rowbuf, 1, "BBB", 20);
    TEST_ASSERT(writer.addRow(rowbuf, mask) != nullptr, "addRow BBB");
    h.fillRow(rowbuf, 1, "CCC", 30);
    TEST_ASSERT(writer.addRow(rowbuf, mask) != nullptr, "addRow CCC");
    TEST_ASSERT(writer.flush() == 0, "flush fill");
    TEST_ASSERT(trans->execute(NdbTransaction::Commit) == 0, "commit fill");
    ndb->closeTransaction(trans);
  }

  auto rows1 = readUniqueRows(mysql, "rb_t15", 1);
  TEST_ASSERT(rows1.size() == 3, "after fill: expected 3 rows");

  // 4th insert wraps to slot 1: AAA freed, DDD takes its place - should succeed
  {
    NdbTransaction *trans = ndb->startTransaction(table);
    TEST_ASSERT(trans != nullptr, "startTransaction wrap1");
    NdbRingBufferWriter writer(table, h.record, trans);
    TEST_ASSERT(writer.getErrorCode() == 0, "writer init wrap1");

    h.fillRow(rowbuf, 1, "DDD", 40);
    TEST_ASSERT(writer.addRow(rowbuf, mask) != nullptr, "addRow DDD");
    TEST_ASSERT(writer.flush() == 0, "flush wrap1");
    TEST_ASSERT(trans->execute(NdbTransaction::Commit) == 0, "commit wrap1");
    ndb->closeTransaction(trans);
  }

  // Verify: slot1=DDD, slot2=BBB, slot3=CCC
  auto rows2 = readUniqueRows(mysql, "rb_t15", 1);
  TEST_ASSERT(rows2.size() == 3, "after wrap1: expected 3 rows");
  TEST_ASSERT(rows2[0].code == "DDD", "slot1 should be DDD");
  TEST_ASSERT(rows2[1].code == "BBB", "slot2 should be BBB");
  TEST_ASSERT(rows2[2].code == "CCC", "slot3 should be CCC");

  // 5th insert wraps to slot 2: BBB would be freed, but 'CCC' collides
  // with active slot 3 - must fail
  {
    NdbTransaction *trans = ndb->startTransaction(table);
    TEST_ASSERT(trans != nullptr, "startTransaction wrap2");
    NdbRingBufferWriter writer(table, h.record, trans);
    TEST_ASSERT(writer.getErrorCode() == 0, "writer init wrap2");

    h.fillRow(rowbuf, 1, "CCC", 50);
    const NdbOperation *op = writer.addRow(rowbuf, mask);
    if (op != nullptr) {
      int flush_rc = writer.flush();
      if (flush_rc == 0) {
        int exec_rc = trans->execute(NdbTransaction::Commit);
        TEST_ASSERT(exec_rc != 0, "wrap collision should fail");
      }
    }
    ndb->closeTransaction(trans);
  }

  // Verify state unchanged after failed insert
  auto rows3 = readUniqueRows(mysql, "rb_t15", 1);
  TEST_ASSERT(rows3.size() == 3, "after failed wrap: expected 3 rows");
  TEST_ASSERT(rows3[0].code == "DDD", "slot1 still DDD");
  TEST_ASSERT(rows3[1].code == "BBB", "slot2 still BBB");
  TEST_ASSERT(rows3[2].code == "CCC", "slot3 still CCC");

  // Retry with non-duplicate value - should succeed at slot 2
  {
    NdbTransaction *trans = ndb->startTransaction(table);
    TEST_ASSERT(trans != nullptr, "startTransaction retry");
    NdbRingBufferWriter writer(table, h.record, trans);
    TEST_ASSERT(writer.getErrorCode() == 0, "writer init retry");

    h.fillRow(rowbuf, 1, "EEE", 50);
    TEST_ASSERT(writer.addRow(rowbuf, mask) != nullptr, "addRow EEE");
    TEST_ASSERT(writer.flush() == 0, "flush retry");
    TEST_ASSERT(trans->execute(NdbTransaction::Commit) == 0, "commit retry");
    ndb->closeTransaction(trans);
  }

  // Verify: slot1=DDD, slot2=EEE, slot3=CCC
  auto rows4 = readUniqueRows(mysql, "rb_t15", 1);
  TEST_ASSERT(rows4.size() == 3, "after retry: expected 3 rows");
  TEST_ASSERT(rows4[0].code == "DDD", "slot1 DDD after retry");
  TEST_ASSERT(rows4[1].code == "EEE", "slot2 EEE after retry");
  TEST_ASSERT(rows4[2].code == "CCC", "slot3 CCC after retry");

  delete[] rowbuf;
  delete[] mask;
  mysql_exec(mysql, "DROP TABLE test.rb_t15");
  TEST_PASS("Unique wrap collision");
  return true;
}

/*
 * Test 16: Freed unique value reusable after wrap.
 * After wrapping overwrites a slot, the old unique value is freed.
 * A subsequent insert (different PK prefix) can reuse it.
 */
static bool test_unique_freed_reuse(Ndb *ndb, MYSQL *mysql) {
  std::cout << "[Test 16] Freed unique value reuse" << std::endl;

  mysql_exec(mysql, "DROP TABLE IF EXISTS test.rb_t16");
  char ddl[1024];
  snprintf(ddl, sizeof(ddl), CREATE_UNIQUE, "rb_t16", 3);
  mysql_exec(mysql, ddl);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("rb_t16");
  const NdbDictionary::Table *table = dict->getTable("rb_t16");
  TEST_ASSERT(table != nullptr, "getTable");

  UniqueRecordHelper h;
  TEST_ASSERT(h.init(table), "init record helper");
  char *rowbuf = h.newRow();
  unsigned char *mask = h.newUserMask(table);

  // Fill ring for id=1: slot1=AAA, slot2=BBB, slot3=CCC
  {
    NdbTransaction *trans = ndb->startTransaction(table);
    TEST_ASSERT(trans != nullptr, "startTransaction fill");
    NdbRingBufferWriter writer(table, h.record, trans);
    TEST_ASSERT(writer.getErrorCode() == 0, "writer init fill");

    h.fillRow(rowbuf, 1, "AAA", 10);
    TEST_ASSERT(writer.addRow(rowbuf, mask) != nullptr, "addRow AAA");
    h.fillRow(rowbuf, 1, "BBB", 20);
    TEST_ASSERT(writer.addRow(rowbuf, mask) != nullptr, "addRow BBB");
    h.fillRow(rowbuf, 1, "CCC", 30);
    TEST_ASSERT(writer.addRow(rowbuf, mask) != nullptr, "addRow CCC");
    TEST_ASSERT(writer.flush() == 0, "flush fill");
    TEST_ASSERT(trans->execute(NdbTransaction::Commit) == 0, "commit fill");
    ndb->closeTransaction(trans);
  }

  // Wrap id=1: slot 1 overwritten, AAA freed, DDD takes its place
  {
    NdbTransaction *trans = ndb->startTransaction(table);
    TEST_ASSERT(trans != nullptr, "startTransaction wrap");
    NdbRingBufferWriter writer(table, h.record, trans);
    TEST_ASSERT(writer.getErrorCode() == 0, "writer init wrap");

    h.fillRow(rowbuf, 1, "DDD", 40);
    TEST_ASSERT(writer.addRow(rowbuf, mask) != nullptr, "addRow DDD");
    TEST_ASSERT(writer.flush() == 0, "flush wrap");
    TEST_ASSERT(trans->execute(NdbTransaction::Commit) == 0, "commit wrap");
    ndb->closeTransaction(trans);
  }

  // AAA was freed - id=2 can now use it
  {
    NdbTransaction *trans = ndb->startTransaction(table);
    TEST_ASSERT(trans != nullptr, "startTransaction reuse");
    NdbRingBufferWriter writer(table, h.record, trans);
    TEST_ASSERT(writer.getErrorCode() == 0, "writer init reuse");

    h.fillRow(rowbuf, 2, "AAA", 50);
    TEST_ASSERT(writer.addRow(rowbuf, mask) != nullptr, "addRow AAA reuse");
    TEST_ASSERT(writer.flush() == 0, "flush reuse");
    TEST_ASSERT(trans->execute(NdbTransaction::Commit) == 0,
                "commit reuse - AAA should be free");
    ndb->closeTransaction(trans);
  }

  // Verify: id=1 has DDD/BBB/CCC, id=2 has AAA
  auto rows = readUniqueRows(mysql, "rb_t16");
  TEST_ASSERT(rows.size() == 4, "expected 4 rows total");

  bool found_id2_AAA = false;
  for (const auto &r : rows) {
    if (r.id == 2 && r.code == "AAA") found_id2_AAA = true;
  }
  TEST_ASSERT(found_id2_AAA, "id=2 should have AAA");

  // BBB is still active in id=1 slot 2 - cannot reuse
  {
    NdbTransaction *trans = ndb->startTransaction(table);
    TEST_ASSERT(trans != nullptr, "startTransaction dup_BBB");
    NdbRingBufferWriter writer(table, h.record, trans);
    TEST_ASSERT(writer.getErrorCode() == 0, "writer init dup_BBB");

    h.fillRow(rowbuf, 2, "BBB", 60);
    const NdbOperation *op = writer.addRow(rowbuf, mask);
    if (op != nullptr) {
      int flush_rc = writer.flush();
      if (flush_rc == 0) {
        int exec_rc = trans->execute(NdbTransaction::Commit);
        TEST_ASSERT(exec_rc != 0, "BBB still active - should fail");
      }
    }
    ndb->closeTransaction(trans);
  }

  delete[] rowbuf;
  delete[] mask;
  mysql_exec(mysql, "DROP TABLE test.rb_t16");
  TEST_PASS("Freed unique value reuse");
  return true;
}

/*
 * Test 17: Concurrent same-prefix inserts from multiple Ndb connections.
 * 4 threads each insert 10 rows for the same client_id.
 * Verifies meta row survives after all concurrent commits.
 */
static bool test_concurrent_same_prefix(Ndb_cluster_connection *conn,
                                        MYSQL *mysql) {
  std::cout << "[Test 17] Concurrent same-prefix inserts" << std::endl;

  mysql_exec(mysql, "DROP TABLE IF EXISTS test.rb_t17");
  char ddl[1024];
  snprintf(ddl, sizeof(ddl), CREATE_BASIC, "rb_t17", 5);
  mysql_exec(mysql, ddl);

  const int NUM_THREADS = 4;
  const int INSERTS_PER_THREAD = 10;
  const int CLIENT_ID = 99;
  std::atomic<int> errors{0};
  std::atomic<int> ready_count{0};
  std::atomic<bool> go{false};

  auto thread_fn = [&](int thread_id) {
    Ndb ndb(conn, "test");
    if (ndb.init() != 0) {
      std::cerr << "  Thread " << thread_id << " Ndb init failed" << std::endl;
      errors++;
      return;
    }

    NdbDictionary::Dictionary *dict = ndb.getDictionary();
    dict->invalidateTable("rb_t17");
    const NdbDictionary::Table *table = dict->getTable("rb_t17");
    if (!table) {
      std::cerr << "  Thread " << thread_id << " getTable failed" << std::endl;
      errors++;
      return;
    }

    BasicRecordHelper h;
    if (!h.init(table)) {
      errors++;
      return;
    }

    char *rowbuf = h.newRow();
    unsigned char *mask = h.newUserMask(table);

    // Signal ready, wait for go
    ready_count++;
    while (!go.load()) {}

    for (int i = 0; i < INSERTS_PER_THREAD; i++) {
      bool ok = false;
      for (int attempt = 0; attempt < 5 && !ok; attempt++) {
        char data[64];
        snprintf(data, sizeof(data), "t%d_i%d", thread_id, i);
        h.fillRow(rowbuf, CLIENT_ID, data);

        NdbTransaction *trans = ndb.startTransaction(table);
        if (!trans) {
          continue;
        }

        NdbRingBufferWriter writer(table, h.record, trans);
        if (writer.getErrorCode() != 0) {
          ndb.closeTransaction(trans);
          continue;
        }

        const NdbOperation *op = writer.addRow(rowbuf, mask);
        if (!op) {
          ndb.closeTransaction(trans);
          continue;
        }

        if (writer.flush() != 0) {
          ndb.closeTransaction(trans);
          continue;
        }

        if (trans->execute(NdbTransaction::Commit) == 0) {
          ok = true;
        }
        ndb.closeTransaction(trans);
      }
      if (!ok) {
        std::cerr << "  Thread " << thread_id << " insert " << i
                  << " failed after retries" << std::endl;
        errors++;
      }
    }

    delete[] rowbuf;
    delete[] mask;
  };

  // Spawn threads
  std::vector<std::thread> threads;
  for (int t = 0; t < NUM_THREADS; t++) {
    threads.emplace_back(thread_fn, t);
  }

  // Wait for all threads ready, then release
  while (ready_count.load() < NUM_THREADS) {}
  go.store(true);

  for (auto &t : threads) t.join();

  TEST_ASSERT(errors.load() == 0, "no thread errors");

  // Verify: 5 data rows should exist
  auto rows = readDataRows(mysql, "rb_t17", CLIENT_ID);
  std::cout << "  Data rows: " << rows.size() << std::endl;
  TEST_ASSERT((int)rows.size() == 5, "expected 5 data rows");

  // Verify: meta row should exist (total = data + meta = 6)
  int total = countAllRows(mysql, "rb_t17", CLIENT_ID);
  std::cout << "  Total rows (with meta): " << total << std::endl;
  TEST_ASSERT(total == 6, "expected 6 total (5 data + 1 meta)");

  // Meta invariant: no concurrent meta update may be lost. With
  // NUM_THREADS x INSERTS_PER_THREAD successful commits the packed meta
  // must show exactly that many total_inserts, a full ring (count =
  // ring_size) and the correspondingly advanced next_pos.
  {
    const Uint64 expected_total = NUM_THREADS * INSERTS_PER_THREAD;  // 40
    std::string hex = readMetaHex(mysql, "rb_t17", CLIENT_ID);
    TEST_ASSERT(!hex.empty(), "meta row readable");
    Uint64 total_inserts = metaFieldLE(hex, 16, 8);
    Uint64 count = metaFieldLE(hex, 8, 4);
    Uint64 next_pos = metaFieldLE(hex, 4, 4);
    std::cout << "  Meta: count=" << count << " total_inserts="
              << total_inserts << std::endl;
    TEST_ASSERT(total_inserts == expected_total,
                "lost meta update: total_inserts=" +
                    std::to_string(total_inserts) + " expected " +
                    std::to_string(expected_total));
    TEST_ASSERT(count == 5, "count should equal ring_size (5)");
    // next_pos: starts at 1, advanced expected_total times in a size-5 ring
    Uint64 expected_next = ((1 - 1 + expected_total) % 5) + 1;
    TEST_ASSERT(next_pos == expected_next,
                "next_pos=" + std::to_string(next_pos) + " expected " +
                    std::to_string(expected_next));
  }

  mysql_exec(mysql, "DROP TABLE test.rb_t17");
  TEST_PASS("Concurrent same-prefix inserts");
  return true;
}

// ---------------------------------------------------------------
// deleteOldest test cases
// ---------------------------------------------------------------

/*
 * Helper: insert N rows for a single client_id via NdbRingBufferWriter
 * with payloads "<tag>_<i>".  Caller owns rowbuf/mask.
 */
static bool insertN(Ndb *ndb, const NdbDictionary::Table *table,
                    BasicRecordHelper &h, char *rowbuf,
                    unsigned char *mask, Int32 cid, const char *tag,
                    int n) {
  NdbTransaction *trans = ndb->startTransaction(table);
  if (!trans) return false;
  NdbRingBufferWriter writer(table, h.record, trans);
  if (writer.getErrorCode() != 0) {
    ndb->closeTransaction(trans);
    return false;
  }
  for (int i = 0; i < n; i++) {
    char buf[32];
    snprintf(buf, sizeof(buf), "%s_%d", tag, i);
    h.fillRow(rowbuf, cid, buf);
    if (!writer.addRow(rowbuf, mask)) {
      ndb->closeTransaction(trans);
      return false;
    }
  }
  if (writer.flush() != 0) {
    ndb->closeTransaction(trans);
    return false;
  }
  if (trans->execute(NdbTransaction::Commit) != 0) {
    ndb->closeTransaction(trans);
    return false;
  }
  ndb->closeTransaction(trans);
  return true;
}

/*
 * Test 18: deleteOldest on a partial (not yet wrapped) ring.
 * Insert 3 rows, deleteOldest(1) - slot 1 must go, slots 2,3 survive.
 */
static bool test_delete_oldest_basic(Ndb *ndb, MYSQL *mysql) {
  std::cout << "[Test 18] deleteOldest - basic partial ring" << std::endl;

  mysql_exec(mysql, "DROP TABLE IF EXISTS test.rb_t18");
  char ddl[1024];
  snprintf(ddl, sizeof(ddl), CREATE_BASIC, "rb_t18", 5);
  mysql_exec(mysql, ddl);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("rb_t18");
  const NdbDictionary::Table *table = dict->getTable("rb_t18");
  TEST_ASSERT(table != nullptr, "getTable");

  BasicRecordHelper h;
  TEST_ASSERT(h.init(table), "init record helper");
  char *rowbuf = h.newRow();
  unsigned char *mask = h.newUserMask(table);

  TEST_ASSERT(insertN(ndb, table, h, rowbuf, mask, 1, "row", 3),
              "insert 3 rows");

  {
    NdbTransaction *trans = ndb->startTransaction(table);
    TEST_ASSERT(trans != nullptr, "startTransaction");
    NdbRingBufferWriter writer(table, h.record, trans);
    TEST_ASSERT(writer.getErrorCode() == 0, "writer init");

    h.fillRow(rowbuf, 1, "");  // only PK column matters
    Uint32 actual = 0xdeadbeef;
    int rc = writer.deleteOldest(rowbuf, 1, &actual);
    TEST_ASSERT(rc == 0,
                std::string("deleteOldest: ") + writer.getErrorMessage());
    TEST_ASSERT(actual == 1,
                "expected 1 deleted, got " + std::to_string(actual));

    TEST_ASSERT(trans->execute(NdbTransaction::Commit) == 0, "commit");
    ndb->closeTransaction(trans);
  }

  auto rows = readDataRows(mysql, "rb_t18", 1);
  TEST_ASSERT(rows.size() == 2,
              "expected 2 rows, got " + std::to_string(rows.size()));
  TEST_ASSERT(rows[0].ring_idx == 2, "survivor at slot 2");
  TEST_ASSERT(rows[0].data == "row_1", "slot 2 has row_1");
  TEST_ASSERT(rows[1].ring_idx == 3, "survivor at slot 3");
  TEST_ASSERT(rows[1].data == "row_2", "slot 3 has row_2");

  delete[] rowbuf;
  delete[] mask;
  mysql_exec(mysql, "DROP TABLE test.rb_t18");
  TEST_PASS("deleteOldest - basic partial ring");
  return true;
}

/*
 * Test 19: deleteOldest clamps to count, then is idempotent on drained ring.
 * Insert 3, deleteOldest(maxN=10) -> outActual=3, ring drained.
 * Call deleteOldest again on drained ring -> outActual=0, no error.
 * Meta row must remain (count=0, slot 0 still present).
 */
static bool test_delete_oldest_drain_idempotent(Ndb *ndb, MYSQL *mysql) {
  std::cout << "[Test 19] deleteOldest - clamp + idempotent on drained"
            << std::endl;

  mysql_exec(mysql, "DROP TABLE IF EXISTS test.rb_t19");
  char ddl[1024];
  snprintf(ddl, sizeof(ddl), CREATE_BASIC, "rb_t19", 5);
  mysql_exec(mysql, ddl);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("rb_t19");
  const NdbDictionary::Table *table = dict->getTable("rb_t19");
  TEST_ASSERT(table != nullptr, "getTable");

  BasicRecordHelper h;
  TEST_ASSERT(h.init(table), "init record helper");
  char *rowbuf = h.newRow();
  unsigned char *mask = h.newUserMask(table);

  TEST_ASSERT(insertN(ndb, table, h, rowbuf, mask, 1, "x", 3),
              "insert 3 rows");

  // Drain via deleteOldest(maxN=10) - should clamp to 3.
  {
    NdbTransaction *trans = ndb->startTransaction(table);
    NdbRingBufferWriter writer(table, h.record, trans);
    h.fillRow(rowbuf, 1, "");
    Uint32 actual = 0;
    int rc = writer.deleteOldest(rowbuf, 10, &actual);
    TEST_ASSERT(rc == 0,
                std::string("deleteOldest: ") + writer.getErrorMessage());
    TEST_ASSERT(actual == 3,
                "expected 3 deleted, got " + std::to_string(actual));
    TEST_ASSERT(trans->execute(NdbTransaction::Commit) == 0, "commit");
    ndb->closeTransaction(trans);
  }

  // 0 data rows remain, but meta row stays (count=0).
  auto rows = readDataRows(mysql, "rb_t19", 1);
  TEST_ASSERT(rows.size() == 0, "expected 0 data rows");
  int total = countAllRows(mysql, "rb_t19", 1);
  TEST_ASSERT(total == 1,
              "expected meta row only, got " + std::to_string(total));

  // Idempotent: deleteOldest on drained ring is a no-op success.
  {
    NdbTransaction *trans = ndb->startTransaction(table);
    NdbRingBufferWriter writer(table, h.record, trans);
    h.fillRow(rowbuf, 1, "");
    Uint32 actual = 99;
    int rc = writer.deleteOldest(rowbuf, 5, &actual);
    TEST_ASSERT(rc == 0, "deleteOldest on drained: should succeed");
    TEST_ASSERT(actual == 0, "expected 0 deleted on drained ring");
    TEST_ASSERT(trans->execute(NdbTransaction::Commit) == 0, "commit");
    ndb->closeTransaction(trans);
  }

  delete[] rowbuf;
  delete[] mask;
  mysql_exec(mysql, "DROP TABLE test.rb_t19");
  TEST_PASS("deleteOldest - clamp + idempotent on drained");
  return true;
}

/*
 * Test 20: deleteOldest on a never-touched prefix is a no-op success.
 * No meta row exists -> readMetaRow returns 626 -> outActual=0, no error.
 */
static bool test_delete_oldest_empty_table(Ndb *ndb, MYSQL *mysql) {
  std::cout << "[Test 20] deleteOldest - empty table" << std::endl;

  mysql_exec(mysql, "DROP TABLE IF EXISTS test.rb_t20");
  char ddl[1024];
  snprintf(ddl, sizeof(ddl), CREATE_BASIC, "rb_t20", 5);
  mysql_exec(mysql, ddl);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("rb_t20");
  const NdbDictionary::Table *table = dict->getTable("rb_t20");
  TEST_ASSERT(table != nullptr, "getTable");

  BasicRecordHelper h;
  TEST_ASSERT(h.init(table), "init record helper");
  char *rowbuf = h.newRow();
  unsigned char *mask = h.newUserMask(table);

  {
    NdbTransaction *trans = ndb->startTransaction(table);
    TEST_ASSERT(trans != nullptr, "startTransaction");
    NdbRingBufferWriter writer(table, h.record, trans);
    h.fillRow(rowbuf, 1, "");
    Uint32 actual = 99;
    int rc = writer.deleteOldest(rowbuf, 3, &actual);
    TEST_ASSERT(rc == 0,
                std::string("deleteOldest: ") + writer.getErrorMessage());
    TEST_ASSERT(actual == 0, "expected 0 deleted on empty");
    TEST_ASSERT(trans->execute(NdbTransaction::Commit) == 0, "commit");
    ndb->closeTransaction(trans);
  }

  // Nothing should have been written.
  int total = countAllRows(mysql, "rb_t20", 1);
  TEST_ASSERT(total == 0,
              "expected 0 rows, got " + std::to_string(total));

  delete[] rowbuf;
  delete[] mask;
  mysql_exec(mysql, "DROP TABLE test.rb_t20");
  TEST_PASS("deleteOldest - empty table");
  return true;
}

/*
 * Test 21: deleteOldest after the ring has wrapped.
 * ring_size=5, insert 7 rows.  After wrap: count=5, next_pos=3, slot order
 * (oldest->newest) = 3,4,5,1,2 (data: row_2, row_3, row_4, row_5, row_6).
 * deleteOldest(1) -> slot 3 (row_2) gone, count=4, next_pos still 3.
 * deleteOldest(2) -> slots 4,5 (row_3, row_4) gone.  Survivors: slots 1,2
 * holding row_5, row_6.
 */
static bool test_delete_oldest_after_wrap(Ndb *ndb, MYSQL *mysql) {
  std::cout << "[Test 21] deleteOldest - after wrap" << std::endl;

  mysql_exec(mysql, "DROP TABLE IF EXISTS test.rb_t21");
  char ddl[1024];
  snprintf(ddl, sizeof(ddl), CREATE_BASIC, "rb_t21", 5);
  mysql_exec(mysql, ddl);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("rb_t21");
  const NdbDictionary::Table *table = dict->getTable("rb_t21");
  TEST_ASSERT(table != nullptr, "getTable");

  BasicRecordHelper h;
  TEST_ASSERT(h.init(table), "init record helper");
  char *rowbuf = h.newRow();
  unsigned char *mask = h.newUserMask(table);

  TEST_ASSERT(insertN(ndb, table, h, rowbuf, mask, 1, "row", 7),
              "insert 7 rows (wraps)");

  // Sanity: post-wrap survivors are row_2..row_6.
  {
    auto rows = readDataRows(mysql, "rb_t21", 1);
    TEST_ASSERT(rows.size() == 5, "expected 5 survivors after wrap");
    // Slot 1 = row_5 (insert#6), slot 2 = row_6 (insert#7), slot 3 = row_2
    // (insert#3, oldest), slot 4 = row_3, slot 5 = row_4.
    TEST_ASSERT(rows[0].ring_idx == 1 && rows[0].data == "row_5", "s1=row_5");
    TEST_ASSERT(rows[1].ring_idx == 2 && rows[1].data == "row_6", "s2=row_6");
    TEST_ASSERT(rows[2].ring_idx == 3 && rows[2].data == "row_2", "s3=row_2");
    TEST_ASSERT(rows[3].ring_idx == 4 && rows[3].data == "row_3", "s4=row_3");
    TEST_ASSERT(rows[4].ring_idx == 5 && rows[4].data == "row_4", "s5=row_4");
  }

  // deleteOldest(1) - slot 3 (row_2, oldest).
  {
    NdbTransaction *trans = ndb->startTransaction(table);
    NdbRingBufferWriter writer(table, h.record, trans);
    h.fillRow(rowbuf, 1, "");
    Uint32 actual = 0;
    TEST_ASSERT(writer.deleteOldest(rowbuf, 1, &actual) == 0,
                std::string("deleteOldest(1): ") + writer.getErrorMessage());
    TEST_ASSERT(actual == 1, "expected 1 deleted");
    TEST_ASSERT(trans->execute(NdbTransaction::Commit) == 0, "commit");
    ndb->closeTransaction(trans);
  }

  // deleteOldest(2) - slots 4,5 (row_3, row_4).
  {
    NdbTransaction *trans = ndb->startTransaction(table);
    NdbRingBufferWriter writer(table, h.record, trans);
    h.fillRow(rowbuf, 1, "");
    Uint32 actual = 0;
    TEST_ASSERT(writer.deleteOldest(rowbuf, 2, &actual) == 0,
                std::string("deleteOldest(2): ") + writer.getErrorMessage());
    TEST_ASSERT(actual == 2, "expected 2 deleted");
    TEST_ASSERT(trans->execute(NdbTransaction::Commit) == 0, "commit");
    ndb->closeTransaction(trans);
  }

  auto rows = readDataRows(mysql, "rb_t21", 1);
  TEST_ASSERT(rows.size() == 2,
              "expected 2 survivors, got " + std::to_string(rows.size()));
  TEST_ASSERT(rows[0].ring_idx == 1 && rows[0].data == "row_5",
              "slot 1 still row_5");
  TEST_ASSERT(rows[1].ring_idx == 2 && rows[1].data == "row_6",
              "slot 2 still row_6");

  delete[] rowbuf;
  delete[] mask;
  mysql_exec(mysql, "DROP TABLE test.rb_t21");
  TEST_PASS("deleteOldest - after wrap");
  return true;
}

/*
 * Test 22: refill order after deleteOldest - the "no hole" claim.
 * ring_size=5.  Fill (5 inserts, count=5, next_pos=1).  deleteOldest(2)
 * removes slots 1,2.  Subsequent inserts must land at slots 1, then 2
 * (next_pos walks forward, freed slots refill in arrival order).
 */
static bool test_delete_oldest_refill_order(Ndb *ndb, MYSQL *mysql) {
  std::cout << "[Test 22] deleteOldest - refill order (no-hole proof)"
            << std::endl;

  mysql_exec(mysql, "DROP TABLE IF EXISTS test.rb_t22");
  char ddl[1024];
  snprintf(ddl, sizeof(ddl), CREATE_BASIC, "rb_t22", 5);
  mysql_exec(mysql, ddl);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("rb_t22");
  const NdbDictionary::Table *table = dict->getTable("rb_t22");
  TEST_ASSERT(table != nullptr, "getTable");

  BasicRecordHelper h;
  TEST_ASSERT(h.init(table), "init record helper");
  char *rowbuf = h.newRow();
  unsigned char *mask = h.newUserMask(table);

  TEST_ASSERT(insertN(ndb, table, h, rowbuf, mask, 1, "orig", 5),
              "fill ring (5 inserts)");

  // deleteOldest(2) - drops slots 1,2.  Survivors: slots 3,4,5 with
  // orig_2, orig_3, orig_4.
  {
    NdbTransaction *trans = ndb->startTransaction(table);
    NdbRingBufferWriter writer(table, h.record, trans);
    h.fillRow(rowbuf, 1, "");
    Uint32 actual = 0;
    TEST_ASSERT(writer.deleteOldest(rowbuf, 2, &actual) == 0,
                std::string("deleteOldest: ") + writer.getErrorMessage());
    TEST_ASSERT(actual == 2, "expected 2 deleted");
    TEST_ASSERT(trans->execute(NdbTransaction::Commit) == 0, "commit");
    ndb->closeTransaction(trans);
  }

  // Insert 2 more - should land at slots 1, 2 in that order.
  TEST_ASSERT(insertN(ndb, table, h, rowbuf, mask, 1, "post", 2),
              "insert 2 post-pop");

  auto rows = readDataRows(mysql, "rb_t22", 1);
  TEST_ASSERT(rows.size() == 5,
              "expected 5 rows, got " + std::to_string(rows.size()));
  // Slots 1,2 = post_0, post_1 (the refilled ones).
  // Slots 3,4,5 = orig_2, orig_3, orig_4 (untouched survivors).
  TEST_ASSERT(rows[0].ring_idx == 1 && rows[0].data == "post_0",
              "slot 1 = post_0 (refilled first)");
  TEST_ASSERT(rows[1].ring_idx == 2 && rows[1].data == "post_1",
              "slot 2 = post_1 (refilled second)");
  TEST_ASSERT(rows[2].ring_idx == 3 && rows[2].data == "orig_2",
              "slot 3 = orig_2 (survivor)");
  TEST_ASSERT(rows[3].ring_idx == 4 && rows[3].data == "orig_3",
              "slot 4 = orig_3 (survivor)");
  TEST_ASSERT(rows[4].ring_idx == 5 && rows[4].data == "orig_4",
              "slot 5 = orig_4 (survivor)");

  delete[] rowbuf;
  delete[] mask;
  mysql_exec(mysql, "DROP TABLE test.rb_t22");
  TEST_PASS("deleteOldest - refill order (no-hole proof)");
  return true;
}

/*
 * Test 23: deleteOldest is scoped to one PK prefix.
 * Two prefixes (cid=1, cid=2) each filled to 3 rows.  deleteOldest on
 * cid=1 must not touch cid=2 rows.
 */
static bool test_delete_oldest_multi_prefix(Ndb *ndb, MYSQL *mysql) {
  std::cout << "[Test 23] deleteOldest - multi-prefix isolation" << std::endl;

  mysql_exec(mysql, "DROP TABLE IF EXISTS test.rb_t23");
  char ddl[1024];
  snprintf(ddl, sizeof(ddl), CREATE_BASIC, "rb_t23", 5);
  mysql_exec(mysql, ddl);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("rb_t23");
  const NdbDictionary::Table *table = dict->getTable("rb_t23");
  TEST_ASSERT(table != nullptr, "getTable");

  BasicRecordHelper h;
  TEST_ASSERT(h.init(table), "init record helper");
  char *rowbuf = h.newRow();
  unsigned char *mask = h.newUserMask(table);

  TEST_ASSERT(insertN(ndb, table, h, rowbuf, mask, 1, "a", 3),
              "fill cid=1");
  TEST_ASSERT(insertN(ndb, table, h, rowbuf, mask, 2, "b", 3),
              "fill cid=2");

  // deleteOldest(2) on cid=1.
  {
    NdbTransaction *trans = ndb->startTransaction(table);
    NdbRingBufferWriter writer(table, h.record, trans);
    h.fillRow(rowbuf, 1, "");
    Uint32 actual = 0;
    TEST_ASSERT(writer.deleteOldest(rowbuf, 2, &actual) == 0,
                std::string("deleteOldest: ") + writer.getErrorMessage());
    TEST_ASSERT(actual == 2, "expected 2 deleted from cid=1");
    TEST_ASSERT(trans->execute(NdbTransaction::Commit) == 0, "commit");
    ndb->closeTransaction(trans);
  }

  auto r1 = readDataRows(mysql, "rb_t23", 1);
  TEST_ASSERT(r1.size() == 1, "cid=1 should have 1 row left");
  TEST_ASSERT(r1[0].ring_idx == 3 && r1[0].data == "a_2",
              "cid=1 survivor is slot 3 / a_2");

  auto r2 = readDataRows(mysql, "rb_t23", 2);
  TEST_ASSERT(r2.size() == 3, "cid=2 must be untouched (3 rows)");
  TEST_ASSERT(r2[0].data == "b_0" && r2[1].data == "b_1" &&
                  r2[2].data == "b_2",
              "cid=2 data unchanged");

  delete[] rowbuf;
  delete[] mask;
  mysql_exec(mysql, "DROP TABLE test.rb_t23");
  TEST_PASS("deleteOldest - multi-prefix isolation");
  return true;
}

// ---------------------------------------------------------------
// Main
// ---------------------------------------------------------------

/*
 * Test 24: the raw NDB API cannot resize a ring buffer table.
 *
 * The MySQL handler rejects ALTER of MAX_ROWS_PER_PK, but the raw NDB API
 * (Table::setRingBufferSize + Dictionary::alterTable) reaches DBDICT directly.
 * DICT must reject the change (getRingBufferSizeFlag in alterTable_parse);
 * otherwise existing meta rows, which pack ManagedState for the original ring
 * size, would be left inconsistent. Before the DICT fix this alterTable
 * returned 0 (silent online resize) - this test would then fail at the
 * "must be rejected" assertion.
 */
static bool test_alter_ring_size_rejected(Ndb *ndb, MYSQL *mysql) {
  std::cout << "[Test 24] raw-API ring buffer resize rejected" << std::endl;

  mysql_exec(mysql, "DROP TABLE IF EXISTS test.rb_t24");
  char ddl[1024];
  snprintf(ddl, sizeof(ddl), CREATE_BASIC, "rb_t24", 5);
  mysql_exec(mysql, ddl);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("rb_t24");
  const NdbDictionary::Table *oldTab = dict->getTable("rb_t24");
  TEST_ASSERT(oldTab != nullptr, "getTable");
  TEST_ASSERT(oldTab->isRingBuffer(), "should be ring buffer");
  TEST_ASSERT(oldTab->getRingBufferSize() == 5, "ring_size=5");

  // Seed one row so a (buggy) resize would have real meta to corrupt.
  BasicRecordHelper h;
  TEST_ASSERT(h.init(oldTab), "init record helper");
  char *rowbuf = h.newRow();
  unsigned char *mask = h.newUserMask(oldTab);
  TEST_ASSERT(insertN(ndb, oldTab, h, rowbuf, mask, 1, "row", 1),
              "insert 1 row before alter");

  // Attempt to grow the ring via the raw API.
  NdbDictionary::Table newTab(*oldTab);
  newTab.setRingBufferSize(oldTab->getRingBufferSize() + 1);
  int rc = dict->alterTable(*oldTab, newTab);
  TEST_ASSERT(rc != 0, "alterTable resize must be rejected by DICT");
  TEST_ASSERT(dict->getNdbError().code == 741,
              "expected error 741, got " +
                  std::to_string(dict->getNdbError().code));
  // Diagnostic to stderr (discarded by the MTR wrapper) - keep stdout, which
  // is compared against the .result, free of the variable error code/message.
  std::cerr << "  (rejected as expected: " << dict->getNdbError().code << " "
            << dict->getNdbError().message << ")" << std::endl;

  // The stored size must be unchanged and the ring must still work.
  dict->invalidateTable("rb_t24");
  const NdbDictionary::Table *check = dict->getTable("rb_t24");
  TEST_ASSERT(check != nullptr, "re-getTable");
  TEST_ASSERT(check->getRingBufferSize() == 5,
              "ring_size still 5 after rejected alter");

  BasicRecordHelper h2;
  TEST_ASSERT(h2.init(check), "re-init record helper");
  char *rowbuf2 = h2.newRow();
  unsigned char *mask2 = h2.newUserMask(check);
  TEST_ASSERT(insertN(ndb, check, h2, rowbuf2, mask2, 1, "row", 2),
              "insert after rejected alter");
  auto rows = readDataRows(mysql, "rb_t24", 1);
  TEST_ASSERT(rows.size() == 3,
              "expected 3 rows after rejected alter, got " +
                  std::to_string(rows.size()));

  delete[] rowbuf;
  delete[] mask;
  delete[] rowbuf2;
  delete[] mask2;
  mysql_exec(mysql, "DROP TABLE test.rb_t24");
  TEST_PASS("raw-API ring buffer resize rejected");
  return true;
}

/*
 * Test 25 (B2): the ring meta-read "absorb" must not scrub the whole
 * transaction. When the first insert for a PK prefix reads the (absent) meta
 * row it gets the expected 626; the writer must ignore only that read, not
 * reset the transaction. Before the fix it did
 *   theCommitStatus=Started; theError.code=0; releaseCompletedOpsAndQueries();
 * which erased the error of, and freed, any UNRELATED operation the caller had
 * queued in the same transaction.
 *
 * Repro: in one transaction queue a duplicate insert into a plain table
 * (AO_IgnoreError, so it records error 630 without aborting), then ring-insert
 * a FRESH prefix (drives the 626 absorb). The unrelated op's error must still
 * be visible on the transaction afterwards.
 *
 * We never dereference the user op after addRow(): before the fix the scrub
 * has freed it, so touching it would be a use-after-free. We assert on the
 * transaction-level error only, which is safe.
 *
 * Before the fix: trans error == 0 (630 swallowed) -> this test FAILS.
 * After the fix:  trans error != 0 (630 preserved) -> this test PASSES.
 */
static bool test_meta_absorb_preserves_unrelated_op(Ndb *ndb, MYSQL *mysql) {
  std::cout << "[Test 25] meta-read absorb preserves unrelated op error"
            << std::endl;

  mysql_exec(mysql, "DROP TABLE IF EXISTS test.pt_t25");
  mysql_exec(mysql,
             "CREATE TABLE test.pt_t25 ("
             "  id INT NOT NULL PRIMARY KEY,"
             "  data VARCHAR(50)"
             ") ENGINE=NDB");
  mysql_exec(mysql, "INSERT INTO test.pt_t25 VALUES (1, 'seed')");
  mysql_exec(mysql, "DROP TABLE IF EXISTS test.rb_t25");
  char ddl[1024];
  snprintf(ddl, sizeof(ddl), CREATE_BASIC, "rb_t25", 5);
  mysql_exec(mysql, ddl);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("pt_t25");
  dict->invalidateTable("rb_t25");
  const NdbDictionary::Table *pt = dict->getTable("pt_t25");
  const NdbDictionary::Table *rb = dict->getTable("rb_t25");
  TEST_ASSERT(pt != nullptr && rb != nullptr, "getTable");

  BasicRecordHelper h;
  TEST_ASSERT(h.init(rb), "init record helper");
  char *rowbuf = h.newRow();
  unsigned char *mask = h.newUserMask(rb);
  h.fillRow(rowbuf, 7, "x");  // fresh prefix 7

  NdbTransaction *trans = ndb->startTransaction(rb);
  TEST_ASSERT(trans != nullptr, "startTransaction");

  // (a) Unrelated op in the SAME transaction: duplicate insert -> 630.
  //     AO_IgnoreError records the error without aborting the transaction.
  NdbOperation *userOp = trans->getNdbOperation(pt);
  TEST_ASSERT(userOp != nullptr, "getNdbOperation(pt)");
  TEST_ASSERT(userOp->insertTuple() == 0, "insertTuple");
  TEST_ASSERT(userOp->equal("id", 1) == 0, "equal id");
  TEST_ASSERT(userOp->setValue("data", "dup") == 0, "setValue data");
  userOp->setAbortOption(NdbOperation::AO_IgnoreError);

  // (b) Ring insert for a FRESH prefix -> readMetaRow() executes the round
  //     (runs the pending dup + the meta read) and, on 626, (unfixed) scrubs
  //     the transaction, erasing the dup's 630.
  NdbRingBufferWriter writer(rb, h.record, trans);
  TEST_ASSERT(writer.getErrorCode() == 0, "writer init");
  const NdbOperation *addOp = writer.addRow(rowbuf, mask);
  TEST_ASSERT(addOp != nullptr, std::string("addRow: ") + writer.getErrorMessage());

  // (c) The unrelated op's failure must still be observable. Do NOT touch
  //     userOp here - before the fix it has been freed by the scrub.
  int transErr = trans->getNdbError().code;
  std::cerr << "  (transErr after absorb = " << transErr << ")" << std::endl;
  TEST_ASSERT(transErr != 0,
              "unrelated dup insert error (630) swallowed by meta-read absorb");

  ndb->closeTransaction(trans);
  delete[] rowbuf;
  delete[] mask;
  mysql_exec(mysql, "DROP TABLE test.rb_t25");
  mysql_exec(mysql, "DROP TABLE test.pt_t25");
  TEST_PASS("meta-read absorb preserves unrelated op error");
  return true;
}

/*
 * Test 26 (B3): refreshTuple() on a ring buffer table must be blocked. The
 * DBTUP write guard only lists INSERT/WRITE/UPDATE/DELETE, so a raw ZREFRESH
 * slips through and mutates row/GCI state and fires events without going
 * through the ring bookkeeping.
 *
 * Before the fix: refreshTuple succeeds (rc == 0) -> this test FAILS.
 * After the fix:  refreshTuple is rejected with error 940 -> this test PASSES.
 */
static bool test_refresh_tuple_blocked(Ndb *ndb, MYSQL *mysql) {
  std::cout << "[Test 26] refreshTuple on ring buffer table blocked"
            << std::endl;

  mysql_exec(mysql, "DROP TABLE IF EXISTS test.rb_t26");
  char ddl[1024];
  snprintf(ddl, sizeof(ddl), CREATE_BASIC, "rb_t26", 5);
  mysql_exec(mysql, ddl);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("rb_t26");
  const NdbDictionary::Table *table = dict->getTable("rb_t26");
  TEST_ASSERT(table != nullptr, "getTable");

  BasicRecordHelper h;
  TEST_ASSERT(h.init(table), "init record helper");
  char *rowbuf = h.newRow();
  unsigned char *mask = h.newUserMask(table);
  TEST_ASSERT(insertN(ndb, table, h, rowbuf, mask, 1, "row", 2),
              "insert 2 rows");

  // refreshTuple on an existing data row (client_id=1, ring_idx=1), built as
  // an NdbRecord key buffer (refreshTuple is the NdbRecord form on the txn).
  const NdbRecord::Attr *ridx = findAttr(h.record, table, "ring_idx");
  TEST_ASSERT(ridx != nullptr, "ring_idx attr");
  h.fillRow(rowbuf, 1, "");         // client_id = 1
  setInt32(rowbuf, ridx, 1);        // ring_idx = 1

  NdbTransaction *trans = ndb->startTransaction(table);
  TEST_ASSERT(trans != nullptr, "startTransaction");
  const NdbOperation *op = trans->refreshTuple(h.record, rowbuf);
  TEST_ASSERT(op != nullptr, "refreshTuple define");
  int rc = trans->execute(NdbTransaction::Commit);
  int err = trans->getNdbError().code;
  std::cerr << "  (refresh rc=" << rc << " err=" << err << ")" << std::endl;
  TEST_ASSERT(rc != 0, "refreshTuple on ring buffer table must be rejected");
  TEST_ASSERT(err == 940,
              "expected error 940, got " + std::to_string(err));
  ndb->closeTransaction(trans);

  // The meta row (ring_idx=0) must be rejected the same way - the guard is
  // op-based, so this pins that no row-kind special case sneaks in.
  h.fillRow(rowbuf, 1, "");
  setInt32(rowbuf, ridx, 0);  // ring_idx = 0 (meta row)
  trans = ndb->startTransaction(table);
  TEST_ASSERT(trans != nullptr, "startTransaction (meta)");
  op = trans->refreshTuple(h.record, rowbuf);
  TEST_ASSERT(op != nullptr, "refreshTuple define (meta)");
  rc = trans->execute(NdbTransaction::Commit);
  err = trans->getNdbError().code;
  std::cerr << "  (meta refresh rc=" << rc << " err=" << err << ")"
            << std::endl;
  TEST_ASSERT(rc != 0, "refreshTuple on meta row must be rejected");
  TEST_ASSERT(err == 940,
              "expected error 940 on meta row, got " + std::to_string(err));
  ndb->closeTransaction(trans);

  delete[] rowbuf;
  delete[] mask;
  mysql_exec(mysql, "DROP TABLE test.rb_t26");
  TEST_PASS("refreshTuple on ring buffer table blocked");
  return true;
}

/*
 * Helper for Tests 27/28: attempt createTable of a deliberately-broken ring
 * buffer definition and require DICT to reject it. Before the fix DICT
 * accepts any ring metadata (col numbers/types/size unvalidated), so the
 * create SUCCEEDS - we then drop the table immediately (NEVER insert into a
 * table with corrupt ring metadata) and report the sub-case as failed.
 * Variable diagnostics go to stderr (the MTR wrapper discards it).
 */
static bool expect_create_rejected(NdbDictionary::Dictionary *dict,
                                   const NdbDictionary::Table &bad,
                                   const char *what, int expected_code) {
  int rc = dict->createTable(bad);
  if (rc == 0) {
    std::cerr << "  SUB-FAIL(" << what
              << "): createTable accepted invalid ring metadata" << std::endl;
    dict->dropTable(bad.getName());
    return false;
  }
  int code = dict->getNdbError().code;
  std::cerr << "  sub-ok(" << what << "): rejected with " << code << " "
            << dict->getNdbError().message << std::endl;
  if (expected_code != 0 && code != expected_code) {
    std::cerr << "  SUB-FAIL(" << what << "): expected error " << expected_code
              << ", got " << code << std::endl;
    return false;
  }
  return true;
}

/*
 * Test 27 (B14a): DICT must validate ring buffer metadata at create time.
 * The MySQL layer enforces: ring_idx is an INT and the last PK column;
 * ring_meta is a nullable VARBINARY(>=32) non-key column; ring size is
 * 1..2^31-1. The raw NDB API bypasses all of that - before the fix DICT
 * copies the three fields unvalidated, so e.g. an out-of-range
 * ring_idx_col_no makes isRingBufferMetaRow() read an arbitrary word of
 * every row. Each sub-case corrupts ONE field of a valid definition and
 * requires createTable to fail with error 703 (InvalidFormat).
 */
static bool test_dict_validates_ring_metadata(Ndb *ndb, MYSQL *mysql) {
  std::cout << "[Test 27] DICT validates ring metadata on create" << std::endl;

  mysql_exec(mysql, "DROP TABLE IF EXISTS test.rb_t27");
  mysql_exec(mysql,
             "CREATE TABLE test.rb_t27 ("
             "  client_id INT NOT NULL,"
             "  ring_idx INT NOT NULL DEFAULT 0,"
             "  ring_meta VARBINARY(64),"
             "  event_data VARCHAR(100),"
             "  small_vb VARBINARY(8),"
             "  vb_nn VARBINARY(64) NOT NULL,"
             "  PRIMARY KEY (client_id, ring_idx)"
             ") ENGINE=NDB,"
             "  COMMENT='NDB_TABLE=MAX_ROWS_PER_PK=5@ring_idx@ring_meta'");

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("rb_t27");
  const NdbDictionary::Table *tab = dict->getTable("rb_t27");
  TEST_ASSERT(tab != nullptr, "getTable");
  TEST_ASSERT(tab->isRingBuffer(), "should be ring buffer");

  const int cid_no = tab->getColumn("client_id")->getColumnNo();
  const int data_no = tab->getColumn("event_data")->getColumnNo();
  const int small_no = tab->getColumn("small_vb")->getColumnNo();
  const int vbnn_no = tab->getColumn("vb_nn")->getColumnNo();

  int failed = 0;
  {
    NdbDictionary::Table bad(*tab);
    bad.setName("rb_t27_bad");
    bad.setRingBufferSize(0);
    if (!expect_create_rejected(dict, bad, "size=0", 703)) failed++;
  }
  {
    NdbDictionary::Table bad(*tab);
    bad.setName("rb_t27_bad");
    bad.setRingBufferSize(0x80000000u);  // 2^31, above the 2^31-1 max
    if (!expect_create_rejected(dict, bad, "size=2^31", 703)) failed++;
  }
  {
    NdbDictionary::Table bad(*tab);
    bad.setName("rb_t27_bad");
    bad.setRingIdxColumnNo(99);  // out of range
    if (!expect_create_rejected(dict, bad, "idx-out-of-range", 703)) failed++;
  }
  {
    NdbDictionary::Table bad(*tab);
    bad.setName("rb_t27_bad");
    bad.setRingIdxColumnNo(data_no);  // VARCHAR, not INT, not a key col
    if (!expect_create_rejected(dict, bad, "idx=varchar-col", 703)) failed++;
  }
  {
    NdbDictionary::Table bad(*tab);
    bad.setName("rb_t27_bad");
    bad.setRingIdxColumnNo(cid_no);  // INT key col, but NOT the last PK col
    if (!expect_create_rejected(dict, bad, "idx=non-last-pk", 703)) failed++;
  }
  {
    NdbDictionary::Table bad(*tab);
    bad.setName("rb_t27_bad");
    bad.setRingMetaColumnNo(99);  // out of range
    if (!expect_create_rejected(dict, bad, "meta-out-of-range", 703)) failed++;
  }
  {
    NdbDictionary::Table bad(*tab);
    bad.setName("rb_t27_bad");
    bad.setRingMetaColumnNo(cid_no);  // INT NOT NULL key col
    if (!expect_create_rejected(dict, bad, "meta=int-key-col", 703)) failed++;
  }
  {
    NdbDictionary::Table bad(*tab);
    bad.setName("rb_t27_bad");
    bad.setRingMetaColumnNo(data_no);  // VARCHAR, not VARBINARY
    if (!expect_create_rejected(dict, bad, "meta=varchar-col", 703)) failed++;
  }
  {
    NdbDictionary::Table bad(*tab);
    bad.setName("rb_t27_bad");
    bad.setRingMetaColumnNo(small_no);  // VARBINARY(8) < 32
    if (!expect_create_rejected(dict, bad, "meta=varbinary(8)", 703)) failed++;
  }
  {
    NdbDictionary::Table bad(*tab);
    bad.setName("rb_t27_bad");
    bad.setRingMetaColumnNo(vbnn_no);  // NOT NULL
    if (!expect_create_rejected(dict, bad, "meta=not-null", 703)) failed++;
  }
  TEST_ASSERT(failed == 0, std::to_string(failed) +
                               " invalid ring definitions were accepted");

  // Control: an unmodified copy must still be creatable.
  {
    NdbDictionary::Table ok(*tab);
    ok.setName("rb_t27_ok");
    int rc = dict->createTable(ok);
    TEST_ASSERT(rc == 0, std::string("control copy create failed: ") +
                             dict->getNdbError().message);
    const NdbDictionary::Table *chk = dict->getTable("rb_t27_ok");
    TEST_ASSERT(chk != nullptr && chk->isRingBuffer(),
                "control copy is a ring buffer");
    TEST_ASSERT(dict->dropTable("rb_t27_ok") == 0, "drop control copy");
  }

  mysql_exec(mysql, "DROP TABLE test.rb_t27");
  TEST_PASS("DICT validates ring metadata on create");
  return true;
}

/*
 * Test 28: DICT rules for TTL / fully-replicated + ring buffer tables.
 * TTL on a ring buffer table is allowed (DBTUP never expires the meta row
 * and lets the TTL purge's only-expired deletes through the ring write
 * guard); the SQL CREATE of rb_t28_ttl below proves DICT accepts it. A
 * fully-replicated ring table stays rejected: its copy triggers carry no
 * ring-buffer flag. TTL on a ring table is a creation-time property: the
 * TTL seconds may change, but alterTable may neither enable nor disable TTL
 * on an existing ring table (741 UnsupportedChange). The MySQL handler
 * rejects that too; the raw NDB API reaches DICT directly, so DICT must.
 */
static bool test_dict_rejects_ttl_and_fr_combo(Ndb *ndb, MYSQL *mysql) {
  std::cout << "[Test 28] DICT rules for TTL / fully-replicated + ring combos"
            << std::endl;

  mysql_exec(mysql, "DROP TABLE IF EXISTS test.rb_t28");
  mysql_exec(mysql, "DROP TABLE IF EXISTS test.rb_t28_ttl");
  mysql_exec(mysql,
             "CREATE TABLE test.rb_t28 ("
             "  client_id INT NOT NULL,"
             "  ring_idx INT NOT NULL DEFAULT 0,"
             "  ring_meta VARBINARY(64),"
             "  ts DATETIME,"
             "  PRIMARY KEY (client_id, ring_idx)"
             ") ENGINE=NDB,"
             "  COMMENT='NDB_TABLE=MAX_ROWS_PER_PK=5@ring_idx@ring_meta'");
  mysql_exec(mysql,
             "CREATE TABLE test.rb_t28_ttl ("
             "  client_id INT NOT NULL,"
             "  ring_idx INT NOT NULL DEFAULT 0,"
             "  ring_meta VARBINARY(64),"
             "  ts DATETIME,"
             "  PRIMARY KEY (client_id, ring_idx)"
             ") ENGINE=NDB,"
             "  COMMENT='NDB_TABLE=TTL=60@ts,"
             "MAX_ROWS_PER_PK=5@ring_idx@ring_meta'");

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("rb_t28");
  dict->invalidateTable("rb_t28_ttl");
  const NdbDictionary::Table *tab = dict->getTable("rb_t28");
  TEST_ASSERT(tab != nullptr, "getTable");
  TEST_ASSERT(tab->isRingBuffer(), "should be ring buffer");
  TEST_ASSERT(!tab->isTTLEnabled(), "rb_t28 has no TTL");
  const NdbDictionary::Table *ttl_tab = dict->getTable("rb_t28_ttl");
  TEST_ASSERT(ttl_tab != nullptr, "getTable (ttl ring)");
  TEST_ASSERT(ttl_tab->isRingBuffer() && ttl_tab->isTTLEnabled(),
              "rb_t28_ttl should be a TTL ring buffer table");
  const int ts_no = tab->getColumn("ts")->getColumnNo();

  int failed = 0;
  {
    NdbDictionary::Table bad(*tab);
    bad.setName("rb_t28_bad");
    bad.setFullyReplicated(true);
    if (!expect_create_rejected(dict, bad, "fully-replicated+ring create",
                                703))
      failed++;
  }

  // Helper: an alterTable that must be rejected with the given code and
  // must leave the table's TTL state unchanged. Counted like the create
  // sub-case so one red run shows every outcome.
  auto expect_alter_rejected = [&](const char *tabname,
                                   const NdbDictionary::Table *old,
                                   const NdbDictionary::Table &newTab,
                                   const char *what, int expected_code,
                                   bool ttl_after) {
    int rc = dict->alterTable(*old, newTab);
    std::cerr << "  (" << what << ": rc=" << rc << " err="
              << dict->getNdbError().code << " "
              << dict->getNdbError().message << ")" << std::endl;
    if (rc == 0) {
      std::cerr << "  SUB-FAIL(" << what << "): alterTable accepted"
                << std::endl;
      failed++;
      return;
    }
    if (dict->getNdbError().code != expected_code) {
      std::cerr << "  SUB-FAIL(" << what << "): expected error "
                << expected_code << std::endl;
      failed++;
    }
    dict->invalidateTable(tabname);
    const NdbDictionary::Table *chk = dict->getTable(tabname);
    if (chk == nullptr || chk->isTTLEnabled() != ttl_after) {
      std::cerr << "  SUB-FAIL(" << what << "): TTL state changed after "
                   "rejected alter" << std::endl;
      failed++;
    }
  };

  // Enabling TTL on an existing ring buffer table: rejected
  {
    NdbDictionary::Table newTab(*tab);
    newTab.setTTLSec(60);
    newTab.setTTLColumnNo(ts_no);
    expect_alter_rejected("rb_t28", tab, newTab, "alter enable ttl on ring",
                          741, false);
  }
  // Disabling TTL on an existing TTL ring buffer table: rejected
  {
    NdbDictionary::Table newTab(*ttl_tab);
    newTab.setTTLSec(RNIL);
    newTab.setTTLColumnNo(RNIL);
    expect_alter_rejected("rb_t28_ttl", ttl_tab, newTab,
                          "alter disable ttl on ring", 741, true);
  }
  TEST_ASSERT(failed == 0, std::to_string(failed) +
                               " DICT TTL/ring rule sub-cases failed");

  mysql_exec(mysql, "DROP TABLE test.rb_t28");
  mysql_exec(mysql, "DROP TABLE test.rb_t28_ttl");
  TEST_PASS("DICT rules for TTL / fully-replicated + ring combos");
  return true;
}

/*
 * Test 29 (B18 DICT backstop): add-fragment / reorganize ALTER on a ring
 * buffer table must be rejected in DICT. The SQL layer already rejects
 * inplace REORGANIZE/ADD PARTITION, but the raw NDB API (setFragmentCount +
 * alterTable) reaches DICT directly; reorg copy writes and reorg triggers
 * carry no ring-buffer flag (940 mid-schema-transaction) and the reorg scan
 * would silently drop meta rows. The table is kept EMPTY so that before the
 * fix the alter deterministically SUCCEEDS (nothing to move - with data the
 * outcome depends on which prefixes hash to moved fragments).
 */
static bool test_dict_rejects_add_fragment(Ndb *ndb, MYSQL *mysql) {
  std::cout << "[Test 29] add-fragment ALTER on ring buffer table rejected"
            << std::endl;

  mysql_exec(mysql, "DROP TABLE IF EXISTS test.rb_t29");
  char ddl[1024];
  snprintf(ddl, sizeof(ddl), CREATE_BASIC, "rb_t29", 5);
  mysql_exec(mysql, ddl);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("rb_t29");
  const NdbDictionary::Table *tab = dict->getTable("rb_t29");
  TEST_ASSERT(tab != nullptr, "getTable");
  TEST_ASSERT(tab->isRingBuffer(), "should be ring buffer");
  const Uint32 old_frags = tab->getFragmentCount();

  // Mimic exactly what mysqld's inplace ADD PARTITION sends (explicit
  // fragment count + PartitionBalance_Specific) - a fragment-count change
  // alone is stopped earlier by generic hashmap/balance checks. This is the
  // vector that reached the unguarded reorg machinery in the SQL-layer red
  // (error 940 mid-schema-transaction with enough prefixes to force
  // movement) before the SQL-layer reject was added.
  NdbDictionary::Table newTab(*tab);
  newTab.setFragmentCount(old_frags + 1);
  newTab.setPartitionBalance(
      NdbDictionary::Object::PartitionBalance_Specific);
  int rc = dict->alterTable(*tab, newTab);
  std::cerr << "  (add-fragment alter: rc=" << rc << " err="
            << dict->getNdbError().code << " " << dict->getNdbError().message
            << ")" << std::endl;
  TEST_ASSERT(rc != 0, "add-fragment alterTable must be rejected");
  TEST_ASSERT(dict->getNdbError().code == 741,
              "expected error 741, got " +
                  std::to_string(dict->getNdbError().code));

  // Fragment count unchanged and the ring still works.
  dict->invalidateTable("rb_t29");
  const NdbDictionary::Table *check = dict->getTable("rb_t29");
  TEST_ASSERT(check != nullptr, "re-getTable");
  TEST_ASSERT(check->getFragmentCount() == old_frags,
              "fragment count unchanged after rejected alter");

  BasicRecordHelper h;
  TEST_ASSERT(h.init(check), "init record helper");
  char *rowbuf = h.newRow();
  unsigned char *mask = h.newUserMask(check);
  TEST_ASSERT(insertN(ndb, check, h, rowbuf, mask, 1, "row", 1),
              "insert after rejected alter");
  auto rows = readDataRows(mysql, "rb_t29", 1);
  TEST_ASSERT(rows.size() == 1, "expected 1 row after rejected alter, got " +
                                    std::to_string(rows.size()));

  delete[] rowbuf;
  delete[] mask;
  mysql_exec(mysql, "DROP TABLE test.rb_t29");
  TEST_PASS("add-fragment ALTER on ring buffer table rejected");
  return true;
}

/*
 * Test 30 (B17): corrupt ring_meta must surface an error, not silently
 * re-initialize. Before the fix, a meta row whose ring_meta was NULL, too
 * short, or carried an unknown version was silently treated as "first
 * insert" (init_first_insert): count/total_inserts reset and every
 * existing data row turned into a phantom the ring no longer tracks.
 * After the fix, addRow/deleteOldest fail with error 4357.
 *
 * The corrupt states are created with a raw flagged updateTuple
 * (OO_RING_BUFFER_OP bypasses the DBTUP write guard exactly like the
 * writer's own ops) - the only way such states can arise in practice.
 *
 * Before the fix: addRow succeeds after corruption -> this test FAILS.
 * After the fix:  addRow/deleteOldest fail with 4357 -> this test PASSES.
 */
static bool test_corrupt_meta_rejected(Ndb *ndb, MYSQL *mysql) {
  std::cout << "[Test 30] corrupt ring_meta rejected" << std::endl;
  const int ERR_CORRUPT_RING_META = 4357;

  mysql_exec(mysql, "DROP TABLE IF EXISTS test.rb_t30");
  char ddl[1024];
  snprintf(ddl, sizeof(ddl), CREATE_BASIC, "rb_t30", 3);
  mysql_exec(mysql, ddl);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("rb_t30");
  const NdbDictionary::Table *table = dict->getTable("rb_t30");
  TEST_ASSERT(table != nullptr, "getTable");

  BasicRecordHelper h;
  TEST_ASSERT(h.init(table), "init record helper");
  char *rowbuf = h.newRow();
  unsigned char *mask = h.newUserMask(table);

  // Fill the size-3 ring: meta = (version=1, next_pos=1, count=3, total=3).
  TEST_ASSERT(insertN(ndb, table, h, rowbuf, mask, 1, "row", 3),
              "insert 3 rows");

  // Mask covering only ring_meta, for the raw corrupting updates.
  unsigned char *meta_only_mask = new unsigned char[h.mask_size];
  const char *meta_cols[] = {"ring_meta"};
  buildMask(table, meta_only_mask, h.mask_size, meta_cols, 1);

  // Raw flagged update of the meta row's ring_meta column.
  auto writeRawMeta = [&](const unsigned char *val, Uint32 len,
                          bool set_null) -> bool {
    NdbTransaction *t = ndb->startTransaction(table);
    if (!t) return false;
    h.fillRow(rowbuf, 1, "");  // client_id=1, ring_idx=0 (memset)
    if (set_null) {
      setNull(rowbuf, h.rmeta_attr);
    } else {
      clearNull(rowbuf, h.rmeta_attr);
      setVarbinary(rowbuf, h.rmeta_attr, val, len);
    }
    NdbOperation::OperationOptions opts;
    memset(&opts, 0, sizeof(opts));
    opts.optionsPresent = NdbOperation::OperationOptions::OO_RING_BUFFER_OP;
    const NdbOperation *op =
        t->updateTuple(h.record, rowbuf, h.record, rowbuf, meta_only_mask,
                       &opts, sizeof(opts));
    if (!op) {
      ndb->closeTransaction(t);
      return false;
    }
    int rc = t->execute(NdbTransaction::Commit);
    ndb->closeTransaction(t);
    return rc == 0;
  };

  // addRow on the corrupted table must fail with 4357.
  auto addRowMustFail = [&](const char *tag) -> bool {
    NdbTransaction *t = ndb->startTransaction(table);
    if (!t) return false;
    NdbRingBufferWriter writer(table, h.record, t);
    if (writer.getErrorCode() != 0) {
      ndb->closeTransaction(t);
      return false;
    }
    h.fillRow(rowbuf, 1, tag);
    const NdbOperation *op = writer.addRow(rowbuf, mask);
    int code = writer.getErrorCode();
    std::cerr << "  (" << tag << ": op=" << (op ? "non-null" : "null")
              << " code=" << code << " " << writer.getErrorMessage() << ")"
              << std::endl;
    ndb->closeTransaction(t);  // never commit - abort whatever was queued
    return op == nullptr && code == ERR_CORRUPT_RING_META;
  };

  // (a) Unknown version: well-formed 32 bytes, version=99.
  unsigned char bad_version[32];
  memset(bad_version, 0, sizeof(bad_version));
  int2store(bad_version + 0, 99);  // version
  int4store(bad_version + 4, 1);   // next_pos
  int4store(bad_version + 8, 3);   // count
  int8store(bad_version + 16, 3);  // total_inserts
  TEST_ASSERT(writeRawMeta(bad_version, sizeof(bad_version), false),
              "corrupt meta (version=99)");
  TEST_ASSERT(addRowMustFail("bad_ver"),
              "addRow on version-corrupt meta must fail with 4357");

  // deleteOldest goes through the same meta read - must fail identically.
  {
    NdbTransaction *t = ndb->startTransaction(table);
    TEST_ASSERT(t != nullptr, "startTransaction (deleteOldest)");
    NdbRingBufferWriter writer(table, h.record, t);
    TEST_ASSERT(writer.getErrorCode() == 0, "writer init (deleteOldest)");
    h.fillRow(rowbuf, 1, "");
    Uint32 actual = 0xdeadbeef;
    int rc = writer.deleteOldest(rowbuf, 2, &actual);
    std::cerr << "  (deleteOldest: rc=" << rc << " code="
              << writer.getErrorCode() << ")" << std::endl;
    TEST_ASSERT(rc != 0 && writer.getErrorCode() == ERR_CORRUPT_RING_META,
                "deleteOldest on corrupt meta must fail with 4357");
    ndb->closeTransaction(t);
  }

  // (b) Too-short value (4 bytes).
  unsigned char bad_short[4] = {0xDE, 0xAD, 0xBE, 0xEF};
  TEST_ASSERT(writeRawMeta(bad_short, sizeof(bad_short), false),
              "corrupt meta (short)");
  TEST_ASSERT(addRowMustFail("bad_short"),
              "addRow on short meta must fail with 4357");

  // (c) NULL ring_meta on an existing meta row.
  TEST_ASSERT(writeRawMeta(nullptr, 0, true), "corrupt meta (NULL)");
  TEST_ASSERT(addRowMustFail("bad_null"),
              "addRow on NULL meta must fail with 4357");

  // (d)-(f) Well-formed version 1 values whose fields are out of range for
  // the size-3 ring: next_pos 0 (would write the data row onto the meta
  // row's key), next_pos 4 (a slot outside 1..3), count 4 (more rows than
  // the ring holds). The NDB API writer and a SQL INSERT must both fail.
  struct BadRange {
    const char *tag;
    Uint32 next_pos;
    Uint32 count;
  };
  const BadRange bad_ranges[] = {
      {"bad_next0", 0, 3}, {"bad_next4", 4, 3}, {"bad_count4", 1, 4}};
  for (const BadRange &b : bad_ranges) {
    unsigned char bad_range[32];
    memset(bad_range, 0, sizeof(bad_range));
    int2store(bad_range + 0, 1);  // version
    int4store(bad_range + 4, b.next_pos);
    int4store(bad_range + 8, b.count);
    int8store(bad_range + 16, 3);  // total_inserts
    TEST_ASSERT(writeRawMeta(bad_range, sizeof(bad_range), false),
                std::string("corrupt meta (") + b.tag + ")");
    TEST_ASSERT(addRowMustFail(b.tag),
                std::string("addRow on ") + b.tag + " meta must fail with 4357");
    const int sql_rc = mysql_query(
        mysql, "INSERT INTO test.rb_t30 (client_id, event_data) "
               "VALUES (1, 'sql_bad_range')");
    std::cerr << "  (" << b.tag << " SQL INSERT: rc=" << sql_rc << " "
              << mysql_error(mysql) << ")" << std::endl;
    TEST_ASSERT(sql_rc != 0 && strstr(mysql_error(mysql), "4357") != nullptr,
                std::string("SQL INSERT on ") + b.tag +
                    " meta must fail with 4357");
  }

  // The data rows themselves must be untouched by all of the above.
  auto rows = readDataRows(mysql, "rb_t30", 1);
  TEST_ASSERT(rows.size() == 3,
              "expected 3 untouched data rows, got " +
                  std::to_string(rows.size()));
  TEST_ASSERT(rows[0].data == "row_0" && rows[1].data == "row_1" &&
                  rows[2].data == "row_2",
              "data rows unchanged");

  delete[] rowbuf;
  delete[] mask;
  delete[] meta_only_mask;
  mysql_exec(mysql, "DROP TABLE test.rb_t30");
  TEST_PASS("corrupt ring_meta rejected");
  return true;
}

/*
 * Test 31: deleteOldest edge coverage - maxN==0 no-op, auto-flush of a
 * pending addRow batch, and a ring_size=1 table.
 */
static bool test_delete_oldest_edges(Ndb *ndb, MYSQL *mysql) {
  std::cout << "[Test 31] deleteOldest - maxN=0, auto-flush, ring_size=1"
            << std::endl;

  mysql_exec(mysql, "DROP TABLE IF EXISTS test.rb_t31");
  char ddl[1024];
  snprintf(ddl, sizeof(ddl), CREATE_BASIC, "rb_t31", 5);
  mysql_exec(mysql, ddl);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("rb_t31");
  const NdbDictionary::Table *table = dict->getTable("rb_t31");
  TEST_ASSERT(table != nullptr, "getTable");

  BasicRecordHelper h;
  TEST_ASSERT(h.init(table), "init record helper");
  char *rowbuf = h.newRow();
  unsigned char *mask = h.newUserMask(table);

  TEST_ASSERT(insertN(ndb, table, h, rowbuf, mask, 1, "row", 3),
              "insert 3 rows");

  // (a) maxN == 0 is a no-op success: rc=0, outActual=0, nothing deleted.
  {
    NdbTransaction *trans = ndb->startTransaction(table);
    TEST_ASSERT(trans != nullptr, "startTransaction");
    NdbRingBufferWriter writer(table, h.record, trans);
    TEST_ASSERT(writer.getErrorCode() == 0, "writer init");
    h.fillRow(rowbuf, 1, "");
    Uint32 actual = 0xdeadbeef;
    int rc = writer.deleteOldest(rowbuf, 0, &actual);
    TEST_ASSERT(rc == 0,
                std::string("deleteOldest(0): ") + writer.getErrorMessage());
    TEST_ASSERT(actual == 0, "outActual must be 0 for maxN=0");
    TEST_ASSERT(trans->execute(NdbTransaction::Commit) == 0, "commit");
    ndb->closeTransaction(trans);
  }
  {
    auto rows = readDataRows(mysql, "rb_t31", 1);
    TEST_ASSERT(rows.size() == 3, "maxN=0 must not delete anything");
  }

  // (b) deleteOldest auto-flushes a pending addRow batch on the same
  // writer: the pending row lands at slot 4, then the 2 oldest
  // (slots 1,2) are deleted in the same transaction.
  {
    NdbTransaction *trans = ndb->startTransaction(table);
    TEST_ASSERT(trans != nullptr, "startTransaction");
    NdbRingBufferWriter writer(table, h.record, trans);
    TEST_ASSERT(writer.getErrorCode() == 0, "writer init");

    h.fillRow(rowbuf, 1, "pend_0");
    TEST_ASSERT(writer.addRow(rowbuf, mask) != nullptr,
                std::string("addRow: ") + writer.getErrorMessage());

    // NO explicit flush - deleteOldest must flush the batch itself.
    h.fillRow(rowbuf, 1, "");
    Uint32 actual = 0;
    int rc = writer.deleteOldest(rowbuf, 2, &actual);
    TEST_ASSERT(rc == 0,
                std::string("deleteOldest(2): ") + writer.getErrorMessage());
    TEST_ASSERT(actual == 2, "expected 2 deleted, got " +
                                 std::to_string(actual));
    TEST_ASSERT(trans->execute(NdbTransaction::Commit) == 0, "commit");
    ndb->closeTransaction(trans);
  }
  {
    auto rows = readDataRows(mysql, "rb_t31", 1);
    TEST_ASSERT(rows.size() == 2,
                "expected 2 rows, got " + std::to_string(rows.size()));
    TEST_ASSERT(rows[0].ring_idx == 3 && rows[0].data == "row_2",
                "slot 3 = row_2 (survivor)");
    TEST_ASSERT(rows[1].ring_idx == 4 && rows[1].data == "pend_0",
                "slot 4 = pend_0 (auto-flushed pending row)");
  }
  mysql_exec(mysql, "DROP TABLE test.rb_t31");

  // (c) ring_size=1: wrap, drain via deleteOldest, refill.
  mysql_exec(mysql, "DROP TABLE IF EXISTS test.rb_t31b");
  snprintf(ddl, sizeof(ddl), CREATE_BASIC, "rb_t31b", 1);
  mysql_exec(mysql, ddl);
  dict->invalidateTable("rb_t31b");
  const NdbDictionary::Table *t1 = dict->getTable("rb_t31b");
  TEST_ASSERT(t1 != nullptr, "getTable rb_t31b");

  BasicRecordHelper h1;
  TEST_ASSERT(h1.init(t1), "init record helper (size 1)");
  char *rowbuf1 = h1.newRow();
  unsigned char *mask1 = h1.newUserMask(t1);

  TEST_ASSERT(insertN(ndb, t1, h1, rowbuf1, mask1, 1, "row", 3),
              "insert 3 rows into size-1 ring");
  {
    auto rows = readDataRows(mysql, "rb_t31b", 1);
    TEST_ASSERT(rows.size() == 1 && rows[0].ring_idx == 1 &&
                    rows[0].data == "row_2",
                "size-1 ring holds only the last row");
  }
  {
    NdbTransaction *trans = ndb->startTransaction(t1);
    TEST_ASSERT(trans != nullptr, "startTransaction");
    NdbRingBufferWriter writer(t1, h1.record, trans);
    h1.fillRow(rowbuf1, 1, "");
    Uint32 actual = 0;
    int rc = writer.deleteOldest(rowbuf1, 10, &actual);
    TEST_ASSERT(rc == 0,
                std::string("deleteOldest: ") + writer.getErrorMessage());
    TEST_ASSERT(actual == 1, "expected 1 deleted from size-1 ring");
    TEST_ASSERT(trans->execute(NdbTransaction::Commit) == 0, "commit");
    ndb->closeTransaction(trans);
  }
  {
    auto rows = readDataRows(mysql, "rb_t31b", 1);
    TEST_ASSERT(rows.size() == 0, "size-1 ring drained");
  }
  TEST_ASSERT(insertN(ndb, t1, h1, rowbuf1, mask1, 1, "re", 1),
              "refill size-1 ring");
  {
    auto rows = readDataRows(mysql, "rb_t31b", 1);
    TEST_ASSERT(rows.size() == 1 && rows[0].ring_idx == 1 &&
                    rows[0].data == "re_0",
                "size-1 ring refilled at slot 1");
  }

  // SQL INSERT into a ring that deleteOldest has operated on - the two
  // writers share the same meta formulas; pin that they compose.
  mysql_exec(mysql,
             "INSERT INTO test.rb_t31b (client_id, event_data) "
             "VALUES (1, 'sql_after')");
  {
    auto rows = readDataRows(mysql, "rb_t31b", 1);
    TEST_ASSERT(rows.size() == 1 && rows[0].ring_idx == 1 &&
                    rows[0].data == "sql_after",
                "SQL INSERT overwrote slot 1 of the size-1 ring");
  }

  delete[] rowbuf;
  delete[] mask;
  delete[] rowbuf1;
  delete[] mask1;
  mysql_exec(mysql, "DROP TABLE test.rb_t31b");
  TEST_PASS("deleteOldest - maxN=0, auto-flush, ring_size=1");
  return true;
}

/*
 * Test 32: deleteOldest on a table with a TEXT column. NdbRecord
 * deleteTuple links blob handles automatically (NdbTransaction.cpp,
 * RecTableHasBlob), so blob parts are deleted with the row - this pins
 * that the writer's delete path composes with that machinery.
 */
static bool test_delete_oldest_blob(Ndb *ndb, MYSQL *mysql) {
  std::cout << "[Test 32] deleteOldest - blob table" << std::endl;

  mysql_exec(mysql, "DROP TABLE IF EXISTS test.rb_t32");
  mysql_exec(mysql,
             "CREATE TABLE test.rb_t32 ("
             "  client_id INT NOT NULL,"
             "  ring_idx INT NOT NULL DEFAULT 0,"
             "  ring_meta VARBINARY(64),"
             "  content TEXT,"
             "  PRIMARY KEY (client_id, ring_idx)"
             ") ENGINE=NDB,"
             "  COMMENT='NDB_TABLE=MAX_ROWS_PER_PK=3@ring_idx@ring_meta'");

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("rb_t32");
  const NdbDictionary::Table *table = dict->getTable("rb_t32");
  TEST_ASSERT(table != nullptr, "getTable");

  const NdbRecord *record = table->getDefaultRecord();
  TEST_ASSERT(record != nullptr, "getDefaultRecord");
  const NdbRecord::Attr *cid_attr = findAttr(record, table, "client_id");
  const NdbRecord::Attr *rmeta_attr = findAttr(record, table, "ring_meta");
  TEST_ASSERT(cid_attr && rmeta_attr, "find attrs");

  Uint32 row_size = record->m_row_size;
  char *rowbuf = new char[row_size];
  Uint32 max_attr = 0;
  for (Uint32 i = 0; i < record->noOfColumns; i++)
    if (record->columns[i].attrId > max_attr)
      max_attr = record->columns[i].attrId;
  Uint32 mask_size = (max_attr / 8) + 1;
  unsigned char *mask = new unsigned char[mask_size];
  const char *cols[] = {"client_id", "content"};
  buildMask(table, mask, mask_size, cols, 2);

  // Insert 3 rows with TEXT payloads (use a payload > 256 bytes so it
  // spills into the blob parts table, not just the inline head).
  std::string big(500, 'x');
  const std::string texts[] = {"small_a", big, "small_c"};

  {
    NdbTransaction *trans = ndb->startTransaction(table);
    TEST_ASSERT(trans != nullptr, "startTransaction");
    NdbRingBufferWriter writer(table, record, trans);
    TEST_ASSERT(writer.getErrorCode() == 0, "writer init");
    for (int i = 0; i < 3; i++) {
      memset(rowbuf, 0, row_size);
      setInt32(rowbuf, cid_attr, 1);
      setNull(rowbuf, rmeta_attr);
      const NdbOperation *op = writer.addRow(rowbuf, mask);
      TEST_ASSERT(op != nullptr,
                  std::string("addRow ") + std::to_string(i) + ": " +
                      writer.getErrorMessage());
      NdbBlob *blob = op->getBlobHandle("content");
      TEST_ASSERT(blob != nullptr, "getBlobHandle");
      TEST_ASSERT(blob->setValue(texts[i].c_str(), texts[i].size()) == 0,
                  "blob setValue");
    }
    TEST_ASSERT(writer.flush() == 0,
                std::string("flush: ") + writer.getErrorMessage());
    TEST_ASSERT(trans->execute(NdbTransaction::Commit) == 0, "commit");
    ndb->closeTransaction(trans);
  }

  // Delete the 2 oldest (slots 1,2 - including the 500-byte blob row).
  {
    NdbTransaction *trans = ndb->startTransaction(table);
    TEST_ASSERT(trans != nullptr, "startTransaction");
    NdbRingBufferWriter writer(table, record, trans);
    TEST_ASSERT(writer.getErrorCode() == 0, "writer init");
    memset(rowbuf, 0, row_size);
    setInt32(rowbuf, cid_attr, 1);
    Uint32 actual = 0;
    int rc = writer.deleteOldest(rowbuf, 2, &actual);
    TEST_ASSERT(rc == 0,
                std::string("deleteOldest: ") + writer.getErrorMessage());
    TEST_ASSERT(actual == 2, "expected 2 deleted, got " +
                                 std::to_string(actual));
    TEST_ASSERT(trans->execute(NdbTransaction::Commit) == 0, "commit");
    ndb->closeTransaction(trans);
  }

  // Survivor: slot 3 = small_c. The deleted blob row must be fully gone.
  mysql_exec(mysql,
             "SELECT ring_idx, content FROM test.rb_t32 "
             "WHERE client_id=1 AND ring_idx>0 ORDER BY ring_idx");
  MYSQL_RES *res = mysql_store_result(mysql);
  int nrows = (int)mysql_num_rows(res);
  TEST_ASSERT(nrows == 1, "expected 1 row, got " + std::to_string(nrows));
  MYSQL_ROW r = mysql_fetch_row(res);
  TEST_ASSERT(r && atoi(r[0]) == 3 && r[1] && std::string(r[1]) == "small_c",
              "survivor is slot 3 / small_c");
  mysql_free_result(res);

  // Refill works and lands in the freed slots.
  {
    NdbTransaction *trans = ndb->startTransaction(table);
    TEST_ASSERT(trans != nullptr, "startTransaction");
    NdbRingBufferWriter writer(table, record, trans);
    memset(rowbuf, 0, row_size);
    setInt32(rowbuf, cid_attr, 1);
    setNull(rowbuf, rmeta_attr);
    const NdbOperation *op = writer.addRow(rowbuf, mask);
    TEST_ASSERT(op != nullptr,
                std::string("addRow refill: ") + writer.getErrorMessage());
    NdbBlob *blob = op->getBlobHandle("content");
    TEST_ASSERT(blob != nullptr, "getBlobHandle refill");
    TEST_ASSERT(blob->setValue("refill", 6) == 0, "blob setValue refill");
    TEST_ASSERT(writer.flush() == 0,
                std::string("flush: ") + writer.getErrorMessage());
    TEST_ASSERT(trans->execute(NdbTransaction::Commit) == 0, "commit");
    ndb->closeTransaction(trans);
  }
  mysql_exec(mysql,
             "SELECT ring_idx, content FROM test.rb_t32 "
             "WHERE client_id=1 AND ring_idx>0 ORDER BY ring_idx");
  res = mysql_store_result(mysql);
  nrows = (int)mysql_num_rows(res);
  TEST_ASSERT(nrows == 2, "expected 2 rows after refill, got " +
                              std::to_string(nrows));
  r = mysql_fetch_row(res);
  TEST_ASSERT(r && atoi(r[0]) == 1 && r[1] && std::string(r[1]) == "refill",
              "refill landed at slot 1");
  mysql_free_result(res);

  delete[] rowbuf;
  delete[] mask;
  mysql_exec(mysql, "DROP TABLE test.rb_t32");
  TEST_PASS("deleteOldest - blob table");
  return true;
}

// ---------------------------------------------------------------
// TTL ring buffer tables
// ---------------------------------------------------------------

/*
 * TTL ring buffer table: ring of %d slots, TTL 3600 s on the nullable
 * TIMESTAMP column ts. Rows written through BasicRecordHelper leave ts
 * NULL, which never expires.
 */
static const char *CREATE_TTL_RING =
    "CREATE TABLE test.%s ("
    "  client_id INT NOT NULL,"
    "  ring_idx INT NOT NULL DEFAULT 0,"
    "  ring_meta VARBINARY(64),"
    "  event_data VARCHAR(100),"
    "  ts TIMESTAMP NULL,"
    "  PRIMARY KEY (client_id, ring_idx)"
    ") ENGINE=NDB,"
    "  COMMENT='NDB_TABLE=TTL=3600@ts,"
    "MAX_ROWS_PER_PK=%d@ring_idx@ring_meta'";

/*
 * SQL helper: first column of the first row of a query, "" if none.
 */
static std::string sqlScalar(MYSQL *mysql, const char *sql) {
  mysql_exec(mysql, sql);
  MYSQL_RES *res = mysql_store_result(mysql);
  std::string val;
  if (res) {
    MYSQL_ROW r = mysql_fetch_row(res);
    if (r && r[0]) val = r[0];
    mysql_free_result(res);
  }
  return val;
}

/*
 * SQL helper: the meta row's TTL column of one PK prefix, read with
 * show_meta in UTC. "" if the meta row is not visible.
 */
static std::string readMetaTs(MYSQL *mysql, const char *tbl, int cid) {
  mysql_exec(mysql, "SET time_zone='+00:00'");
  mysql_exec(mysql, "SET ndb_ring_buffer_show_meta=1");
  char sql[256];
  snprintf(sql, sizeof(sql),
           "SELECT ts FROM test.%s WHERE client_id=%d AND ring_idx=0", tbl,
           cid);
  std::string val = sqlScalar(mysql, sql);
  mysql_exec(mysql, "SET ndb_ring_buffer_show_meta=0");
  return val;
}

/*
 * Test 33: TTL ring buffer table through the writer. The meta row's TTL
 * column holds the TIMESTAMP maximum (W1: its ordered-index entry sorts
 * past every purge range); deleteOldest is rejected with 4358 and leaves
 * the table untouched; the writer keeps working afterwards.
 */
static bool test_ttl_ring_writer_and_delete_oldest(Ndb *ndb, MYSQL *mysql) {
  std::cout << "[Test 33] TTL ring - meta TTL column = max, deleteOldest "
               "rejected"
            << std::endl;

  mysql_exec(mysql, "DROP TABLE IF EXISTS test.rb_t33");
  char ddl[1024];
  snprintf(ddl, sizeof(ddl), CREATE_TTL_RING, "rb_t33", 5);
  mysql_exec(mysql, ddl);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("rb_t33");
  const NdbDictionary::Table *table = dict->getTable("rb_t33");
  TEST_ASSERT(table != nullptr, "getTable");
  TEST_ASSERT(table->isRingBuffer() && table->isTTLEnabled(),
              "should be a TTL ring buffer table");

  BasicRecordHelper h;
  TEST_ASSERT(h.init(table), "init record helper");
  char *rowbuf = h.newRow();
  unsigned char *mask = h.newUserMask(table);

  TEST_ASSERT(insertN(ndb, table, h, rowbuf, mask, 1, "row", 3),
              "insert 3 rows");
  auto rows = readDataRows(mysql, "rb_t33", 1);
  TEST_ASSERT(rows.size() == 3,
              "expected 3 rows, got " + std::to_string(rows.size()));

  std::string meta_ts = readMetaTs(mysql, "rb_t33", 1);
  TEST_ASSERT(meta_ts == "2038-01-19 03:14:07",
              "meta row TTL column should be the TIMESTAMP maximum, got '" +
                  meta_ts + "'");

  {
    NdbTransaction *trans = ndb->startTransaction(table);
    TEST_ASSERT(trans != nullptr, "startTransaction");
    NdbRingBufferWriter writer(table, h.record, trans);
    TEST_ASSERT(writer.getErrorCode() == 0, "writer init");
    h.fillRow(rowbuf, 1, "");
    Uint32 actual = 0xdeadbeef;
    int rc = writer.deleteOldest(rowbuf, 1, &actual);
    std::cerr << "  (deleteOldest on TTL ring: rc=" << rc << " err="
              << writer.getErrorCode() << " " << writer.getErrorMessage()
              << ")" << std::endl;
    TEST_ASSERT(rc == -1, "deleteOldest must fail on a TTL ring table");
    TEST_ASSERT(writer.getErrorCode() == 4358,
                "expected error 4358, got " +
                    std::to_string(writer.getErrorCode()));
    TEST_ASSERT(actual == 0, "outActual must be 0 on rejection");
    ndb->closeTransaction(trans);
  }
  rows = readDataRows(mysql, "rb_t33", 1);
  TEST_ASSERT(rows.size() == 3, "rows untouched by the rejected call");

  TEST_ASSERT(insertN(ndb, table, h, rowbuf, mask, 1, "more", 1),
              "insert after rejected deleteOldest");
  rows = readDataRows(mysql, "rb_t33", 1);
  TEST_ASSERT(rows.size() == 4,
              "expected 4 rows, got " + std::to_string(rows.size()));
  TEST_ASSERT(metaFieldLE(readMetaHex(mysql, "rb_t33", 1), 16, 8) == 4,
              "total_inserts should be 4");

  delete[] rowbuf;
  delete[] mask;
  mysql_exec(mysql, "DROP TABLE test.rb_t33");
  TEST_PASS("TTL ring - meta TTL column = max, deleteOldest rejected");
  return true;
}

/*
 * Test 34 (K1): a ring meta row never expires, whatever its TTL column
 * holds. The meta row's ts is rewritten to the zero TIMESTAMP, which
 * checkTTL reads as expired on a data row (the state of meta rows written
 * before the max fill). Read path: show_meta still sees the meta row.
 * Update path: the next insert continues the ring (total_inserts 2)
 * instead of re-initialising it - without K1 the writer's meta read got
 * 626, the re-insert was converted by DBACC into a TTL upsert and the meta
 * row was silently reset to total_inserts 1.
 */
static bool test_ttl_ring_meta_never_expires(Ndb *ndb, MYSQL *mysql) {
  std::cout << "[Test 34] TTL ring - meta row never expires (zero TTL column)"
            << std::endl;

  mysql_exec(mysql, "DROP TABLE IF EXISTS test.rb_t34");
  char ddl[1024];
  snprintf(ddl, sizeof(ddl), CREATE_TTL_RING, "rb_t34", 5);
  mysql_exec(mysql, ddl);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("rb_t34");
  const NdbDictionary::Table *table = dict->getTable("rb_t34");
  TEST_ASSERT(table != nullptr, "getTable");

  BasicRecordHelper h;
  TEST_ASSERT(h.init(table), "init record helper");
  char *rowbuf = h.newRow();
  unsigned char *mask = h.newUserMask(table);
  const NdbRecord::Attr *ridx = findAttr(h.record, table, "ring_idx");
  const NdbRecord::Attr *ts = findAttr(h.record, table, "ts");
  TEST_ASSERT(ridx != nullptr && ts != nullptr, "ring_idx / ts attrs");

  TEST_ASSERT(insertN(ndb, table, h, rowbuf, mask, 1, "row", 1),
              "insert 1 row");
  TEST_ASSERT(readMetaTs(mysql, "rb_t34", 1) == "2038-01-19 03:14:07",
              "meta row TTL column should start at the maximum");

  // Raw update of the meta row (ring_idx = 0) with the ring-buffer flag:
  // ts := zero TIMESTAMP (4 zero bytes, not NULL).
  {
    h.fillRow(rowbuf, 1, "");
    setInt32(rowbuf, ridx, 0);
    clearNull(rowbuf, ts);
    memset(rowbuf + ts->offset, 0, ts->maxSize);
    unsigned char *ts_mask = new unsigned char[h.mask_size];
    const char *cols[] = {"ts"};
    buildMask(table, ts_mask, h.mask_size, cols, 1);

    NdbTransaction *trans = ndb->startTransaction(table);
    TEST_ASSERT(trans != nullptr, "startTransaction");
    NdbOperation::OperationOptions opts;
    memset(&opts, 0, sizeof(opts));
    opts.optionsPresent = NdbOperation::OperationOptions::OO_RING_BUFFER_OP;
    const NdbOperation *op = trans->updateTuple(
        h.record, rowbuf, h.record, rowbuf, ts_mask, &opts, sizeof(opts));
    TEST_ASSERT(op != nullptr, "updateTuple define (meta ts)");
    int rc = trans->execute(NdbTransaction::Commit);
    TEST_ASSERT(rc == 0, std::string("meta ts update: ") +
                             trans->getNdbError().message);
    ndb->closeTransaction(trans);
    delete[] ts_mask;
  }

  // Read path: the meta row stays visible to show_meta
  std::string meta_ts = readMetaTs(mysql, "rb_t34", 1);
  std::cerr << "  (meta ts after zero fill: '" << meta_ts << "')"
            << std::endl;
  TEST_ASSERT(!meta_ts.empty(),
              "meta row with a zero TTL column must stay visible");
  TEST_ASSERT(meta_ts != "2038-01-19 03:14:07",
              "meta row TTL column should have been zeroed");

  // Update path: the writer sees the meta row and continues the ring
  TEST_ASSERT(insertN(ndb, table, h, rowbuf, mask, 1, "row", 1),
              "insert after zeroing the meta TTL column");
  std::string hex = readMetaHex(mysql, "rb_t34", 1);
  TEST_ASSERT(metaFieldLE(hex, 16, 8) == 2,
              "total_inserts should be 2 (ring continued), got " +
                  std::to_string(metaFieldLE(hex, 16, 8)));
  TEST_ASSERT(metaFieldLE(hex, 8, 4) == 2, "count should be 2");
  auto rows = readDataRows(mysql, "rb_t34", 1);
  TEST_ASSERT(rows.size() == 2 && rows[1].ring_idx == 2,
              "second row should sit in slot 2");
  // The writer's meta update restored the maximum
  TEST_ASSERT(readMetaTs(mysql, "rb_t34", 1) == "2038-01-19 03:14:07",
              "meta row TTL column should be back at the maximum");

  delete[] rowbuf;
  delete[] mask;
  mysql_exec(mysql, "DROP TABLE test.rb_t34");
  TEST_PASS("TTL ring - meta row never expires (zero TTL column)");
  return true;
}

/*
 * Test 35 (K2): an only-expired delete (what the TTL purge issues) passes
 * the ring write guard without the ring-buffer flag, but only for an
 * expired row: on a live row and on the meta row it is not-found (626),
 * a plain delete without any flag is still blocked (940), and the pair
 * only-expired + ignore-TTL is rejected by the API (4360). The meta row is
 * untouched by the purge-style delete and the ring continues past the
 * hole.
 */
static bool test_ttl_ring_only_expired_delete(Ndb *ndb, MYSQL *mysql) {
  std::cout << "[Test 35] TTL ring - only-expired delete passes the ring "
               "write guard"
            << std::endl;

  mysql_exec(mysql, "DROP TABLE IF EXISTS test.rb_t35");
  char ddl[1024];
  snprintf(ddl, sizeof(ddl), CREATE_TTL_RING, "rb_t35", 5);
  mysql_exec(mysql, ddl);

  // Slot 1 expired (2 h old, TTL 3600 s), slot 2 live - through the SQL
  // ring writer.
  mysql_exec(mysql,
             "INSERT INTO test.rb_t35 (client_id, event_data, ts) VALUES "
             "(1, 'old', DATE_SUB(NOW(), INTERVAL 2 HOUR))");
  mysql_exec(mysql,
             "INSERT INTO test.rb_t35 (client_id, event_data, ts) VALUES "
             "(1, 'live', NOW())");
  auto rows = readDataRows(mysql, "rb_t35", 1);
  TEST_ASSERT(rows.size() == 1 && rows[0].ring_idx == 2 &&
                  rows[0].data == "live",
              "only the live row (slot 2) should be visible");
  const std::string hex_before = readMetaHex(mysql, "rb_t35", 1);
  TEST_ASSERT(metaFieldLE(hex_before, 8, 4) == 2 &&
                  metaFieldLE(hex_before, 16, 8) == 2,
              "meta count/total_inserts should be 2");

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("rb_t35");
  const NdbDictionary::Table *table = dict->getTable("rb_t35");
  TEST_ASSERT(table != nullptr, "getTable");
  BasicRecordHelper h;
  TEST_ASSERT(h.init(table), "init record helper");
  char *rowbuf = h.newRow();
  const NdbRecord::Attr *ridx = findAttr(h.record, table, "ring_idx");
  TEST_ASSERT(ridx != nullptr, "ring_idx attr");

  NdbOperation::OperationOptions only_expired;
  memset(&only_expired, 0, sizeof(only_expired));
  only_expired.optionsPresent =
      NdbOperation::OperationOptions::OO_TTL_ONLY_EXPIRED;

  // Issue one NdbRecord deleteTuple of (1, slot) and return the error code
  // (0 on success).
  auto delete_slot = [&](Uint32 slot,
                         const NdbOperation::OperationOptions *opts,
                         const char *what) -> int {
    h.fillRow(rowbuf, 1, "");
    setInt32(rowbuf, ridx, slot);
    NdbTransaction *trans = ndb->startTransaction(table);
    if (!trans) return -1;
    const NdbOperation *op =
        trans->deleteTuple(h.record, rowbuf, h.record, nullptr, nullptr, opts,
                           opts ? sizeof(*opts) : 0);
    if (!op) {
      int e = trans->getNdbError().code;
      ndb->closeTransaction(trans);
      return e ? e : -1;
    }
    int rc = trans->execute(NdbTransaction::Commit);
    int err = rc == 0 ? 0 : trans->getNdbError().code;
    std::cerr << "  (" << what << ": rc=" << rc << " err=" << err << ")"
              << std::endl;
    ndb->closeTransaction(trans);
    return err;
  };

  // (a) expired slot 1: the purge-style delete passes the guard and deletes
  int err = delete_slot(1, &only_expired, "only-expired delete, expired row");
  TEST_ASSERT(err == 0,
              "only-expired delete of an expired ring row must succeed, got " +
                  std::to_string(err));
  // (a2) ... and the row is physically gone
  err = delete_slot(1, &only_expired, "only-expired delete, deleted row");
  TEST_ASSERT(err == 626, "second delete of slot 1 should be not-found (626), got " +
                              std::to_string(err));
  // (b) live slot 2: out of the only-expired scope -> not found, row stays
  err = delete_slot(2, &only_expired, "only-expired delete, live row");
  TEST_ASSERT(err == 626,
              "only-expired delete of a live row should be 626, got " +
                  std::to_string(err));
  // (c) plain delete without any flag: still blocked by the ring guard
  err = delete_slot(2, nullptr, "plain delete, live row");
  TEST_ASSERT(err == 940,
              "plain delete on a ring table should still be 940, got " +
                  std::to_string(err));
  // (d) the meta row (ring_idx 0) is live to an only-expired delete as
  //     well (K1: it never expires): not found, meta row untouched
  err = delete_slot(0, &only_expired, "only-expired delete, meta row");
  TEST_ASSERT(err == 626,
              "only-expired delete of the meta row should be 626, got " +
                  std::to_string(err));
  // (e) only-expired combined with ignore-TTL would skip the expiry check
  //     and the ring write guard: the NDB API rejects the pair (4360) on
  //     a live row and on the meta row alike
  NdbOperation::OperationOptions both_flags;
  memset(&both_flags, 0, sizeof(both_flags));
  both_flags.optionsPresent =
      NdbOperation::OperationOptions::OO_TTL_ONLY_EXPIRED |
      NdbOperation::OperationOptions::OO_TTL_IGNORE;
  err = delete_slot(2, &both_flags, "only-expired + ignore-TTL, live row");
  TEST_ASSERT(err == 4360,
              "only-expired + ignore-TTL delete should be rejected with 4360, "
              "got " + std::to_string(err));
  err = delete_slot(0, &both_flags, "only-expired + ignore-TTL, meta row");
  TEST_ASSERT(err == 4360,
              "only-expired + ignore-TTL delete of the meta row should be "
              "rejected with 4360, got " + std::to_string(err));

  // (f) a row this transaction has already locked is read with TTL ignored
  //     (read what you locked); an only-expired delete of it must still
  //     check expiry: a live row and the meta row stay
  auto lock_then_delete = [&](Uint32 slot, const char *what) -> int {
    h.fillRow(rowbuf, 1, "");
    setInt32(rowbuf, ridx, slot);
    NdbTransaction *trans = ndb->startTransaction(table);
    if (!trans) return -1;
    NdbOperation::OperationOptions show_meta;
    memset(&show_meta, 0, sizeof(show_meta));
    show_meta.optionsPresent =
        NdbOperation::OperationOptions::OO_RING_BUFFER_SHOW_META;
    const NdbOperation *rop =
        trans->readTuple(h.record, rowbuf, h.record, rowbuf,
                         NdbOperation::LM_Exclusive, nullptr, &show_meta,
                         sizeof(show_meta));
    if (!rop || trans->execute(NdbTransaction::NoCommit) != 0) {
      int e = trans->getNdbError().code;
      ndb->closeTransaction(trans);
      return e ? -e : -1;
    }
    const NdbOperation *dop =
        trans->deleteTuple(h.record, rowbuf, h.record, nullptr, nullptr,
                           &only_expired, sizeof(only_expired));
    if (!dop) {
      int e = trans->getNdbError().code;
      ndb->closeTransaction(trans);
      return e ? e : -1;
    }
    int rc = trans->execute(NdbTransaction::Commit);
    int e = rc == 0 ? 0 : trans->getNdbError().code;
    std::cerr << "  (" << what << ": rc=" << rc << " err=" << e << ")"
              << std::endl;
    ndb->closeTransaction(trans);
    return e;
  };
  err = lock_then_delete(2, "lock live row, only-expired delete");
  TEST_ASSERT(err == 626,
              "only-expired delete of a locked live row should be 626, got " +
                  std::to_string(err));
  err = lock_then_delete(0, "lock meta row, only-expired delete");
  TEST_ASSERT(err == 626,
              "only-expired delete of the locked meta row should be 626, got " +
                  std::to_string(err));

  rows = readDataRows(mysql, "rb_t35", 1);
  TEST_ASSERT(rows.size() == 1 && rows[0].ring_idx == 2 &&
                  rows[0].data == "live",
              "the live row must survive");
  TEST_ASSERT(readMetaHex(mysql, "rb_t35", 1) == hex_before,
              "meta row must be untouched by the purge-style delete");

  // The ring continues past the hole: next insert lands in slot 3
  mysql_exec(mysql,
             "INSERT INTO test.rb_t35 (client_id, event_data, ts) VALUES "
             "(1, 'new', NOW())");
  rows = readDataRows(mysql, "rb_t35", 1);
  TEST_ASSERT(rows.size() == 2 && rows[1].ring_idx == 3 &&
                  rows[1].data == "new",
              "new row should land in slot 3");
  std::string hex_after = readMetaHex(mysql, "rb_t35", 1);
  TEST_ASSERT(metaFieldLE(hex_after, 16, 8) == 3 &&
                  metaFieldLE(hex_after, 8, 4) == 3,
              "meta count/total_inserts should be 3 (count is a span)");

  delete[] rowbuf;
  mysql_exec(mysql, "DROP TABLE test.rb_t35");
  TEST_PASS("TTL ring - only-expired delete passes the ring write guard");
  return true;
}

/*
 * Test 36: TTL column precision and the every-column rule. The writer
 * packs the meta row's TIMESTAMP(3) maximum with the column's fraction
 * bytes ('2038-01-19 03:14:07.000'). A slot write onto an occupied slot is
 * an update, so a column absent from the write keeps the overwritten
 * row's value (an expired TTL value hides the new row; a replica whose
 * purge removed the slot cannot rebuild the row): on a TTL table the
 * writer rejects an NdbRecord that lacks a column (4359 at construction)
 * and a row whose mask lacks any column (4359 from addRow), leaving the
 * table untouched.
 */
static bool test_ttl_ring_precision_and_partial_record(Ndb *ndb,
                                                       MYSQL *mysql) {
  std::cout << "[Test 36] TTL ring - TIMESTAMP(3) meta maximum, every column "
               "mandatory"
            << std::endl;

  mysql_exec(mysql, "DROP TABLE IF EXISTS test.rb_t36");
  mysql_exec(mysql,
             "CREATE TABLE test.rb_t36 ("
             "  client_id INT NOT NULL,"
             "  ring_idx INT NOT NULL DEFAULT 0,"
             "  ring_meta VARBINARY(64),"
             "  event_data VARCHAR(100),"
             "  ts TIMESTAMP(3) NULL,"
             "  PRIMARY KEY (client_id, ring_idx)"
             ") ENGINE=NDB,"
             "  COMMENT='NDB_TABLE=TTL=3600@ts,"
             "MAX_ROWS_PER_PK=5@ring_idx@ring_meta'");

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("rb_t36");
  const NdbDictionary::Table *table = dict->getTable("rb_t36");
  TEST_ASSERT(table != nullptr, "getTable");

  // (a) default record, all columns: the meta row's TIMESTAMP(3) maximum
  //     carries the fraction digits
  {
    BasicRecordHelper h;
    TEST_ASSERT(h.init(table), "init record helper");
    char *rowbuf = h.newRow();
    unsigned char *mask = h.newUserMask(table);
    const bool ok = insertN(ndb, table, h, rowbuf, mask, 1, "row", 2);
    delete[] rowbuf;
    delete[] mask;
    TEST_ASSERT(ok, "insert 2 rows");
    const std::string meta_ts = readMetaTs(mysql, "rb_t36", 1);
    TEST_ASSERT(meta_ts == "2038-01-19 03:14:07.000",
                "meta row TIMESTAMP(3) should be the maximum with 3 "
                "fraction digits, got '" +
                    meta_ts + "'");
  }

  // (b) an NdbRecord without the TTL column is rejected by the constructor
  //     (any missing column would be: the write must carry the whole row)
  {
    const char *names[4] = {"client_id", "ring_idx", "ring_meta",
                            "event_data"};
    NdbDictionary::RecordSpecification_v1 spec[4];
    memset(spec, 0, sizeof(spec));
    Uint32 offset = 0;
    Uint32 nullbit = 0;
    const Uint32 null_byte = 1000;  // null bits behind the column data
    for (int i = 0; i < 4; i++) {
      const NdbDictionary::Column *c = table->getColumn(names[i]);
      TEST_ASSERT(c != nullptr, std::string("column ") + names[i]);
      spec[i].column = c;
      spec[i].offset = offset;
      if (c->getNullable()) {
        spec[i].nullbit_byte_offset = null_byte;
        spec[i].nullbit_bit_in_byte = nullbit++;
      }
      offset += ((c->getSizeInBytesForRecord() + 7) / 8) * 8;
    }
    // The v1 specification is selected by its element size
    NdbRecord *rec = dict->createRecord(
        table,
        reinterpret_cast<const NdbDictionary::RecordSpecification *>(spec),
        4, sizeof(NdbDictionary::RecordSpecification_v1));
    TEST_ASSERT(rec != nullptr, std::string("createRecord: ") +
                                    dict->getNdbError().message);
    TEST_ASSERT(findAttr(rec, table, "ts") == nullptr,
                "the partial record must not carry the TTL column");

    NdbTransaction *trans = ndb->startTransaction(table);
    TEST_ASSERT(trans != nullptr, "startTransaction");
    {
      NdbRingBufferWriter writer(table, rec, trans);
      std::cerr << "  (writer on a record without the TTL column: err="
                << writer.getErrorCode() << " " << writer.getErrorMessage()
                << ")" << std::endl;
      TEST_ASSERT(writer.getErrorCode() == 4359,
                  "a record without the TTL column should fail with 4359, got " +
                      std::to_string(writer.getErrorCode()));
    }
    ndb->closeTransaction(trans);
    dict->releaseRecord(rec);
  }

  // (c) the default record, but a row whose mask leaves the TTL column out
  //     (then one that leaves event_data out): rejected by addRow, nothing
  //     written
  {
    BasicRecordHelper h;
    TEST_ASSERT(h.init(table), "init record helper");
    char *rowbuf = h.newRow();
    unsigned char *mask = new unsigned char[h.mask_size];
    const char *cols[] = {"client_id", "event_data"};  // no ts
    buildMask(table, mask, h.mask_size, cols, 2);
    NdbTransaction *trans = ndb->startTransaction(table);
    TEST_ASSERT(trans != nullptr, "startTransaction");
    {
      NdbRingBufferWriter writer(table, h.record, trans);
      TEST_ASSERT(writer.getErrorCode() == 0, "writer init");
      h.fillRow(rowbuf, 1, "no_ts");
      const NdbOperation *op = writer.addRow(rowbuf, mask);
      std::cerr << "  (addRow without the TTL column in the mask: err="
                << writer.getErrorCode() << ")" << std::endl;
      TEST_ASSERT(op == nullptr && writer.getErrorCode() == 4359,
                  "addRow without the TTL column in the mask should fail with "
                  "4359, got " +
                      std::to_string(writer.getErrorCode()));
    }
    ndb->closeTransaction(trans);
    {
      const char *cols_no_data[] = {"client_id", "ts"};  // no event_data
      buildMask(table, mask, h.mask_size, cols_no_data, 2);
      trans = ndb->startTransaction(table);
      TEST_ASSERT(trans != nullptr, "startTransaction");
      NdbRingBufferWriter writer(table, h.record, trans);
      TEST_ASSERT(writer.getErrorCode() == 0, "writer init");
      h.fillRow(rowbuf, 1, "no_data");
      const NdbOperation *op = writer.addRow(rowbuf, mask);
      std::cerr << "  (addRow without event_data in the mask: err="
                << writer.getErrorCode() << " " << writer.getErrorMessage()
                << ")" << std::endl;
      TEST_ASSERT(op == nullptr && writer.getErrorCode() == 4359,
                  "addRow without event_data in the mask should fail with "
                  "4359, got " +
                      std::to_string(writer.getErrorCode()));
      ndb->closeTransaction(trans);
    }
    delete[] rowbuf;
    delete[] mask;
    auto rows = readDataRows(mysql, "rb_t36", 1);
    TEST_ASSERT(rows.size() == 2, "the rejected row must not be written");
    TEST_ASSERT(metaFieldLE(readMetaHex(mysql, "rb_t36", 1), 16, 8) == 2,
                "total_inserts must stay 2");
  }

  mysql_exec(mysql, "DROP TABLE test.rb_t36");
  TEST_PASS("TTL ring - TIMESTAMP(3) meta maximum, every column mandatory");
  return true;
}

int main(int argc, char **argv) {
  if (argc != 4) {
    std::cout << "Usage: ndb_ndbapi_ring_buffer_test <mysql_host> "
                 "<mysql_port> <ndb_connectstring>"
              << std::endl;
    std::cout << "Example: ndb_ndbapi_ring_buffer_test 127.0.0.1 3308 "
                 "127.0.0.1:1188"
              << std::endl;
    return EXIT_FAILURE;
  }

  const char *mysql_host = argv[1];
  unsigned int mysql_port = (unsigned int)atoi(argv[2]);
  const char *connectstring = argv[3];

  ndb_init();

  MYSQL mysql;
  if (!mysql_init(&mysql)) {
    std::cerr << "mysql_init failed" << std::endl;
    return EXIT_FAILURE;
  }
  if (!mysql_real_connect(&mysql, mysql_host, "root", "", "test", mysql_port,
                          nullptr, 0)) {
    std::cerr << "MySQL connect failed: " << mysql_error(&mysql) << std::endl;
    return EXIT_FAILURE;
  }

  // Scope the cluster connection and Ndb so they are destroyed before ndb_end()
  {
    Ndb_cluster_connection cluster_connection(connectstring);
    if (cluster_connection.connect(5, 3, 1)) {
      std::cerr << "Cannot connect to cluster management server" << std::endl;
      return EXIT_FAILURE;
    }
    if (cluster_connection.wait_until_ready(30, 0)) {
      std::cerr << "Cluster was not ready within 30 secs" << std::endl;
      return EXIT_FAILURE;
    }

    Ndb ndb(&cluster_connection, "test");
    if (ndb.init() != 0) {
      std::cerr << "Ndb init failed: " << ndb.getNdbError().message
                << std::endl;
      return EXIT_FAILURE;
    }

    std::cout << "=== NdbRingBufferWriter Test Suite ===" << std::endl;

    test_single_insert(&ndb, &mysql);
    test_fill_ring(&ndb, &mysql);
    test_multiple_prefixes(&ndb, &mysql);
    test_batch_insert(&ndb, &mysql);
    test_notnull_column(&ndb, &mysql);
    test_ring_size_1(&ndb, &mysql);
    test_error_non_ring_table(&ndb, &mysql);
    test_multi_transaction(&ndb, &mysql);
    test_blob_text(&ndb, &mysql);
    test_delete_reinsert(&ndb, &mysql);
    test_rollback(&ndb, &mysql);
    test_multi_wrap_batch(&ndb, &mysql);
    test_multi_col_pk_prefix(&ndb, &mysql);
    test_unique_dup_same_prefix(&ndb, &mysql);
    test_unique_wrap_collision(&ndb, &mysql);
    test_unique_freed_reuse(&ndb, &mysql);

    test_concurrent_same_prefix(&cluster_connection, &mysql);

    test_delete_oldest_basic(&ndb, &mysql);
    test_delete_oldest_drain_idempotent(&ndb, &mysql);
    test_delete_oldest_empty_table(&ndb, &mysql);
    test_delete_oldest_after_wrap(&ndb, &mysql);
    test_delete_oldest_refill_order(&ndb, &mysql);
    test_delete_oldest_multi_prefix(&ndb, &mysql);

    test_alter_ring_size_rejected(&ndb, &mysql);
    test_meta_absorb_preserves_unrelated_op(&ndb, &mysql);
    test_refresh_tuple_blocked(&ndb, &mysql);

    test_dict_validates_ring_metadata(&ndb, &mysql);
    test_dict_rejects_ttl_and_fr_combo(&ndb, &mysql);
    test_dict_rejects_add_fragment(&ndb, &mysql);

    test_corrupt_meta_rejected(&ndb, &mysql);
    test_delete_oldest_edges(&ndb, &mysql);
    test_delete_oldest_blob(&ndb, &mysql);

    test_ttl_ring_writer_and_delete_oldest(&ndb, &mysql);
    test_ttl_ring_meta_never_expires(&ndb, &mysql);
    test_ttl_ring_only_expired_delete(&ndb, &mysql);
    test_ttl_ring_precision_and_partial_record(&ndb, &mysql);

    std::cout << "\n=== Results ===" << std::endl;
    std::cout << "Passed: " << g_tests_passed << std::endl;
    std::cout << "Failed: " << g_tests_failed << std::endl;
  }

  mysql_close(&mysql);
  mysql_library_end();
  ndb_end(0);

  return g_tests_failed > 0 ? EXIT_FAILURE : EXIT_SUCCESS;
}
