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
// NdbRecord buffer helpers — charset-safe varchar encoding
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
  Uint32 mask_size;

  bool init(const NdbDictionary::Table *table) {
    record = table->getDefaultRecord();
    if (!record) return false;
    row_size = record->m_row_size;
    cid_attr = findAttr(record, table, "client_id");
    data_attr = findAttr(record, table, "event_data");
    rmeta_attr = findAttr(record, table, "ring_meta");
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
  }

  unsigned char *newUserMask(const NdbDictionary::Table *table) const {
    unsigned char *mask = new unsigned char[mask_size];
    const char *cols[] = {"client_id", "event_data"};
    buildMask(table, mask, mask_size, cols, 2);
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

static const char *CREATE_BASIC =
    "CREATE TABLE test.%s ("
    "  client_id INT NOT NULL,"
    "  ring_idx INT NOT NULL DEFAULT 0,"
    "  ring_meta VARBINARY(64),"
    "  event_data VARCHAR(100),"
    "  PRIMARY KEY (client_id, ring_idx)"
    ") ENGINE=NDB,"
    "  COMMENT='NDB_TABLE=RING_BUFFER=%d@ring_idx@ring_meta'";

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
 * 7 rows into ring_size=5 — only last 5 survive.
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
 * Test 4: Batch insert — 10 rows, same PK prefix, ring_size=10.
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
             "  COMMENT='NDB_TABLE=RING_BUFFER=3@ring_idx@ring_meta'");

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
 * Test 6: ring_size=1 — every insert overwrites the single slot.
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
 * Test 7: Non-ring-buffer table — writer should reject.
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
 * Test 8: Multiple transactions — verify meta row carries state.
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

// ---------------------------------------------------------------
// Main
// ---------------------------------------------------------------

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
    std::cerr << "Ndb init failed: " << ndb.getNdbError().message << std::endl;
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

  std::cout << "\n=== Results ===" << std::endl;
  std::cout << "Passed: " << g_tests_passed << std::endl;
  std::cout << "Failed: " << g_tests_failed << std::endl;

  mysql_close(&mysql);
  ndb_end(0);

  return g_tests_failed > 0 ? EXIT_FAILURE : EXIT_SUCCESS;
}
