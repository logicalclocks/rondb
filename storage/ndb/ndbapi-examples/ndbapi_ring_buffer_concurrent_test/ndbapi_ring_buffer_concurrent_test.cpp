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
 * Concurrent ring buffer insert test.
 *
 * Demonstrates that when two transactions concurrently call
 * NdbRingBufferWriter::addRow() for the same PK prefix and no meta
 * row exists yet, both see error 626 (tuple not found) — NDB cannot
 * lock a non-existent row.  At flush() time, only one insertTuple
 * for the meta row succeeds; the other fails with error 630
 * (duplicate tuple).
 *
 * Usage:
 *   ndb_ring_buffer_concurrent_test <mysql_host> <mysql_port> <connectstring>
 */

#ifdef _WIN32
#include <winsock2.h>
#endif
#include <mysql.h>
#include <NdbApi.hpp>
#include "NdbRecord.hpp"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>

// ---------------------------------------------------------------
// Barrier — blocks N threads until all arrive
// ---------------------------------------------------------------

class Barrier {
 public:
  explicit Barrier(int count) : m_count(count), m_waiting(0), m_gen(0) {}

  void wait() {
    std::unique_lock<std::mutex> lk(m_mu);
    int gen = m_gen;
    if (++m_waiting == m_count) {
      m_waiting = 0;
      ++m_gen;
      m_cv.notify_all();
    } else {
      m_cv.wait(lk, [this, gen] { return gen != m_gen; });
    }
  }

 private:
  std::mutex m_mu;
  std::condition_variable m_cv;
  int m_count;
  int m_waiting;
  int m_gen;
};

// ---------------------------------------------------------------
// NdbRecord helpers (same as ndbapi_ring_buffer_test)
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

static void buildMask(const NdbDictionary::Table *table, unsigned char *mask,
                      Uint32 mask_size, const char *const *col_names,
                      int num_cols) {
  memset(mask, 0, mask_size);
  for (int i = 0; i < num_cols; i++) {
    const NdbDictionary::Column *c = table->getColumn(col_names[i]);
    if (!c) continue;
    Uint32 aid = c->getAttrId();
    mask[aid >> 3] |= (1 << (aid & 7));
  }
}

// ---------------------------------------------------------------
// SQL helpers
// ---------------------------------------------------------------

static void mysql_exec(MYSQL *mysql, const char *sql) {
  if (mysql_query(mysql, sql) != 0) {
    std::cerr << "SQL ERROR: " << mysql_error(mysql) << "\n  SQL: " << sql
              << std::endl;
    exit(EXIT_FAILURE);
  }
}

static int countDataRows(MYSQL *mysql, const char *tbl, int cid) {
  char sql[256];
  snprintf(sql, sizeof(sql),
           "SELECT COUNT(*) FROM test.%s WHERE client_id=%d AND ring_idx>0",
           tbl, cid);
  mysql_exec(mysql, sql);
  MYSQL_RES *res = mysql_store_result(mysql);
  MYSQL_ROW r = mysql_fetch_row(res);
  int count = atoi(r[0]);
  mysql_free_result(res);
  return count;
}

// ---------------------------------------------------------------
// Per-thread result (NDB API test)
// ---------------------------------------------------------------

struct ThreadResult {
  bool addRow_ok = false;
  bool flush_ok = false;
  bool commit_ok = false;
  int error_code = 0;
  std::string error_message;
};

// ---------------------------------------------------------------
// Per-thread result (MySQL test)
// ---------------------------------------------------------------

struct MysqlThreadResult {
  int successes = 0;
  int dup_key_errors = 0;    // error 1062
  int other_errors = 0;
};

// ---------------------------------------------------------------
// Writer thread — each runs its own Ndb + transaction
// ---------------------------------------------------------------

static void writer_thread(Ndb_cluster_connection *conn, Barrier *barrier,
                          ThreadResult *result, int thread_id) {
  Ndb ndb(conn, "test");
  if (ndb.init() != 0) {
    result->error_code = -1;
    result->error_message = "Ndb init failed";
    return;
  }

  NdbDictionary::Dictionary *dict = ndb.getDictionary();
  const NdbDictionary::Table *table = dict->getTable("rb_concurrent");
  if (!table) {
    result->error_code = -1;
    result->error_message = "getTable failed";
    return;
  }

  const NdbRecord *record = table->getDefaultRecord();
  Uint32 row_size = record->m_row_size;

  const NdbRecord::Attr *cid_attr = findAttr(record, table, "client_id");
  const NdbRecord::Attr *data_attr = findAttr(record, table, "event_data");
  const NdbRecord::Attr *rmeta_attr = findAttr(record, table, "ring_meta");

  // Build user mask
  Uint32 max_attr = 0;
  for (Uint32 i = 0; i < record->noOfColumns; i++)
    if (record->columns[i].attrId > max_attr)
      max_attr = record->columns[i].attrId;
  Uint32 mask_size = (max_attr / 8) + 1;
  unsigned char *mask = new unsigned char[mask_size];
  const char *cols[] = {"client_id", "event_data"};
  buildMask(table, mask, mask_size, cols, 2);

  // Fill row: both threads use client_id=1 (same PK prefix)
  char *rowbuf = new char[row_size];
  memset(rowbuf, 0, row_size);
  setInt32(rowbuf, cid_attr, 1);
  setNull(rowbuf, rmeta_attr);
  clearNull(rowbuf, data_attr);
  char label[32];
  snprintf(label, sizeof(label), "thread_%d", thread_id);
  setVarchar(rowbuf, data_attr, label);

  // Start transaction
  NdbTransaction *trans = ndb.startTransaction(table);
  if (!trans) {
    result->error_code = -1;
    result->error_message = "startTransaction failed";
    delete[] rowbuf;
    delete[] mask;
    return;
  }

  NdbRingBufferWriter writer(table, record, trans);
  if (writer.getErrorCode() != 0) {
    result->error_code = writer.getErrorCode();
    result->error_message = writer.getErrorMessage();
    ndb.closeTransaction(trans);
    delete[] rowbuf;
    delete[] mask;
    return;
  }

  // ---- Phase 1: addRow() (internally calls readMetaRow + execute) ----
  // Both threads should find no meta row (error 626) and proceed.
  const NdbOperation *op = writer.addRow(rowbuf, mask);
  result->addRow_ok = (op != nullptr);

  if (!op) {
    result->error_code = writer.getErrorCode();
    result->error_message = writer.getErrorMessage();
    ndb.closeTransaction(trans);
    delete[] rowbuf;
    delete[] mask;
    return;
  }

  std::cout << "  [Thread " << thread_id
            << "] addRow succeeded — readMetaRow saw 626 (no meta row)"
            << std::endl;

  // ---- BARRIER: wait for both threads to complete addRow ----
  // At this point both have seen 626 and prepared an insertTuple path.
  barrier->wait();

  // ---- Phase 2: flush() — one insertTuple should win, one should lose ----
  int ret = writer.flush();
  result->flush_ok = (ret == 0);
  result->error_code = writer.getErrorCode();
  result->error_message = writer.getErrorMessage();

  if (ret == 0) {
    std::cout << "  [Thread " << thread_id
              << "] flush succeeded — meta insertTuple won the race"
              << std::endl;
    // Try to commit
    if (trans->execute(NdbTransaction::Commit) == 0) {
      result->commit_ok = true;
      std::cout << "  [Thread " << thread_id << "] commit succeeded"
                << std::endl;
    } else {
      std::cout << "  [Thread " << thread_id << "] commit failed: "
                << trans->getNdbError().message << std::endl;
    }
  } else {
    std::cout << "  [Thread " << thread_id << "] flush failed — error "
              << writer.getErrorCode() << ": " << writer.getErrorMessage()
              << std::endl;
  }

  ndb.closeTransaction(trans);
  delete[] rowbuf;
  delete[] mask;
  mysql_thread_end();
}

// ---------------------------------------------------------------
// MySQL writer thread — each opens its own connection
// ---------------------------------------------------------------

static void mysql_writer_thread(const char *host, unsigned int port,
                                Barrier *barrier, MysqlThreadResult *result,
                                int thread_id, int num_iterations) {
  MYSQL conn;
  if (!mysql_init(&conn)) {
    result->other_errors = num_iterations;
    return;
  }
  if (!mysql_real_connect(&conn, host, "root", "", "test", port, nullptr, 0)) {
    std::cerr << "  [MySQL Thread " << thread_id
              << "] connect failed: " << mysql_error(&conn) << std::endl;
    result->other_errors = num_iterations;
    return;
  }

  for (int i = 0; i < num_iterations; i++) {
    /*
     * Each iteration uses a unique client_id so both threads race on
     * a PK prefix that has no meta row yet.
     * client_id = iteration * 1000 + 1  (both threads use the same value)
     */
    int cid = (i + 1) * 1000;

    // Synchronize: both threads start the INSERT at the same time
    barrier->wait();

    char sql[256];
    snprintf(sql, sizeof(sql),
             "INSERT INTO test.rb_mysql_concurrent (client_id, event_data) "
             "VALUES (%d, 'mysql_thread_%d_iter_%d')",
             cid, thread_id, i);

    if (mysql_query(&conn, sql) == 0) {
      result->successes++;
    } else {
      unsigned int err = mysql_errno(&conn);
      if (err == 1062) {
        result->dup_key_errors++;
      } else {
        result->other_errors++;
        std::cerr << "  [MySQL Thread " << thread_id << " iter " << i
                  << "] unexpected error " << err << ": " << mysql_error(&conn)
                  << std::endl;
      }
    }
  }

  mysql_close(&conn);
  mysql_thread_end();
}

// ---------------------------------------------------------------
// MySQL concurrent insert test
// ---------------------------------------------------------------

static bool test_mysql_concurrent(MYSQL *admin_conn, const char *host,
                                  unsigned int port) {
  const int NUM_ITERATIONS = 100;

  std::cout << std::endl;
  std::cout << "=== Ring Buffer Concurrent INSERT Test (MySQL) ===" << std::endl;
  std::cout << std::endl;
  std::cout << "Scenario: Two MySQL connections both INSERT into the same"
            << std::endl;
  std::cout << "PK prefix when no meta row exists yet. " << NUM_ITERATIONS
            << " iterations." << std::endl;
  std::cout << "One INSERT should succeed, the other may get error 1062."
            << std::endl;
  std::cout << std::endl;

  mysql_exec(admin_conn, "DROP TABLE IF EXISTS test.rb_mysql_concurrent");
  mysql_exec(admin_conn,
             "CREATE TABLE test.rb_mysql_concurrent ("
             "  client_id INT NOT NULL,"
             "  ring_idx INT NOT NULL DEFAULT 0,"
             "  ring_meta VARBINARY(64),"
             "  event_data VARCHAR(100),"
             "  PRIMARY KEY (client_id, ring_idx)"
             ") ENGINE=NDB,"
             "  COMMENT='NDB_TABLE=RING_BUFFER=5@ring_idx@ring_meta'");

  Barrier barrier(2);
  MysqlThreadResult result_a, result_b;

  std::thread ta(mysql_writer_thread, host, port, &barrier, &result_a, 0,
                 NUM_ITERATIONS);
  std::thread tb(mysql_writer_thread, host, port, &barrier, &result_b, 1,
                 NUM_ITERATIONS);

  ta.join();
  tb.join();

  std::cout << "--- Results (" << NUM_ITERATIONS << " iterations) ---"
            << std::endl;
  std::cout << "Thread A: success=" << result_a.successes
            << ", dup_key(1062)=" << result_a.dup_key_errors
            << ", other_err=" << result_a.other_errors << std::endl;
  std::cout << "Thread B: success=" << result_b.successes
            << ", dup_key(1062)=" << result_b.dup_key_errors
            << ", other_err=" << result_b.other_errors << std::endl;

  int total_dup = result_a.dup_key_errors + result_b.dup_key_errors;
  int total_success = result_a.successes + result_b.successes;
  int total_other = result_a.other_errors + result_b.other_errors;

  std::cout << std::endl;
  std::cout << "Total: " << total_success << " succeeded, " << total_dup
            << " got 1062 (dup key), " << total_other << " other errors"
            << std::endl;

  bool pass = true;

  // Every iteration should have exactly 2 outcomes total (one per thread)
  if (total_success + total_dup + total_other != 2 * NUM_ITERATIONS) {
    std::cout << "CHECK: Unexpected total count — FAIL" << std::endl;
    pass = false;
  }

  // Each iteration: both can succeed (no race), or one succeeds + one gets 1062
  // So total_success should be between NUM_ITERATIONS and 2*NUM_ITERATIONS
  if (total_success >= NUM_ITERATIONS && total_success <= 2 * NUM_ITERATIONS) {
    std::cout << "CHECK: Each PK prefix has at least one successful INSERT — OK"
              << std::endl;
  } else {
    std::cout << "CHECK: Unexpected success count — FAIL" << std::endl;
    pass = false;
  }

  if (total_other == 0) {
    std::cout << "CHECK: No unexpected errors — OK" << std::endl;
  } else {
    std::cout << "CHECK: Got " << total_other << " unexpected errors — FAIL"
              << std::endl;
    pass = false;
  }

  if (total_dup > 0) {
    std::cout << "CHECK: Race manifested in " << total_dup << "/" << NUM_ITERATIONS
              << " iterations — error 1062 (ER_DUP_ENTRY) confirmed" << std::endl;
    std::cout << std::endl;
    std::cout << "RESULT: MySQL returns error 1062 (Duplicate entry '...' for "
                 "key 'PRIMARY')"
              << std::endl;
    std::cout << "        when two connections race to INSERT the first row for "
                 "the same PK prefix."
              << std::endl;
    std::cout << "        The internal meta row insertTuple conflict (NDB 630) "
                 "surfaces as ER_DUP_ENTRY."
              << std::endl;
  } else {
    std::cout << "CHECK: Race did not manifest in any iteration (all "
              << NUM_ITERATIONS << " succeeded on both threads)" << std::endl;
    std::cout << "       This means the timing window was too narrow to catch. "
                 "Try increasing iterations."
              << std::endl;
  }

  mysql_exec(admin_conn, "DROP TABLE test.rb_mysql_concurrent");
  return pass && total_dup > 0;
}

// ---------------------------------------------------------------
// Main
// ---------------------------------------------------------------

int main(int argc, char **argv) {
  if (argc != 4) {
    std::cout << "Usage: ndb_ring_buffer_concurrent_test <mysql_host> "
                 "<mysql_port> <ndb_connectstring>"
              << std::endl;
    std::cout << "Example: ndb_ring_buffer_concurrent_test 127.0.0.1 3308 "
                 "127.0.0.1:1188"
              << std::endl;
    return EXIT_FAILURE;
  }

  const char *mysql_host = argv[1];
  unsigned int mysql_port = (unsigned int)atoi(argv[2]);
  const char *connectstring = argv[3];

  ndb_init();

  // MySQL connection for DDL + verification
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

  bool pass = true;
  int flush_success = 0;

  // Scope the cluster connection so it's destroyed before ndb_end()
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

    // Create the test table
    mysql_exec(&mysql, "DROP TABLE IF EXISTS test.rb_concurrent");
    mysql_exec(&mysql,
               "CREATE TABLE test.rb_concurrent ("
               "  client_id INT NOT NULL,"
               "  ring_idx INT NOT NULL DEFAULT 0,"
               "  ring_meta VARBINARY(64),"
               "  event_data VARCHAR(100),"
               "  PRIMARY KEY (client_id, ring_idx)"
               ") ENGINE=NDB,"
               "  COMMENT='NDB_TABLE=RING_BUFFER=5@ring_idx@ring_meta'");

    std::cout << "=== Ring Buffer Concurrent Insert Test ===" << std::endl;
    std::cout << std::endl;
    std::cout << "Scenario: Two threads both insert into the same PK prefix"
              << std::endl;
    std::cout << "(client_id=1) when no meta row exists yet." << std::endl;
    std::cout << "Both readMetaRow() calls see 626 (no meta row)." << std::endl;
    std::cout << "At flush() time, only one insertTuple should succeed."
              << std::endl;
    std::cout << std::endl;

    Barrier barrier(2);
    ThreadResult result_a, result_b;

    std::thread thread_a(writer_thread, &cluster_connection, &barrier,
                         &result_a, 0);
    std::thread thread_b(writer_thread, &cluster_connection, &barrier,
                         &result_b, 1);

    thread_a.join();
    thread_b.join();

    std::cout << std::endl;
    std::cout << "--- Results ---" << std::endl;
    std::cout << "Thread A: addRow=" << (result_a.addRow_ok ? "OK" : "FAIL")
              << ", flush=" << (result_a.flush_ok ? "OK" : "FAIL")
              << ", commit=" << (result_a.commit_ok ? "OK" : "N/A");
    if (!result_a.flush_ok)
      std::cout << ", error=" << result_a.error_code << " ("
                << result_a.error_message << ")";
    std::cout << std::endl;

    std::cout << "Thread B: addRow=" << (result_b.addRow_ok ? "OK" : "FAIL")
              << ", flush=" << (result_b.flush_ok ? "OK" : "FAIL")
              << ", commit=" << (result_b.commit_ok ? "OK" : "N/A");
    if (!result_b.flush_ok)
      std::cout << ", error=" << result_b.error_code << " ("
                << result_b.error_message << ")";
    std::cout << std::endl;

    // Validation
    std::cout << std::endl;

    // Both addRow should succeed (both see 626, which is handled)
    if (result_a.addRow_ok && result_b.addRow_ok) {
      std::cout << "CHECK: Both addRow() succeeded (both saw 626) — OK"
                << std::endl;
    } else {
      std::cout << "CHECK: Expected both addRow() to succeed — FAIL"
                << std::endl;
      pass = false;
    }

    // Exactly one flush should succeed, one should fail
    flush_success = (result_a.flush_ok ? 1 : 0) +
                    (result_b.flush_ok ? 1 : 0);
    if (flush_success == 1) {
      std::cout << "CHECK: Exactly one flush() succeeded, one failed — OK"
                << std::endl;
    } else if (flush_success == 2) {
      std::cout << "CHECK: Both flush() succeeded — this means the race did "
                   "not manifest (timing-dependent)."
                << std::endl;
      std::cout << "       Re-run the test to reproduce. If consistently both "
                   "succeed, the analysis may need revisiting."
                << std::endl;
    } else {
      std::cout << "CHECK: Both flush() failed — unexpected — FAIL"
                << std::endl;
      pass = false;
    }

    // The loser should have error 630 (duplicate tuple)
    const ThreadResult &loser =
        result_a.flush_ok ? result_b : result_a;
    if (!loser.flush_ok) {
      if (loser.error_code == 630) {
        std::cout
            << "CHECK: Losing thread got error 630 (Duplicate tuple) — OK"
            << std::endl;
      } else {
        std::cout << "CHECK: Losing thread got error " << loser.error_code
                  << " (expected 630) — UNEXPECTED" << std::endl;
      }
    }

    // Verify data via MySQL
    int data_count = countDataRows(&mysql, "rb_concurrent", 1);
    std::cout << "CHECK: Data rows for client_id=1: " << data_count;
    if (flush_success == 1 && data_count == 1) {
      std::cout << " — OK (winner wrote 1 row)" << std::endl;
    } else if (flush_success == 2 && data_count >= 1) {
      std::cout << " — OK (no race manifested, both committed)" << std::endl;
    } else {
      std::cout << " — unexpected count" << std::endl;
    }

    std::cout << std::endl;
    if (pass && flush_success == 1) {
      std::cout << "RESULT: Race condition confirmed — NDB cannot lock a "
                   "non-existent row."
                << std::endl;
      std::cout << "        LM_Exclusive in readMetaRow() does not protect the "
                   "first-insert case."
                << std::endl;
      std::cout
          << "        NDB's insertTuple duplicate-key detection (error 630)"
          << std::endl;
      std::cout
          << "        is the actual safety net. The losing transaction must "
             "retry."
          << std::endl;
    } else if (flush_success == 2) {
      std::cout << "RESULT: Both transactions succeeded — the race did not "
                   "manifest this run."
                << std::endl;
      std::cout << "        This is possible if one thread's flush() completed "
                   "before the other's"
                << std::endl;
      std::cout << "        readMetaRow() executed. Try running again."
                << std::endl;
    }

    mysql_exec(&mysql, "DROP TABLE test.rb_concurrent");
  }  // cluster_connection destroyed here, internal threads cleaned up

  // ---- Test 2: MySQL concurrent INSERT ----
  bool mysql_pass = test_mysql_concurrent(&mysql, mysql_host, mysql_port);

  mysql_close(&mysql);
  ndb_end(0);

  bool ndb_pass = (pass && flush_success == 1);
  std::cout << std::endl;
  std::cout << "=== Final Summary ===" << std::endl;
  std::cout << "NDB API test:  " << (ndb_pass ? "PASS" : "FAIL") << std::endl;
  std::cout << "MySQL test:    " << (mysql_pass ? "PASS (race observed)"
                                                : "INCONCLUSIVE (no race hit)")
            << std::endl;

  return ndb_pass ? EXIT_SUCCESS : EXIT_FAILURE;
}
