#!/usr/bin/env python3
#   Copyright (c) 2024, 2025, Hopsworks and/or its affiliates.
#
#   This program is free software; you can redistribute it and/or modify
#   it under the terms of the GNU General Public License, version 2.0,
#   as published by the Free Software Foundation.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License, version 2.0, for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program; if not, write to the Free Software
#   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

"""
TTL (Time-To-Live) Test Suite for RonDB
=======================================

This test suite validates the TTL functionality in NDB Cluster. It is a refactored
version of ttl_test.py with an OOP structure for better maintainability.

USAGE
-----
    # Run all tests
    python3 ttl_test_suite.py

    # Run specific category
    python3 ttl_test_suite.py --category read_locked

    # Run specific test
    python3 ttl_test_suite.py --test case_47_unique_index_replication

    # List all tests
    python3 ttl_test_suite.py --list

    # Skip global table setup (if already exists)
    python3 ttl_test_suite.py --skip-setup

CATEGORIES
----------
Current categories (matching original ttl_test.py structure):
- read_locked    : FOR SHARE locking with FULLY_REPLICATED tables (cases 28, 48, 29)
- unique_index   : Unique index behavior with replication (cases 47, 50, 53)
- disk_column    : Disk-based columns with TTL (cases 51, 52)
- foreign_key    : Foreign key interactions (case 56; cases 54, 55 disabled)
- backup_restore : Backup and restore with TTL (case 46)

SETUP REQUIREMENTS
------------------
1. Primary MySQL on port 3308 (configurable)
2. Replica MySQL on port 3309 for replication tests
3. MySQL binlog replication configured between primary and replica
4. Tablespace 'ts_1' for disk_column tests

CONFIGURATION
-------------
Create /tmp/ttl_config.py to override defaults:
    MYSQLD_PORT_P = 3308    # Primary MySQL port
    MYSQLD_PORT_R = 3309    # Replica MySQL port

GLOBAL TEST TABLE
-----------------
Most tests use the global 'test.sz' table created at startup:
    CREATE TABLE test.sz (
        col_a INT PRIMARY KEY,
        col_b TIMESTAMP,        -- TTL column
        col_c INT UNIQUE KEY
    ) ENGINE=NDB COMMENT="NDB_TABLE=TTL=10@col_b"

Tests that need different schemas (FULLY_REPLICATED, disk columns, etc.)
create their own 'sz1' table in setup() and drop it in teardown().

ADDING NEW TEST CASES
---------------------
1. NAMING: Use "case_N_descriptive_name" format to match original ttl_test.py
   numbering. This helps cross-reference with the original test file.

2. BASE CLASS: Choose the appropriate base class:
   - TTLTestBase: Single-threaded tests
   - ConcurrentTTLTest: Two-thread tests (thread_a on primary, thread_b on replica or primary)

3. REGISTRATION: Use the @category.register decorator:
       @cat_unique_index.register
       class TestCaseNNewTest(ConcurrentTTLTest):
           ...

4. REQUIRED PROPERTIES:
   - name: Return "case_N_descriptive_name"
   - description: Short description for --list output
   - requires_replica: Set True if thread_b needs replica connection
   - uses_global_sz: Set False if test creates its own table (sz1)

5. CUSTOM TABLES: If your test needs a custom table (sz1):
   - Override uses_global_sz to return False
   - Create table in setup(), drop in teardown()
   - Use "DROP TABLE IF EXISTS sz1" before CREATE to handle leftover tables

6. THREAD TIMING: Tests rely on TTL=10 seconds. Common patterns:
   - Insert, sleep(11), verify expired
   - Insert, sleep(5), verify visible, sleep(6), verify expired

7. REPLICATION TESTS: For tests verifying replica behavior:
   - Set requires_replica = True
   - thread_a runs on primary (port 3308)
   - thread_b runs on replica (port 3309)
   - Use check_replication_health() to verify replica is healthy

KNOWN LIMITATIONS / DISABLED TESTS
----------------------------------
- Case 49: FULLY_REPLICATED + UNIQUE INDEX causes binlog replication to get stuck
  with "Waiting for dependent transaction to commit". Disabled in both original
  ttl_test.py and this suite.

- Cases 54, 55: Foreign key tests with TTL tables as parent/child not yet supported.
  Class definitions exist but registration is commented out.

- DEBUG_SYNC tests (cases 23, 24, 25): Require MySQL built with DEBUG_SYNC.
  These are skipped automatically if DEBUG_SYNC is not available.

DIFFERENCES FROM ORIGINAL ttl_test.py
-------------------------------------
1. OOP structure with test classes instead of global functions
2. Each test creates fresh connections (original reused A_conn for pre/thdA/post)
3. Automatic test discovery and categorization
4. Better error reporting with stack traces
5. --list option to see available tests
"""

import argparse
import importlib.util
import json
import os
import re
import subprocess
import sys
import threading
import time
import urllib.request
from abc import ABC, abstractmethod

import pymysql


###############################################################################
# Configuration
###############################################################################

class TestConfig:
    """Test suite configuration"""

    def __init__(self):
        # Connection settings
        self.host = "127.0.0.1"
        self.primary_port = 3308
        self.replica_port = 3309
        self.user = "root"
        self.password = ""
        self.database = "test"

        # TTL settings
        self.default_ttl = 10  # seconds

        # RDRS settings
        self.rdrs_host = "127.0.0.1"
        self.rdrs_port = 4406
        self.purge_enabled = False

        # NDB management / backup-restore settings
        self.bin_dir = "/home/zhao/workspace/kernelmaker/rondb-bin/bin"
        self.primary_mgmd_port = 1188
        self.replica_mgmd_port = 1198
        self.data_dir_p_1 = "/home/zhao/workspace/rondb-run/ndbmtd_1"
        self.data_dir_p_2 = "/home/zhao/workspace/rondb-run/ndbmtd_2"

        # Replication credentials (used to restart replication after backup/restore)
        self.repl_user = "replicator"
        self.repl_password = "rep123"

        # Test execution settings
        self.verbose = False
        self.stop_on_failure = False

    @classmethod
    def load(cls, config_path="/tmp/ttl_config.py"):
        """Load configuration from file if exists"""
        config = cls()
        if os.path.exists(config_path):
            spec = importlib.util.spec_from_file_location("ttl_config", config_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            config.primary_port = getattr(module, 'MYSQLD_PORT_P', config.primary_port)
            config.replica_port = getattr(module, 'MYSQLD_PORT_R', config.replica_port)
            config.default_ttl = getattr(module, 'TTL_DEFAULT', config.default_ttl)
            config.rdrs_host = getattr(module, 'RDRS_HOST', config.rdrs_host)
            config.rdrs_port = getattr(module, 'RDRS_PORT', config.rdrs_port)
            config.bin_dir = getattr(module, 'BIN_DIR', config.bin_dir)
            config.primary_mgmd_port = getattr(module, 'MGMD_PORT_P', config.primary_mgmd_port)
            config.replica_mgmd_port = getattr(module, 'MGMD_PORT_R', config.replica_mgmd_port)
            config.data_dir_p_1 = getattr(module, 'DATA_DIR_P_1', config.data_dir_p_1)
            config.data_dir_p_2 = getattr(module, 'DATA_DIR_P_2', config.data_dir_p_2)
            config.repl_user = getattr(module, 'REPL_USER', config.repl_user)
            config.repl_password = getattr(module, 'REPL_PASSWORD', config.repl_password)

        # Environment overrides
        config.host = os.environ.get('MYSQLD_HOST', config.host)
        config.primary_port = int(os.environ.get('MYSQLD_PORT_P', config.primary_port))
        config.replica_port = int(os.environ.get('MYSQLD_PORT_R', config.replica_port))
        config.default_ttl = int(os.environ.get('TTL_DEFAULT', config.default_ttl))
        config.rdrs_host = os.environ.get('RDRS_HOST', config.rdrs_host)
        config.rdrs_port = int(os.environ.get('RDRS_PORT', config.rdrs_port))
        config.bin_dir = os.environ.get('TTL_BIN_DIR', config.bin_dir)
        config.primary_mgmd_port = int(os.environ.get('TTL_PRIMARY_MGMD_PORT', config.primary_mgmd_port))
        config.replica_mgmd_port = int(os.environ.get('TTL_REPLICA_MGMD_PORT', config.replica_mgmd_port))
        config.data_dir_p_1 = os.environ.get('TTL_DATA_DIR_P_1', config.data_dir_p_1)
        config.data_dir_p_2 = os.environ.get('TTL_DATA_DIR_P_2', config.data_dir_p_2)
        config.repl_user = os.environ.get('TTL_REPL_USER', config.repl_user)
        config.repl_password = os.environ.get('TTL_REPL_PASSWORD', config.repl_password)

        return config


###############################################################################
# Test Result Types
###############################################################################

class TestStatus:
    """Test status constants"""
    PASSED = "PASSED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    ERROR = "ERROR"


class TestResult:
    """Result of a single test"""

    def __init__(self, name, status, duration=0.0, message="", error=None):
        self.name = name
        self.status = status
        self.duration = duration
        self.message = message
        self.error = error


class CategoryResult:
    """Result of a test category"""

    def __init__(self, name):
        self.name = name
        self.results = []

    @property
    def passed(self):
        return sum(1 for r in self.results if r.status == TestStatus.PASSED)

    @property
    def failed(self):
        return sum(1 for r in self.results if r.status == TestStatus.FAILED)

    @property
    def skipped(self):
        return sum(1 for r in self.results if r.status == TestStatus.SKIPPED)

    @property
    def total(self):
        return len(self.results)


###############################################################################
# Test Base Classes
###############################################################################

class TTLTestBase(ABC):
    """Base class for all TTL tests"""

    def __init__(self, config):
        self.config = config
        self.primary_conn = None
        self.replica_conn = None

    @property
    @abstractmethod
    def name(self):
        """Test name"""
        pass

    @property
    @abstractmethod
    def description(self):
        """Test description"""
        pass

    @property
    def requires_replica(self):
        """Whether this test requires replica connection"""
        return False

    @property
    def requires_debug_sync(self):
        """Whether this test requires DEBUG_SYNC"""
        return False

    @property
    def requires_purge(self):
        """Whether this test requires RDRS purging"""
        return False

    def setup(self):
        """Setup before test execution"""
        self.primary_conn = pymysql.connect(
            host=self.config.host,
            port=self.config.primary_port,
            user=self.config.user,
            password=self.config.password,
            database=self.config.database
        )
        if self.requires_replica:
            self.replica_conn = pymysql.connect(
                host=self.config.host,
                port=self.config.replica_port,
                user=self.config.user,
                password=self.config.password,
                database=self.config.database
            )

    def teardown(self):
        """Cleanup after test execution"""
        if self.primary_conn:
            self.primary_conn.close()
        if self.replica_conn:
            self.replica_conn.close()

    @abstractmethod
    def run(self):
        """Execute the test - raise AssertionError on failure"""
        pass

    def execute(self):
        """Execute test with setup/teardown and result capture"""
        start_time = time.time()
        try:
            # Check prerequisites
            if self.requires_debug_sync and not self._check_debug_sync():
                return TestResult(
                    name=self.name,
                    status=TestStatus.SKIPPED,
                    message="DEBUG_SYNC not enabled"
                )
            if self.requires_purge and not self.config.purge_enabled:
                return TestResult(
                    name=self.name,
                    status=TestStatus.SKIPPED,
                    message="Purge tests disabled (use --enable-purge)"
                )

            self.setup()
            self.run()
            return TestResult(
                name=self.name,
                status=TestStatus.PASSED,
                duration=time.time() - start_time
            )
        except AssertionError as e:
            return TestResult(
                name=self.name,
                status=TestStatus.FAILED,
                duration=time.time() - start_time,
                message=str(e),
                error=e
            )
        except Exception as e:
            return TestResult(
                name=self.name,
                status=TestStatus.ERROR,
                duration=time.time() - start_time,
                message=str(e),
                error=e
            )
        finally:
            self.teardown()

    def _check_debug_sync(self):
        """Check if DEBUG_SYNC is enabled"""
        try:
            conn = pymysql.connect(
                host=self.config.host,
                port=self.config.primary_port,
                user=self.config.user,
                password=self.config.password
            )
            cur = conn.cursor()
            cur.execute("SHOW VARIABLES LIKE 'DEBUG_SYNC'")
            result = cur.fetchone()
            conn.close()
            return result and "ON" in result[1]
        except:
            return False


class ConcurrentTTLTest(TTLTestBase):
    """Base class for tests with concurrent threads (thread_a and thread_b).

    This matches the original ttl_test.py pattern where each test case has:
    - case_N_thdA(conn): Thread A logic (runs on primary)
    - case_N_thdB(conn): Thread B logic (runs on primary or replica)

    Connection handling:
    - self.primary_conn: Used for setup/teardown (e.g., CREATE/DROP TABLE)
    - conn_a: Fresh connection passed to thread_a (always primary)
    - conn_b: Fresh connection passed to thread_b (replica if requires_replica=True)

    Key properties to override:
    - requires_replica: If True, thread_b connects to replica (port 3309)
    - uses_global_sz: If False, test manages its own table (sz1) instead of global sz
    """

    def __init__(self, config):
        super().__init__(config)
        self.thread_a_success = False
        self.thread_b_success = False
        self.thread_a_error = None
        self.thread_b_error = None

    @property
    def uses_global_sz(self):
        """Override to False if test uses its own table (sz1, etc.) instead of global sz"""
        return True

    def setup(self):
        super().setup()

    def teardown(self):
        super().teardown()

    @abstractmethod
    def thread_a(self, conn):
        """Thread A logic"""
        pass

    @abstractmethod
    def thread_b(self, conn):
        """Thread B logic"""
        pass

    def run(self):
        """Execute concurrent test"""
        conn_a = pymysql.connect(
            host=self.config.host,
            port=self.config.primary_port,
            user=self.config.user,
            password=self.config.password,
            database=self.config.database
        )
        conn_b = pymysql.connect(
            host=self.config.host,
            port=self.config.replica_port if self.requires_replica else self.config.primary_port,
            user=self.config.user,
            password=self.config.password,
            database=self.config.database
        )

        def run_thread_a():
            try:
                self.thread_a(conn_a)
                self.thread_a_success = True
            except Exception as e:
                self.thread_a_error = e

        def run_thread_b():
            try:
                self.thread_b(conn_b)
                self.thread_b_success = True
            except Exception as e:
                self.thread_b_error = e

        t_a = threading.Thread(target=run_thread_a)
        t_b = threading.Thread(target=run_thread_b)

        t_a.start()
        t_b.start()
        t_a.join()
        t_b.join()

        conn_a.close()
        conn_b.close()

        if self.thread_a_error:
            raise self.thread_a_error
        if self.thread_b_error:
            raise self.thread_b_error
        assert self.thread_a_success and self.thread_b_success, "Thread execution failed"


###############################################################################
# Test Category Registry
###############################################################################

class TestCategory:
    """A category of tests"""

    def __init__(self, name, description):
        self.name = name
        self.description = description
        self.tests = []

    def register(self, test_class):
        """Decorator to register a test class"""
        self.tests.append(test_class)
        return test_class

    def run(self, config):
        """Run all tests in this category"""
        result = CategoryResult(name=self.name)
        for i, test_class in enumerate(self.tests):
            test = test_class(config)
            print(f"  [{i+1}/{len(self.tests)}] Running: {test.name}...", end="", flush=True)
            test_result = test.execute()
            result.results.append(test_result)
            # Clear the "Running" line and show result
            print(f"\r  [{i+1}/{len(self.tests)}] [{test_result.status}] {test.name}", end="")
            if test_result.message:
                print(f" - {test_result.message}", end="")
            print(f" ({test_result.duration:.2f}s)")
            if test_result.status == TestStatus.FAILED and config.stop_on_failure:
                break
        return result


# Create category registry
CATEGORIES = {}


def category(name, description):
    """Create or get a test category"""
    if name not in CATEGORIES:
        CATEGORIES[name] = TestCategory(name, description)
    return CATEGORIES[name]


###############################################################################
# Category Definitions
###############################################################################

cat_insert = category("insert", "INSERT operations with TTL expiration")
cat_upsert = category("upsert", "INSERT ON DUPLICATE KEY UPDATE with TTL")
cat_update = category("update", "UPDATE operations on TTL tables")
cat_delete = category("delete", "DELETE operations on TTL tables")
cat_replace = category("replace", "REPLACE INTO operations")
cat_binlog = category("binlog", "Replication and binlog tests")


###############################################################################
# Helper Functions
###############################################################################

def create_ttl_table(conn, table_name, ttl_seconds,
                     extra_columns="", extra_keys="",
                     table_options=""):
    """Create a TTL table with standard structure"""
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {table_name}")

    columns = f"col_a INT, col_b TIMESTAMP, col_c INT{', ' + extra_columns if extra_columns else ''}"
    keys = f"PRIMARY KEY(col_a){', ' + extra_keys if extra_keys else ''}"

    sql = f"""
        CREATE TABLE {table_name} (
            {columns},
            {keys}
        ) ENGINE = NDB
        {', ' + table_options if table_options else ''}
        COMMENT="NDB_TABLE=TTL={ttl_seconds}@col_b"
    """
    cur.execute(sql)
    cur.close()


def drop_table(conn, table_name):
    """Drop a table if exists"""
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {table_name}")
    cur.close()


def check_replication_health(conn):
    """Check if replication is healthy (matches original check_rep logic)"""
    ret = False
    try:
        cur = conn.cursor()
        cur.execute("SHOW VARIABLES LIKE 'server_id'")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            server_id = row[1]
            assert server_id == "2", "ASSERT"

        cur.execute("SHOW REPLICA STATUS")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            io_running = row[10]
            sql_running = row[11]
            last_errno = row[18]
            last_io_errno = row[34]
            last_sql_errno = row[36]
            if io_running == "Yes" and sql_running == "Yes" and \
               last_errno == 0 and \
               last_io_errno == 0 and last_sql_errno == 0:
                ret = True
            return ret
    except Exception as e:
        print(f"check_rep failed: {e}")
        cur.close()
        return ret


###############################################################################
# INSERT Tests - Migrated from ttl_test.py cases 1-5
###############################################################################
#
# These tests verify INSERT behavior with TTL:
# - Case 1: INSERT with duplicate key error (row not expired)
# - Case 2: ZINSERT_TTL - INSERT succeeds to expired row slot
# - Case 3: Both inserts expire
# - Case 4: ROLLBACK before TTL
# - Case 5: ROLLBACK after TTL
#

@cat_insert.register
class TestCase1InsertDuplicateKey(ConcurrentTTLTest):
    """Case 1: INSERT with duplicate key error before TTL expiration

    Thread A inserts and commits before TTL.
    Thread B tries to insert same PK while row not expired -> gets duplicate key error.
    Both verify row visible initially, then invisible after TTL.
    """

    @property
    def name(self):
        return "case_1_insert_duplicate_key"

    @property
    def description(self):
        return "INSERT duplicate key error when existing row not expired"

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(5)
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT: Should have 1 row"
        for row in results:
            col_a = row[0]
            col_c = row[2]
            assert col_a == 1 and col_c == 100, "ASSERT: Should be col_c=100"
        time.sleep(6)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should expire after TTL"
        cur.close()

    def thread_b(self, conn):
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        try:
            cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 200)")
            assert False, "ASSERT: Should get duplicate key error"
        except pymysql.err.IntegrityError as e:
            assert e.args[0] == 1062, f"ASSERT: Expected error 1062, got {e.args[0]}"

        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT: Should have 1 row"
        for row in results:
            col_a = row[0]
            col_c = row[2]
            assert col_a == 1 and col_c == 100, "ASSERT: Should be col_c=100"
        time.sleep(6)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should expire after TTL"
        cur.close()


@cat_insert.register
class TestCase2InsertZinsertTTL(ConcurrentTTLTest):
    """Case 2: ZINSERT_TTL - INSERT to expired row slot succeeds

    Thread A inserts but holds transaction past TTL (11s), then commits.
    Thread B inserts at T+5s (when A's row is expired but uncommitted).
    Thread B's INSERT succeeds as ZINSERT_TTL (inserts to expired slot).
    Both see Thread B's row (col_c=200).
    """

    @property
    def name(self):
        return "case_2_insert_zinsert_ttl"

    @property
    def description(self):
        return "ZINSERT_TTL allows INSERT to expired row slot"

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(11)
        cur.execute("COMMIT")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT: Should have 1 row"
        for row in results:
            col_a = row[0]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT: Should be Thread B's row col_c=200"
        time.sleep(4)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should expire"
        cur.close()

    def thread_b(self, conn):
        time.sleep(5)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 200)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT: Should have 1 row"
        for row in results:
            col_a = row[0]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT: Should be col_c=200"
        time.sleep(5)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should expire"
        cur.close()


@cat_insert.register
class TestCase3BothInsertsExpire(ConcurrentTTLTest):
    """Case 3: Both threads' inserts expire

    Thread A holds transaction 13s (past TTL=10s), then commits.
    Thread B inserts at T+2s, row immediately expires.
    Both see 0 rows after their operations.
    """

    @property
    def name(self):
        return "case_3_both_inserts_expire"

    @property
    def description(self):
        return "Both INSERT operations result in expired rows"

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(13)
        cur.execute("COMMIT")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should be expired"
        cur.close()

    def thread_b(self, conn):
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 200)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should be expired"
        cur.close()


@cat_insert.register
class TestCase4RollbackBeforeTTL(ConcurrentTTLTest):
    """Case 4: ROLLBACK before TTL expiration

    Thread A inserts, sleeps 5s, then ROLLBACK.
    Thread B inserts at T+2s and commits (succeeds after A rollback).
    Both see Thread B's row (col_c=200).
    """

    @property
    def name(self):
        return "case_4_rollback_before_ttl"

    @property
    def description(self):
        return "ROLLBACK releases row, other INSERT succeeds"

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(5)
        cur.execute("ROLLBACK")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT: Should have 1 row"
        for row in results:
            col_a = row[0]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT: Should be Thread B's row col_c=200"
        time.sleep(7)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should expire"
        cur.close()

    def thread_b(self, conn):
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 200)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT: Should have 1 row"
        for row in results:
            col_a = row[0]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT: Should be col_c=200"
        time.sleep(8)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should expire"
        cur.close()


@cat_insert.register
class TestCase5RollbackAfterTTL(ConcurrentTTLTest):
    """Case 5: ROLLBACK after TTL expired

    Thread A inserts, sleeps 11s (past TTL), then ROLLBACK.
    Thread B inserts at T+5s with ZINSERT_TTL (A's row expired).
    Both see Thread B's row (col_c=200).
    """

    @property
    def name(self):
        return "case_5_rollback_after_ttl"

    @property
    def description(self):
        return "ROLLBACK after TTL, other INSERT already succeeded"

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(11)
        cur.execute("ROLLBACK")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT: Should have 1 row"
        for row in results:
            col_a = row[0]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT: Should be Thread B's row col_c=200"
        time.sleep(10)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should expire"
        cur.close()

    def thread_b(self, conn):
        time.sleep(5)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 200)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT: Should have 1 row"
        for row in results:
            col_a = row[0]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT: Should be col_c=200"
        time.sleep(6)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should expire"
        cur.close()


###############################################################################
# UPSERT Tests - Migrated from ttl_test.py cases 6-12
###############################################################################
#
# These tests verify INSERT ON DUPLICATE KEY UPDATE behavior with TTL:
# - Case 6: ROLLBACK after TTL, plain INSERT (both expire)
# - Case 7: COMMIT before TTL, ON DUP KEY UPDATE updates existing row
# - Case 8: COMMIT after TTL, ON DUP KEY does INSERT (row expired)
# - Case 9: COMMIT after TTL, ON DUP KEY does INSERT, both expire
# - Case 10: ROLLBACK before TTL, ON DUP KEY does INSERT (fresh)
# - Case 11: ROLLBACK after TTL, ON DUP KEY does INSERT
# - Case 12: ROLLBACK after TTL, both expire
#

@cat_upsert.register
class TestCase6RollbackAfterTTLPlainInsert(ConcurrentTTLTest):
    """Case 6: ROLLBACK after TTL with plain INSERT

    Thread A inserts, sleeps 13s (past TTL), ROLLBACK.
    Thread B inserts at T+2s with plain INSERT.
    Both rows expire.
    """

    @property
    def name(self):
        return "case_6_rollback_after_ttl_plain_insert"

    @property
    def description(self):
        return "ROLLBACK after TTL, plain INSERT, both expire"

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(13)
        cur.execute("ROLLBACK")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should be expired"
        cur.close()

    def thread_b(self, conn):
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 200)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should be expired"
        cur.close()


@cat_upsert.register
class TestCase7UpsertUpdatesRow(ConcurrentTTLTest):
    """Case 7: INSERT ON DUP KEY UPDATE updates existing row

    Thread A inserts, sleeps 5s (before TTL), COMMIT.
    Thread B does INSERT ON DUP KEY at T+2s, updates to col_c=201.
    Both see col_c=201, TTL refreshed by NOW().
    """

    @property
    def name(self):
        return "case_7_upsert_updates_row"

    @property
    def description(self):
        return "INSERT ON DUP KEY UPDATE updates existing row"

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(5)
        cur.execute("COMMIT")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT: Should have 1 row"
        for row in results:
            col_a = row[0]
            col_c = row[2]
            assert col_a == 1 and col_c == 201, "ASSERT: Should be updated to col_c=201"
        time.sleep(7)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should expire"
        cur.close()

    def thread_b(self, conn):
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 200) ON DUPLICATE KEY UPDATE col_c = 201, col_b = NOW()")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT: Should have 1 row"
        for row in results:
            col_a = row[0]
            col_c = row[2]
            assert col_a == 1 and col_c == 201, "ASSERT: Should be col_c=201"
        time.sleep(8)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should expire"
        cur.close()


@cat_upsert.register
class TestCase8UpsertInsertsWhenExpired(ConcurrentTTLTest):
    """Case 8: INSERT ON DUP KEY does INSERT when row expired

    Thread A inserts, sleeps 11s (past TTL), COMMIT (row timestamp expired).
    Thread B does INSERT ON DUP KEY at T+5s.
    Since A's row is expired, B does fresh INSERT with col_c=200.
    """

    @property
    def name(self):
        return "case_8_upsert_inserts_when_expired"

    @property
    def description(self):
        return "INSERT ON DUP KEY does fresh INSERT when row expired"

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(11)
        cur.execute("COMMIT")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT: Should have 1 row"
        for row in results:
            col_a = row[0]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT: Should be Thread B's fresh insert col_c=200"
        time.sleep(4)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should expire"
        cur.close()

    def thread_b(self, conn):
        time.sleep(5)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 200) ON DUPLICATE KEY UPDATE col_c = 201, col_b = NOW()")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT: Should have 1 row"
        for row in results:
            col_a = row[0]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT: Should be fresh insert col_c=200"
        time.sleep(5)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should expire"
        cur.close()


@cat_upsert.register
class TestCase9UpsertInsertsBothExpire(ConcurrentTTLTest):
    """Case 9: INSERT ON DUP KEY does INSERT, both expire

    Thread A inserts, sleeps 13s (past TTL), COMMIT.
    Thread B does INSERT ON DUP KEY at T+2s.
    Both rows expire.
    """

    @property
    def name(self):
        return "case_9_upsert_inserts_both_expire"

    @property
    def description(self):
        return "INSERT ON DUP KEY does INSERT, both expire"

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(13)
        cur.execute("COMMIT")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should be expired"
        cur.close()

    def thread_b(self, conn):
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 200) ON DUPLICATE KEY UPDATE col_c = 201, col_b = NOW()")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should be expired"
        cur.close()


@cat_upsert.register
class TestCase10RollbackBeforeTTLUpsertInserts(ConcurrentTTLTest):
    """Case 10: ROLLBACK before TTL, INSERT ON DUP KEY does INSERT

    Thread A inserts, sleeps 5s, ROLLBACK.
    Thread B does INSERT ON DUP KEY at T+2s.
    Since A rolled back, B does fresh INSERT with col_c=200.
    """

    @property
    def name(self):
        return "case_10_rollback_before_ttl_upsert_inserts"

    @property
    def description(self):
        return "ROLLBACK before TTL, INSERT ON DUP KEY does fresh INSERT"

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(5)
        cur.execute("ROLLBACK")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT: Should have 1 row"
        for row in results:
            col_a = row[0]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT: Should be Thread B's row col_c=200"
        time.sleep(7)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should expire"
        cur.close()

    def thread_b(self, conn):
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 200) ON DUPLICATE KEY UPDATE col_c = 201, col_b = NOW()")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT: Should have 1 row"
        for row in results:
            col_a = row[0]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT: Should be fresh insert col_c=200"
        time.sleep(8)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should expire"
        cur.close()


@cat_upsert.register
class TestCase11RollbackAfterTTLUpsertInserts(ConcurrentTTLTest):
    """Case 11: ROLLBACK after TTL, INSERT ON DUP KEY does INSERT

    Thread A inserts, sleeps 11s (past TTL), ROLLBACK.
    Thread B does INSERT ON DUP KEY at T+5s.
    B does fresh INSERT with col_c=200.
    """

    @property
    def name(self):
        return "case_11_rollback_after_ttl_upsert_inserts"

    @property
    def description(self):
        return "ROLLBACK after TTL, INSERT ON DUP KEY does fresh INSERT"

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(11)
        cur.execute("ROLLBACK")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT: Should have 1 row"
        for row in results:
            col_a = row[0]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT: Should be Thread B's row col_c=200"
        time.sleep(4)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should expire"
        cur.close()

    def thread_b(self, conn):
        time.sleep(5)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 200) ON DUPLICATE KEY UPDATE col_c = 201, col_b = NOW()")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT: Should have 1 row"
        for row in results:
            col_a = row[0]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT: Should be fresh insert col_c=200"
        time.sleep(5)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should expire"
        cur.close()


@cat_upsert.register
class TestCase12RollbackAfterTTLBothExpire(ConcurrentTTLTest):
    """Case 12: ROLLBACK after TTL, both expire

    Thread A inserts, sleeps 14s (past TTL), ROLLBACK.
    Thread B does INSERT ON DUP KEY at T+2s.
    Both rows expire.
    """

    @property
    def name(self):
        return "case_12_rollback_after_ttl_both_expire"

    @property
    def description(self):
        return "ROLLBACK after TTL, INSERT ON DUP KEY, both expire"

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(14)
        cur.execute("ROLLBACK")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should be expired"
        cur.close()

    def thread_b(self, conn):
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 200) ON DUPLICATE KEY UPDATE col_c = 201, col_b = NOW()")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should be expired"
        cur.close()


###############################################################################
# UPDATE Tests - Migrated from ttl_test.py cases 13-18
###############################################################################
#
# These tests verify UPDATE behavior with TTL:
# - Case 13: UPDATE succeeds when row not expired
# - Case 14: UPDATE finds no row (row expired when B tries)
# - Case 15: UPDATE finds no row (A commits after TTL)
# - Case 16: ROLLBACK before TTL, UPDATE finds no row
# - Case 17: ROLLBACK after TTL, UPDATE finds no row
# - Case 18: ROLLBACK after TTL (13s), UPDATE finds no row
#

@cat_update.register
class TestCase13UpdateSucceeds(ConcurrentTTLTest):
    """Case 13: UPDATE succeeds when row not expired

    Thread A inserts, sleeps 5s, COMMIT (row not expired).
    Thread B does UPDATE at T+2s, updates col_c to 200.
    Both see col_c=200.
    """

    @property
    def name(self):
        return "case_13_update_succeeds"

    @property
    def description(self):
        return "UPDATE succeeds when row not expired"

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(5)
        cur.execute("COMMIT")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT: Should have 1 row"
        for row in results:
            col_a = row[0]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT: Should be updated to col_c=200"
        time.sleep(5)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should expire"
        cur.close()

    def thread_b(self, conn):
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("UPDATE sz SET col_c = 200 WHERE col_a = 1")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 1 and matched_rows == 1, "ASSERT: Should update 1 row"
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT: Should have 1 row"
        for row in results:
            col_a = row[0]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT: Should be col_c=200"
        time.sleep(6)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should expire"
        cur.close()


@cat_update.register
class TestCase14UpdateFindsNoRowExpired(ConcurrentTTLTest):
    """Case 14: UPDATE finds no row (row expired when B tries)

    Thread A inserts, sleeps 11s (past TTL), COMMIT.
    Thread B does UPDATE at T+2s, finds no row (expired).
    """

    @property
    def name(self):
        return "case_14_update_finds_no_row_expired"

    @property
    def description(self):
        return "UPDATE finds no row when row expired"

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(11)
        cur.execute("COMMIT")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should be expired"
        cur.close()

    def thread_b(self, conn):
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("UPDATE sz SET col_c = 200 WHERE col_a = 1")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 0 and matched_rows == 0, "ASSERT: Should update 0 rows"
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Should have 0 rows"
        cur.close()


@cat_update.register
class TestCase15UpdateFindsNoRowBothExpire(ConcurrentTTLTest):
    """Case 15: UPDATE finds no row (A commits after 13s)

    Thread A inserts, sleeps 13s (past TTL), COMMIT.
    Thread B does UPDATE at T+2s, finds no row (expired).
    """

    @property
    def name(self):
        return "case_15_update_finds_no_row_both_expire"

    @property
    def description(self):
        return "UPDATE finds no row, both expire"

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(13)
        cur.execute("COMMIT")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should be expired"
        cur.close()

    def thread_b(self, conn):
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("UPDATE sz SET col_c = 200 WHERE col_a = 1")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 0 and matched_rows == 0, "ASSERT: Should update 0 rows"
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Should have 0 rows"
        cur.close()


@cat_update.register
class TestCase16RollbackBeforeTTLUpdateNoRow(ConcurrentTTLTest):
    """Case 16: ROLLBACK before TTL, UPDATE finds no row

    Thread A inserts, sleeps 5s, ROLLBACK.
    Thread B does UPDATE at T+2s, finds no row (A rolled back).
    """

    @property
    def name(self):
        return "case_16_rollback_before_ttl_update_no_row"

    @property
    def description(self):
        return "ROLLBACK before TTL, UPDATE finds no row"

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(5)
        cur.execute("ROLLBACK")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Should have 0 rows"
        cur.close()

    def thread_b(self, conn):
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("UPDATE sz SET col_c = 200 WHERE col_a = 1")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 0 and matched_rows == 0, "ASSERT: Should update 0 rows"
        cur.execute("COMMIT")
        time.sleep(6)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Should have 0 rows"
        cur.close()


@cat_update.register
class TestCase17RollbackAfterTTLUpdateNoRow(ConcurrentTTLTest):
    """Case 17: ROLLBACK after TTL, UPDATE finds no row

    Thread A inserts, sleeps 11s (past TTL), ROLLBACK.
    Thread B does UPDATE at T+2s, finds no row.
    """

    @property
    def name(self):
        return "case_17_rollback_after_ttl_update_no_row"

    @property
    def description(self):
        return "ROLLBACK after TTL, UPDATE finds no row"

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(11)
        cur.execute("ROLLBACK")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Should have 0 rows"
        cur.close()

    def thread_b(self, conn):
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("UPDATE sz SET col_c = 200 WHERE col_a = 1")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 0 and matched_rows == 0, "ASSERT: Should update 0 rows"
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Should have 0 rows"
        cur.close()


@cat_update.register
class TestCase18RollbackAfterTTLUpdateNoRowBothExpire(ConcurrentTTLTest):
    """Case 18: ROLLBACK after TTL (13s), UPDATE finds no row

    Thread A inserts, sleeps 13s (past TTL), ROLLBACK.
    Thread B does UPDATE at T+2s, finds no row.
    """

    @property
    def name(self):
        return "case_18_rollback_after_ttl_update_no_row_both_expire"

    @property
    def description(self):
        return "ROLLBACK after TTL (13s), UPDATE finds no row"

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(13)
        cur.execute("ROLLBACK")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Should have 0 rows"
        cur.close()

    def thread_b(self, conn):
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("UPDATE sz SET col_c = 200 WHERE col_a = 1")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 0 and matched_rows == 0, "ASSERT: Should update 0 rows"
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Should have 0 rows"
        cur.close()


###############################################################################
# DELETE Tests - Migrated from ttl_test.py cases 19-22, 23-25, 27
###############################################################################
#
# These tests verify DELETE behavior with TTL:
# - Case 19: DELETE succeeds when row not expired
# - Case 20: DELETE finds no row (row expired)
# - Case 21: ROLLBACK before TTL, DELETE finds no row
# - Case 22: ROLLBACK after TTL, DELETE finds no row
# - Case 23: DEBUG_SYNC - DELETE row that expires during operation
# - Case 24: DEBUG_SYNC - INSERT ON DUP UPDATE with timing sync
# - Case 25: DEBUG_SYNC - INSERT ON DUP UPDATE with timing sync (variant)
# - Case 27: ttl_expired_rows_visible_in_delete comprehensive test
#

@cat_delete.register
class TestCase19DeleteSucceeds(ConcurrentTTLTest):
    """Case 19: DELETE succeeds when row not expired

    Thread A inserts, sleeps 5s, COMMIT (row not expired).
    Thread B does DELETE at T+2s, deletes 1 row.
    """

    @property
    def name(self):
        return "case_19_delete_succeeds"

    @property
    def description(self):
        return "DELETE succeeds when row not expired"

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(5)
        cur.execute("COMMIT")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should be deleted"
        cur.close()

    def thread_b(self, conn):
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("DELETE FROM sz WHERE col_a = 1")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 1 and matched_rows == 1, "ASSERT: Should delete 1 row"
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Should have 0 rows"
        cur.close()


@cat_delete.register
class TestCase20DeleteFindsNoRowExpired(ConcurrentTTLTest):
    """Case 20: DELETE finds no row (row expired)

    Thread A inserts, sleeps 11s (past TTL), COMMIT.
    Thread B does DELETE at T+2s, finds no row (expired).
    """

    @property
    def name(self):
        return "case_20_delete_finds_no_row_expired"

    @property
    def description(self):
        return "DELETE finds no row when row expired"

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(11)
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should be expired"
        cur.close()

    def thread_b(self, conn):
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("DELETE FROM sz WHERE col_a = 1")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 0 and matched_rows == 0, "ASSERT: Should delete 0 rows"
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Should have 0 rows"
        cur.close()


@cat_delete.register
class TestCase21RollbackBeforeTTLDeleteNoRow(ConcurrentTTLTest):
    """Case 21: ROLLBACK before TTL, DELETE finds no row

    Thread A inserts, sleeps 5s, ROLLBACK.
    Thread B does DELETE at T+2s, finds no row (A rolled back).
    """

    @property
    def name(self):
        return "case_21_rollback_before_ttl_delete_no_row"

    @property
    def description(self):
        return "ROLLBACK before TTL, DELETE finds no row"

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(5)
        cur.execute("ROLLBACK")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Should have 0 rows"
        cur.close()

    def thread_b(self, conn):
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("DELETE FROM sz WHERE col_a = 1")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 0 and matched_rows == 0, "ASSERT: Should delete 0 rows"
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Should have 0 rows"
        cur.close()


@cat_delete.register
class TestCase22RollbackAfterTTLDeleteNoRow(ConcurrentTTLTest):
    """Case 22: ROLLBACK after TTL, DELETE finds no row

    Thread A inserts, sleeps 11s (past TTL), ROLLBACK.
    Thread B does DELETE at T+2s, finds no row.
    """

    @property
    def name(self):
        return "case_22_rollback_after_ttl_delete_no_row"

    @property
    def description(self):
        return "ROLLBACK after TTL, DELETE finds no row"

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(11)
        cur.execute("ROLLBACK")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Should have 0 rows"
        cur.close()

    def thread_b(self, conn):
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("DELETE FROM sz WHERE col_a = 1")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 0 and matched_rows == 0, "ASSERT: Should delete 0 rows"
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Should have 0 rows"
        cur.close()


@cat_delete.register
class TestCase23DebugSyncDeleteExpiring(ConcurrentTTLTest):
    """Case 23: DEBUG_SYNC - DELETE row that expires during operation

    Thread A inserts, sleeps 5s, COMMIT, waits for row to expire, signals.
    Thread B waits at DEBUG_SYNC, then DELETEs (row visible but expiring).
    Requires DEBUG_SYNC enabled.
    """

    @property
    def name(self):
        return "case_23_debug_sync_delete_expiring"

    @property
    def description(self):
        return "DEBUG_SYNC: DELETE row that expires during operation"

    @property
    def requires_debug_sync(self):
        return True

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(5)
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT: Should have 1 row"
        time.sleep(6)
        cur.execute("SET DEBUG_SYNC = 'now SIGNAL go_ahead'")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should be deleted"
        cur.close()

    def thread_b(self, conn):
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("SET DEBUG_SYNC = 'ttl_wait_for_row_get_expired_after_reading_4 WAIT_FOR go_ahead'")
        cur.execute("DELETE FROM sz")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 1 and matched_rows == 1, "ASSERT: Should delete 1 row"
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Should have 0 rows"
        cur.close()


@cat_delete.register
class TestCase24DebugSyncUpsertTiming1(ConcurrentTTLTest):
    """Case 24: DEBUG_SYNC - INSERT ON DUP UPDATE with timing sync

    Thread A inserts, sleeps 7s, COMMIT, waits, signals.
    Thread B waits at sync point 1, then does INSERT ON DUP UPDATE.
    Row gets updated to col_c=201.
    Requires DEBUG_SYNC enabled.
    """

    @property
    def name(self):
        return "case_24_debug_sync_upsert_timing_1"

    @property
    def description(self):
        return "DEBUG_SYNC: INSERT ON DUP UPDATE with sync point 1"

    @property
    def requires_debug_sync(self):
        return True

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(7)
        cur.execute("COMMIT")
        time.sleep(4)
        cur.execute("SET DEBUG_SYNC = 'now SIGNAL go_ahead'")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT1: Should have 1 row"
        for row in results:
            col_a = row[0]
            col_c = row[2]
            assert col_a == 1 and col_c == 201, "ASSERT2: Should be col_c=201"
        cur.execute("SELECT * FROM sz")
        time.sleep(4)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT3: Row should expire"
        cur.close()

    def thread_b(self, conn):
        time.sleep(5)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("SET DEBUG_SYNC = 'ttl_wait_for_row_get_expired_after_reading_1 WAIT_FOR go_ahead'")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 200) ON DUPLICATE KEY UPDATE col_c = 201, col_b = NOW()")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 2 and matched_rows == 2, "ASSERT: affected_rows should be 2"
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT1: Should have 1 row"
        for row in results:
            col_a = row[0]
            col_c = row[2]
            assert col_a == 1 and col_c == 201, "ASSERT2: Should be col_c=201"
        time.sleep(5)
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT3: Row should expire"
        cur.close()


@cat_delete.register
class TestCase25DebugSyncUpsertTiming2(ConcurrentTTLTest):
    """Case 25: DEBUG_SYNC - INSERT ON DUP UPDATE with timing sync (variant)

    Thread A inserts, sleeps 7s, COMMIT, waits, signals.
    Thread B waits at sync point 2, then does INSERT ON DUP UPDATE.
    Similar to case 24 but different sync point.
    Requires DEBUG_SYNC enabled.
    """

    @property
    def name(self):
        return "case_25_debug_sync_upsert_timing_2"

    @property
    def description(self):
        return "DEBUG_SYNC: INSERT ON DUP UPDATE with sync point 2"

    @property
    def requires_debug_sync(self):
        return True

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(7)
        cur.execute("COMMIT")
        time.sleep(4)
        cur.execute("SET DEBUG_SYNC = 'now SIGNAL go_ahead'")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT1: Should have 1 row"
        for row in results:
            col_a = row[0]
            col_c = row[2]
            assert col_a == 1 and col_c == 201, "ASSERT2: Should be col_c=201"
        cur.execute("SELECT * FROM sz")
        time.sleep(3)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should expire"
        cur.close()

    def thread_b(self, conn):
        time.sleep(5)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("SET DEBUG_SYNC = 'ttl_wait_for_row_get_expired_after_reading_2 WAIT_FOR go_ahead'")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 200) ON DUPLICATE KEY UPDATE col_c = 201, col_b = NOW()")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 2 and matched_rows == 2, "ASSERT: affected_rows should be 2"
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT1: Should have 1 row"
        for row in results:
            col_a = row[0]
            col_c = row[2]
            assert col_a == 1 and col_c == 201, "ASSERT2: Should be col_c=201"
        time.sleep(5)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should expire"
        cur.close()


@cat_delete.register
class TestCase27TTLExpiredRowsVisibleInDelete(ConcurrentTTLTest):
    """Case 27: Comprehensive test of ttl_expired_rows_visible_in_delete

    Tests DELETE with various WHERE conditions when ttl_expired_rows_visible_in_delete
    is ON vs OFF. Thread B does nothing.
    Tests: full table, PK conditions, AND/OR conditions, LIMIT, etc.
    """

    @property
    def name(self):
        return "case_27_ttl_expired_rows_visible_in_delete"

    @property
    def description(self):
        return "Comprehensive ttl_expired_rows_visible_in_delete test"

    def thread_a(self, conn):
        cur = conn.cursor()

        # Test 1: DELETE FROM sz (full table)
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 101)")
        cur.execute("INSERT INTO sz VALUES(2, SYSDATE(), 102)")
        cur.execute("INSERT INTO sz VALUES(3, SYSDATE(), 103)")
        cur.execute("INSERT INTO sz VALUES(4, SYSDATE(), 104)")
        cur.execute("INSERT INTO sz VALUES(5, SYSDATE(), 105)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT: Should have 5 rows"
        time.sleep(11)
        cur.execute("BEGIN")
        cur.execute("DELETE FROM sz")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 0 and matched_rows == 0, "ASSERT: DELETE with OFF should affect 0 rows"
        cur.execute("SET ttl_expired_rows_visible_in_delete = ON")
        cur.execute("DELETE FROM sz")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 5 and matched_rows == 5, "ASSERT: DELETE with ON should affect 5 rows"
        cur.execute("COMMIT")

        # Test 2: DELETE FROM sz WHERE col_a > 1
        cur.execute("SET ttl_expired_rows_visible_in_delete = OFF")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 101)")
        cur.execute("INSERT INTO sz VALUES(2, SYSDATE(), 102)")
        cur.execute("INSERT INTO sz VALUES(3, SYSDATE(), 103)")
        cur.execute("INSERT INTO sz VALUES(4, SYSDATE(), 104)")
        cur.execute("INSERT INTO sz VALUES(5, SYSDATE(), 105)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT: Should have 5 rows"
        time.sleep(11)
        cur.execute("BEGIN")
        cur.execute("DELETE FROM sz WHERE col_a > 1")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 0 and matched_rows == 0, "ASSERT: DELETE with OFF should affect 0 rows"
        cur.execute("SET ttl_expired_rows_visible_in_delete = ON")
        cur.execute("DELETE FROM sz WHERE col_a > 1")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 4 and matched_rows == 4, "ASSERT: DELETE with ON should affect 4 rows"
        cur.execute("COMMIT")

        # Test 3: DELETE FROM sz WHERE col_a > 1 and col_c < 104
        cur.execute("SET ttl_expired_rows_visible_in_delete = OFF")
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 101)")
        cur.execute("INSERT INTO sz VALUES(2, SYSDATE(), 102)")
        cur.execute("INSERT INTO sz VALUES(3, SYSDATE(), 103)")
        cur.execute("INSERT INTO sz VALUES(4, SYSDATE(), 104)")
        cur.execute("INSERT INTO sz VALUES(5, SYSDATE(), 105)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT: Should have 5 rows"
        time.sleep(11)
        cur.execute("BEGIN")
        cur.execute("DELETE FROM sz WHERE col_a > 1 and col_c < 104")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 0 and matched_rows == 0, "ASSERT: DELETE with OFF should affect 0 rows"
        cur.execute("SET ttl_expired_rows_visible_in_delete = ON")
        cur.execute("DELETE FROM sz WHERE col_a > 1 and col_c < 104")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 2 and matched_rows == 2, "ASSERT: DELETE with ON should affect 2 rows"
        cur.execute("COMMIT")

        # Test 4: DELETE FROM sz WHERE col_a > 1 or col_c < 104
        cur.execute("SET ttl_expired_rows_visible_in_delete = OFF")
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 101)")
        cur.execute("INSERT INTO sz VALUES(2, SYSDATE(), 102)")
        cur.execute("INSERT INTO sz VALUES(3, SYSDATE(), 103)")
        cur.execute("INSERT INTO sz VALUES(4, SYSDATE(), 104)")
        cur.execute("INSERT INTO sz VALUES(5, SYSDATE(), 105)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT: Should have 5 rows"
        time.sleep(11)
        cur.execute("BEGIN")
        cur.execute("DELETE FROM sz WHERE col_a > 1 or col_c < 104")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 0 and matched_rows == 0, "ASSERT: DELETE with OFF should affect 0 rows"
        cur.execute("SET ttl_expired_rows_visible_in_delete = ON")
        cur.execute("DELETE FROM sz WHERE col_a > 1 or col_c < 104")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 5 and matched_rows == 5, "ASSERT: DELETE with ON should affect 5 rows"
        cur.execute("COMMIT")

        # Test 5: DELETE FROM sz WHERE col_c > 101 and col_c < 104
        cur.execute("SET ttl_expired_rows_visible_in_delete = OFF")
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 101)")
        cur.execute("INSERT INTO sz VALUES(2, SYSDATE(), 102)")
        cur.execute("INSERT INTO sz VALUES(3, SYSDATE(), 103)")
        cur.execute("INSERT INTO sz VALUES(4, SYSDATE(), 104)")
        cur.execute("INSERT INTO sz VALUES(5, SYSDATE(), 105)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT: Should have 5 rows"
        time.sleep(11)
        cur.execute("BEGIN")
        cur.execute("DELETE FROM sz WHERE col_c > 101 and col_c < 104")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 0 and matched_rows == 0, "ASSERT: DELETE with OFF should affect 0 rows"
        cur.execute("SET ttl_expired_rows_visible_in_delete = ON")
        cur.execute("DELETE FROM sz WHERE col_c > 101 and col_c < 104")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 2 and matched_rows == 2, "ASSERT: DELETE with ON should affect 2 rows"
        cur.execute("COMMIT")

        # Test 6: DELETE FROM sz WHERE col_c > 101 or col_c < 104
        cur.execute("SET ttl_expired_rows_visible_in_delete = OFF")
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 101)")
        cur.execute("INSERT INTO sz VALUES(2, SYSDATE(), 102)")
        cur.execute("INSERT INTO sz VALUES(3, SYSDATE(), 103)")
        cur.execute("INSERT INTO sz VALUES(4, SYSDATE(), 104)")
        cur.execute("INSERT INTO sz VALUES(5, SYSDATE(), 105)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT: Should have 5 rows"
        time.sleep(11)
        cur.execute("BEGIN")
        cur.execute("DELETE FROM sz WHERE col_c > 101 or col_c < 104")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 0 and matched_rows == 0, "ASSERT: DELETE with OFF should affect 0 rows"
        cur.execute("SET ttl_expired_rows_visible_in_delete = ON")
        cur.execute("DELETE FROM sz WHERE col_c > 101 or col_c < 104")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 5 and matched_rows == 5, "ASSERT: DELETE with ON should affect 5 rows"
        cur.execute("COMMIT")

        # Test 7: DELETE FROM sz WHERE col_c = 103
        cur.execute("SET ttl_expired_rows_visible_in_delete = OFF")
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 101)")
        cur.execute("INSERT INTO sz VALUES(2, SYSDATE(), 102)")
        cur.execute("INSERT INTO sz VALUES(3, SYSDATE(), 103)")
        cur.execute("INSERT INTO sz VALUES(4, SYSDATE(), 104)")
        cur.execute("INSERT INTO sz VALUES(5, SYSDATE(), 105)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT: Should have 5 rows"
        time.sleep(11)
        cur.execute("BEGIN")
        cur.execute("DELETE FROM sz WHERE col_c = 103")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 0 and matched_rows == 0, "ASSERT: DELETE with OFF should affect 0 rows"
        cur.execute("SET ttl_expired_rows_visible_in_delete = ON")
        cur.execute("DELETE FROM sz WHERE col_c = 103")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 1 and matched_rows == 1, "ASSERT: DELETE with ON should affect 1 row"
        cur.execute("COMMIT")

        # Test 8: DELETE FROM sz WHERE col_a = 4
        cur.execute("SET ttl_expired_rows_visible_in_delete = OFF")
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 101)")
        cur.execute("INSERT INTO sz VALUES(2, SYSDATE(), 102)")
        cur.execute("INSERT INTO sz VALUES(3, SYSDATE(), 103)")
        cur.execute("INSERT INTO sz VALUES(4, SYSDATE(), 104)")
        cur.execute("INSERT INTO sz VALUES(5, SYSDATE(), 105)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT: Should have 5 rows"
        time.sleep(11)
        cur.execute("BEGIN")
        cur.execute("DELETE FROM sz WHERE col_a = 4")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 0 and matched_rows == 0, "ASSERT: DELETE with OFF should affect 0 rows"
        cur.execute("SET ttl_expired_rows_visible_in_delete = ON")
        cur.execute("DELETE FROM sz WHERE col_a = 4")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 1 and matched_rows == 1, "ASSERT: DELETE with ON should affect 1 row"
        cur.execute("COMMIT")

        # Test 9: DELETE FROM sz WHERE col_c <= 105 LIMIT 2
        cur.execute("SET ttl_expired_rows_visible_in_delete = OFF")
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 101)")
        cur.execute("INSERT INTO sz VALUES(2, SYSDATE(), 102)")
        cur.execute("INSERT INTO sz VALUES(3, SYSDATE(), 103)")
        cur.execute("INSERT INTO sz VALUES(4, SYSDATE(), 104)")
        cur.execute("INSERT INTO sz VALUES(5, SYSDATE(), 105)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT: Should have 5 rows"
        time.sleep(11)
        cur.execute("BEGIN")
        cur.execute("DELETE FROM sz WHERE col_c <= 105 LIMIT 2")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 0 and matched_rows == 0, "ASSERT: DELETE with OFF should affect 0 rows"
        cur.execute("SET ttl_expired_rows_visible_in_delete = ON")
        cur.execute("DELETE FROM sz WHERE col_c <= 105 LIMIT 2")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 2 and matched_rows == 2, "ASSERT: DELETE LIMIT 2 should affect 2 rows"
        cur.execute("DELETE FROM sz WHERE col_c <= 105 LIMIT 2")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 2 and matched_rows == 2, "ASSERT: DELETE LIMIT 2 should affect 2 more rows"
        cur.execute("DELETE FROM sz WHERE col_c <= 105 LIMIT 2")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 1 and matched_rows == 1, "ASSERT: DELETE LIMIT 2 should affect 1 remaining row"
        cur.execute("COMMIT")

        cur.execute("SET ttl_expired_rows_visible_in_delete = OFF")
        cur.close()

    def thread_b(self, conn):
        # Thread B does nothing in this test
        cur = conn.cursor()
        cur.close()


###############################################################################
# REPLACE INTO Tests - Migrated from ttl_test.py cases 30-35
###############################################################################
#
# These tests verify REPLACE INTO behavior with TTL:
# - Case 30: REPLACE before TTL (replaces existing row)
# - Case 31: REPLACE when A's row expired (inserts new)
# - Case 32: Both rows expire
# - Case 33: ROLLBACK before TTL, REPLACE succeeds
# - Case 34: ROLLBACK after TTL, REPLACE succeeds
# - Case 35: ROLLBACK after TTL, both expire
#

@cat_replace.register
class TestCase30ReplaceBeforeTTL(ConcurrentTTLTest):
    """Case 30: REPLACE before TTL (replaces existing row)

    Thread A inserts, sleeps 5s, COMMIT (row not expired).
    Thread B does REPLACE at T+2s, replaces to col_c=200.
    Both see col_c=200.
    """

    @property
    def name(self):
        return "case_30_replace_before_ttl"

    @property
    def description(self):
        return "REPLACE before TTL replaces existing row"

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(5)
        cur.execute("COMMIT")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT: Should have 1 row"
        for row in results:
            col_a = row[0]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT: Should be replaced to col_c=200"
        time.sleep(7)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should expire"
        cur.close()

    def thread_b(self, conn):
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("REPLACE INTO sz VALUES(1, SYSDATE(), 200)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT: Should have 1 row"
        for row in results:
            col_a = row[0]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT: Should be col_c=200"
        time.sleep(8)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should expire"
        cur.close()


@cat_replace.register
class TestCase31ReplaceWhenExpired(ConcurrentTTLTest):
    """Case 31: REPLACE when A's row expired (inserts new)

    Thread A inserts, sleeps 11s (past TTL), COMMIT.
    Thread B does REPLACE at T+5s (when A's row expired).
    Both see col_c=200.
    """

    @property
    def name(self):
        return "case_31_replace_when_expired"

    @property
    def description(self):
        return "REPLACE when existing row expired inserts new row"

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(11)
        cur.execute("COMMIT")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT: Should have 1 row"
        for row in results:
            col_a = row[0]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT: Should be B's row col_c=200"
        time.sleep(4)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should expire"
        cur.close()

    def thread_b(self, conn):
        time.sleep(5)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("REPLACE INTO sz VALUES(1, SYSDATE(), 200)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT: Should have 1 row"
        for row in results:
            col_a = row[0]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT: Should be col_c=200"
        time.sleep(5)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should expire"
        cur.close()


@cat_replace.register
class TestCase32ReplaceBothExpire(ConcurrentTTLTest):
    """Case 32: Both rows expire

    Thread A inserts, sleeps 13s (past TTL), COMMIT.
    Thread B does REPLACE at T+2s.
    Both rows expire.
    """

    @property
    def name(self):
        return "case_32_replace_both_expire"

    @property
    def description(self):
        return "REPLACE, both rows expire"

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(13)
        cur.execute("COMMIT")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should be expired"
        cur.close()

    def thread_b(self, conn):
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("REPLACE INTO sz VALUES(1, SYSDATE(), 200)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should be expired"
        cur.close()


@cat_replace.register
class TestCase33RollbackBeforeTTLReplace(ConcurrentTTLTest):
    """Case 33: ROLLBACK before TTL, REPLACE succeeds

    Thread A inserts, sleeps 5s, ROLLBACK.
    Thread B does REPLACE at T+2s.
    Both see col_c=200.
    """

    @property
    def name(self):
        return "case_33_rollback_before_ttl_replace"

    @property
    def description(self):
        return "ROLLBACK before TTL, REPLACE succeeds"

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(5)
        cur.execute("ROLLBACK")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT: Should have 1 row"
        for row in results:
            col_a = row[0]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT: Should be B's row col_c=200"
        time.sleep(7)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should expire"
        cur.close()

    def thread_b(self, conn):
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("REPLACE INTO sz VALUES(1, SYSDATE(), 200)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT: Should have 1 row"
        for row in results:
            col_a = row[0]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT: Should be col_c=200"
        time.sleep(8)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should expire"
        cur.close()


@cat_replace.register
class TestCase34RollbackAfterTTLReplace(ConcurrentTTLTest):
    """Case 34: ROLLBACK after TTL, REPLACE succeeds

    Thread A inserts, sleeps 11s (past TTL), ROLLBACK.
    Thread B does REPLACE at T+5s.
    Both see col_c=200.
    """

    @property
    def name(self):
        return "case_34_rollback_after_ttl_replace"

    @property
    def description(self):
        return "ROLLBACK after TTL, REPLACE succeeds"

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(11)
        cur.execute("ROLLBACK")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT: Should have 1 row"
        for row in results:
            col_a = row[0]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT: Should be B's row col_c=200"
        time.sleep(4)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should expire"
        cur.close()

    def thread_b(self, conn):
        time.sleep(5)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("REPLACE INTO sz VALUES(1, SYSDATE(), 200)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT: Should have 1 row"
        for row in results:
            col_a = row[0]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT: Should be col_c=200"
        time.sleep(5)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should expire"
        cur.close()


@cat_replace.register
class TestCase35RollbackAfterTTLBothExpire(ConcurrentTTLTest):
    """Case 35: ROLLBACK after TTL (14s), both expire

    Thread A inserts, sleeps 14s (past TTL), ROLLBACK.
    Thread B does REPLACE at T+2s.
    Both rows expire.
    """

    @property
    def name(self):
        return "case_35_rollback_after_ttl_both_expire"

    @property
    def description(self):
        return "ROLLBACK after TTL, REPLACE, both expire"

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(14)
        cur.execute("ROLLBACK")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should be expired"
        cur.close()

    def thread_b(self, conn):
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("REPLACE INTO sz VALUES(1, SYSDATE(), 200)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should be expired"
        cur.close()


###############################################################################
# BINLOG Tests - Migrated from ttl_test.py cases 36-45
###############################################################################
#
# These tests verify replication/binlog behavior with TTL:
# Thread A operates on primary, Thread B verifies on replica
# - Case 36: Basic replication of INSERT
# - Case 37: Replication when row expires before commit
# - Case 38: Multiple inserts with expiration
# - Case 39: INSERT ON DUP KEY with multiple updates
# - Case 40: FULLY_REPLICATED table (uses sz1)
# - Case 41: Multiple inserts on sz table
# - Case 42: More complex FULLY_REPLICATED (uses sz1)
# - Case 43: Delete and re-insert
# - Case 44: UPDATE on FULLY_REPLICATED (uses sz1)
# - Case 45: Mixed operations
#

@cat_binlog.register
class TestCase36BinlogBasic(ConcurrentTTLTest):
    """Case 36: Basic replication of INSERT

    Thread A: INSERT on primary, verify visible, wait for TTL, verify expired.
    Thread B: Verify on replica at T+5s, verify after TTL, check replication.
    """

    @property
    def name(self):
        return "case_36_binlog_basic"

    @property
    def description(self):
        return "Basic binlog replication of INSERT"

    @property
    def requires_replica(self):
        return True

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT 1: Should have 1 row"
        time.sleep(11)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT 2: Row should expire"
        cur.close()

    def thread_b(self, conn):
        time.sleep(5)
        cur = conn.cursor()
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT 3: Should have 1 row on replica"
        for row in results:
            col_a = row[0]
            assert col_a == 1, "ASSERT 4: col_a should be 1"
        time.sleep(6)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT 5: Row should expire on replica"
        assert check_replication_health(conn) == True, "ASSERT 6: Replication should be healthy"
        cur.close()


@cat_binlog.register
class TestCase37BinlogExpireBeforeCommit(ConcurrentTTLTest):
    """Case 37: Replication when row expires before commit

    Thread A: INSERT, sleep 11s (past TTL), COMMIT.
    Thread B: Verify on replica sees 0 rows, check replication.
    """

    @property
    def name(self):
        return "case_37_binlog_expire_before_commit"

    @property
    def description(self):
        return "Replication when row expires before commit"

    @property
    def requires_replica(self):
        return True

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(11)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT: Should see row in tx"
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Row should be expired"
        cur.close()

    def thread_b(self, conn):
        time.sleep(5)
        cur = conn.cursor()
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Replica should see 0 rows"
        time.sleep(6)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT: Still 0 rows"
        assert check_replication_health(conn) == True, "ASSERT: Replication healthy"
        cur.close()


@cat_binlog.register
class TestCase38BinlogMultipleInserts(ConcurrentTTLTest):
    """Case 38: Multiple inserts with expiration

    Thread A: INSERT, expire, INSERT again, expire, INSERT third time.
    Thread B: Track changes on replica, verify each state, check replication.
    """

    @property
    def name(self):
        return "case_38_binlog_multiple_inserts"

    @property
    def description(self):
        return "Multiple inserts with expiration on replica"

    @property
    def requires_replica(self):
        return True

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(11)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        time.sleep(2)
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 200)")
        cur.execute("COMMIT")
        time.sleep(11)
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 300)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_c = row[2]
            assert col_c == 300, "ASSERT"
        cur.close()

    def thread_b(self, conn):
        time.sleep(5)
        cur = conn.cursor()
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        time.sleep(6)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        assert check_replication_health(conn) == True, "ASSERT"

        time.sleep(4)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_c = row[2]
            assert col_c == 200, "ASSERT"
        time.sleep(11)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_c = row[2]
            assert col_c == 300, "ASSERT"
        time.sleep(11)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        assert check_replication_health(conn) == True, "ASSERT"
        cur.close()


@cat_binlog.register
class TestCase39BinlogUpsertMultiple(ConcurrentTTLTest):
    """Case 39: INSERT ON DUP KEY with multiple updates

    Thread A: INSERT ON DUP multiple times with timing.
    Thread B: Verify changes replicate correctly.
    """

    @property
    def name(self):
        return "case_39_binlog_upsert_multiple"

    @property
    def description(self):
        return "INSERT ON DUP KEY multiple times with replication"

    @property
    def requires_replica(self):
        return True

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100) ON DUPLICATE KEY UPDATE col_c = 101")
        time.sleep(3)
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 200) ON DUPLICATE KEY UPDATE col_c = 201")
        time.sleep(11)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        time.sleep(2)
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 300) ON DUPLICATE KEY UPDATE col_c = 301")
        cur.execute("COMMIT")
        time.sleep(4)
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 400) ON DUPLICATE KEY UPDATE col_c = 401")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_c = row[2]
            assert col_c == 401, "ASSERT"
        cur.close()

    def thread_b(self, conn):
        time.sleep(4)
        cur = conn.cursor()
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT1"
        time.sleep(11)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT2"
        assert check_replication_health(conn) == True, "ASSERT3"

        time.sleep(4)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT4"
        for row in results:
            col_c = row[2]
            assert col_c == 300, "ASSERT5"
        time.sleep(5)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT6"
        for row in results:
            col_c = row[2]
            assert col_c == 401, "ASSERT7"
        time.sleep(4)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT8"
        assert check_replication_health(conn) == True, "ASSERT9"
        cur.close()


###############################################################################
# BINLOG Tests Continued - Cases 40-45
###############################################################################

@cat_binlog.register
class TestCase40BinlogFullyReplicatedUpsert(ConcurrentTTLTest):
    """Case 40: FULLY_REPLICATED table with INSERT ON DUP KEY

    Uses sz1 table with FULLY_REPLICATED=1.
    Thread A: Multiple INSERT ON DUP KEY operations.
    Thread B: Verify on replica.
    """

    @property
    def name(self):
        return "case_40_binlog_fully_replicated_upsert"

    @property
    def description(self):
        return "FULLY_REPLICATED table with INSERT ON DUP KEY"

    @property
    def uses_global_sz(self):
        return False

    @property
    def requires_replica(self):
        return True

    def setup(self):
        super().setup()
        cur = self.primary_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS sz1")
        cur.execute("CREATE TABLE test.sz1 ("
                    "col_a INT, "
                    "col_b TIMESTAMP, "
                    "col_c INT)"
                    "ENGINE = NDB, "
                    "COMMENT=\"NDB_TABLE=FULLY_REPLICATED=1,TTL=10@col_b\"")
        cur.close()

    def teardown(self):
        cur = self.primary_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS sz1")
        cur.close()
        super().teardown()

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz1 VALUES(1, SYSDATE(), 100) ON DUPLICATE KEY UPDATE col_c = 101")
        time.sleep(3)
        cur.execute("INSERT INTO sz1 VALUES(1, SYSDATE(), 200) ON DUPLICATE KEY UPDATE col_c = 201")
        time.sleep(11)
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 2, "ASSERT"
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        time.sleep(2)
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz1 VALUES(1, SYSDATE(), 300) ON DUPLICATE KEY UPDATE col_c = 301")
        cur.execute("COMMIT")
        time.sleep(6)
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz1 VALUES(1, SYSDATE(), 400) ON DUPLICATE KEY UPDATE col_c = 401")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 2, "ASSERT"
        cur.close()

    def thread_b(self, conn):
        time.sleep(5)
        cur = conn.cursor()
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT1"
        time.sleep(11)
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT2"
        assert check_replication_health(conn) == True, "ASSERT3"

        time.sleep(4)
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT4"
        for row in results:
            col_c = row[2]
            assert col_c == 300, "ASSERT5"
        time.sleep(4)
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 2, "ASSERT6"
        time.sleep(10)
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT7"
        assert check_replication_health(conn) == True, "ASSERT8"
        cur.close()


@cat_binlog.register
class TestCase41BinlogMultipleReplaceInserts(ConcurrentTTLTest):
    """Case 41: Multiple REPLACE INTO operations

    Thread A: INSERT, REPLACE, wait for TTL, REPLACE again.
    Thread B: Verify on replica.
    """

    @property
    def name(self):
        return "case_41_binlog_multiple_replace"

    @property
    def description(self):
        return "Multiple REPLACE INTO operations with replication"

    @property
    def requires_replica(self):
        return True

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        time.sleep(5)
        cur.execute("BEGIN")
        cur.execute("REPLACE INTO sz VALUES(1, SYSDATE(), 200)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_c = row[2]
            assert col_c == 200, "ASSERT"
        time.sleep(11)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"

        time.sleep(2)
        cur.execute("BEGIN")
        cur.execute("REPLACE INTO sz VALUES(1, SYSDATE(), 300)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_c = row[2]
            assert col_c == 300, "ASSERT"
        cur.close()

    def thread_b(self, conn):
        time.sleep(6)
        cur = conn.cursor()
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_c = row[2]
            assert col_c == 200, "ASSERT"
        time.sleep(11)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        assert check_replication_health(conn) == True, "ASSERT"

        time.sleep(2)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_c = row[2]
            assert col_c == 300, "ASSERT"
        time.sleep(11)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        assert check_replication_health(conn) == True, "ASSERT"
        cur.close()


@cat_binlog.register
class TestCase42BinlogFullyReplicatedReplace(ConcurrentTTLTest):
    """Case 42: FULLY_REPLICATED with REPLACE INTO

    Uses sz1 table with FULLY_REPLICATED=1.
    Thread A: INSERT, REPLACE, wait for TTL, REPLACE again.
    Thread B: Verify on replica.
    """

    @property
    def name(self):
        return "case_42_binlog_fully_replicated_replace"

    @property
    def description(self):
        return "FULLY_REPLICATED table with REPLACE INTO"

    @property
    def uses_global_sz(self):
        return False

    @property
    def requires_replica(self):
        return True

    def setup(self):
        super().setup()
        cur = self.primary_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS sz1")
        cur.execute("CREATE TABLE test.sz1 ("
                    "col_a INT, "
                    "col_b TIMESTAMP, "
                    "col_c INT)"
                    "ENGINE = NDB, "
                    "COMMENT=\"NDB_TABLE=FULLY_REPLICATED=1,TTL=10@col_b\"")
        cur.close()

    def teardown(self):
        cur = self.primary_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS sz1")
        cur.close()
        super().teardown()

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz1 VALUES(1, SYSDATE(), 100)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        time.sleep(5)
        cur.execute("BEGIN")
        cur.execute("REPLACE INTO sz1 VALUES(1, SYSDATE(), 200)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 2, "ASSERT"
        time.sleep(11)
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"

        time.sleep(5)
        cur.execute("BEGIN")
        cur.execute("REPLACE INTO sz1 VALUES(1, SYSDATE(), 300)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_c = row[2]
            assert col_c == 300, "ASSERT"
        cur.close()

    def thread_b(self, conn):
        time.sleep(8)
        cur = conn.cursor()
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 2, "ASSERT1"
        time.sleep(9)
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT2"
        assert check_replication_health(conn) == True, "ASSERT3"

        time.sleep(6)
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT4"
        for row in results:
            col_c = row[2]
            assert col_c == 300, "ASSERT5"
        time.sleep(9)
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT6"
        assert check_replication_health(conn) == True, "ASSERT7"
        cur.close()


@cat_binlog.register
class TestCase43BinlogDeleteReinsert(ConcurrentTTLTest):
    """Case 43: Delete and re-insert

    Thread A: INSERT 2 rows, UPDATE, DELETE, verify.
    Thread B: Verify same operations on replica.
    """

    @property
    def name(self):
        return "case_43_binlog_delete_reinsert"

    @property
    def description(self):
        return "Delete and re-insert with replication"

    @property
    def requires_replica(self):
        return True

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        cur.execute("INSERT INTO sz VALUES(2, SYSDATE(), 200)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 2, "ASSERT"
        time.sleep(3)
        cur.execute("BEGIN")
        cur.execute("UPDATE sz SET col_c = 200")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz WHERE col_a = 1")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_c = row[2]
            assert col_c == 200, "ASSERT"
        time.sleep(3)
        cur.execute("BEGIN")
        cur.execute("DELETE FROM sz WHERE col_c >= 100")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.close()

    def thread_b(self, conn):
        time.sleep(1)
        cur = conn.cursor()
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 2, "ASSERT"
        time.sleep(3)
        cur.execute("SELECT * FROM sz WHERE col_a = 1")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_c = row[2]
            assert col_c == 200, "ASSERT"
        assert check_replication_health(conn) == True, "ASSERT"
        time.sleep(3)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        assert check_replication_health(conn) == True, "ASSERT"
        cur.close()


@cat_binlog.register
class TestCase44BinlogMixedTimestamps(ConcurrentTTLTest):
    """Case 44: INSERT with mixed timestamps

    Thread A: INSERT 4 rows with different timestamps (some future).
    Thread B: Verify only non-expired row remains on replica.
    """

    @property
    def name(self):
        return "case_44_binlog_mixed_timestamps"

    @property
    def description(self):
        return "INSERT with mixed timestamps, some future"

    @property
    def requires_replica(self):
        return True

    @property
    def uses_global_sz(self):
        return False  # Use explicit cleanup, not inherited

    def teardown(self):
        # Match original case_44_post: explicit DELETE without ttl flag
        cur = self.primary_conn.cursor()
        cur.execute("BEGIN")
        cur.execute("DELETE FROM sz")
        cur.execute("COMMIT")
        cur.close()
        super().teardown()

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        cur.execute("INSERT INTO sz VALUES(2, SYSDATE(), 200)")
        cur.execute("INSERT INTO sz VALUES(3, DATE_ADD(SYSDATE(), INTERVAL 1 DAY), 300)")
        cur.execute("INSERT INTO sz VALUES(4, SYSDATE(), 400)")
        time.sleep(11)
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.close()

    def thread_b(self, conn):
        time.sleep(12)
        cur = conn.cursor()
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_c = row[2]
            assert col_c == 300, "ASSERT"
        assert check_replication_health(conn) == True, "ASSERT"
        cur.close()


@cat_binlog.register
class TestCase45BinlogMixedOperations(ConcurrentTTLTest):
    """Case 45: Mixed operations with FOR UPDATE and FOR SHARE

    Thread A: INSERT, wait for TTL, then FOR UPDATE/UPDATE/DELETE in transaction.
    Thread B: Verify operations with FOR SHARE/FOR UPDATE.
    """

    @property
    def name(self):
        return "case_45_binlog_mixed_operations"

    @property
    def description(self):
        return "Mixed operations with FOR UPDATE/FOR SHARE"

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, sysdate(), 1), (2, sysdate(), 2), (3, sysdate(), 3), (4, sysdate(), 4), (5, sysdate(), 5), (6, sysdate(), 6), (7, sysdate(), 7), (8, sysdate(), 8), (9, sysdate(), 9), (10, sysdate(), 10)")
        time.sleep(11)
        cur.execute("SELECT * FROM sz WHERE col_c >= 7 FOR UPDATE")
        results = cur.fetchall()
        assert len(results) == 4, "ASSERT1"
        cur.execute("UPDATE sz SET col_c = 100  WHERE col_c <= 3")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 3 and matched_rows == 3, "ASSERT2"
        cur.execute("SELECT * FROM sz WHERE col_a <= 3 FOR SHARE")
        results = cur.fetchall()
        assert len(results) == 3, "ASSERT3"
        for row in results:
            col_c = row[2]
            assert col_c == 100, "ASSERT4"
        cur.execute("DELETE FROM sz WHERE col_c >= 20")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 3 and matched_rows == 3, "ASSERT5"
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 7, "ASSERT6"
        cur.execute("COMMIT")

        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT7"
        cur.close()

    def thread_b(self, conn):
        time.sleep(5)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("SELECT * FROM sz FOR SHARE")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        time.sleep(5)
        cur.execute("SELECT * FROM sz FOR UPDATE")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        time.sleep(5)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.close()


###############################################################################
# READ LOCKED Tests - Migrated from ttl_test.py cases 28, 29, 48, 49
###############################################################################
#
# These tests verify TTL behavior with row-level locking (FOR SHARE):
# - Case 28: FULLY_REPLICATED table with FOR SHARE
# - Case 48: Regular table with UNIQUE KEY and FOR SHARE
# - Case 29: FULLY_REPLICATED with different lock scenarios
# - Case 49: FULLY_REPLICATED + UNIQUE KEY with FOR SHARE
#

# Create a new category for READ LOCKED tests
cat_read_locked = TestCategory(
    name="read_locked",
    description="TTL tests with row-level locking (FOR SHARE)"
)
CATEGORIES["read_locked"] = cat_read_locked


@cat_read_locked.register
class TestCase28FullyReplicatedForShare(ConcurrentTTLTest):
    """Case 28: FULLY_REPLICATED table with FOR SHARE locking

    Uses sz1 table with FULLY_REPLICATED=1 and KEY(col_c).
    Thread A: INSERT, various SELECT queries, wait for TTL.
    Thread B: FOR SHARE locking with TTL interaction.
    """

    @property
    def name(self):
        return "case_28_fully_replicated_for_share"

    @property
    def description(self):
        return "FULLY_REPLICATED table with FOR SHARE locking"

    @property
    def uses_global_sz(self):
        return False

    def setup(self):
        super().setup()
        cur = self.primary_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS sz1")
        cur.execute("CREATE TABLE test.sz1 ("
                    "col_a INT, "
                    "col_b TIMESTAMP, "
                    "col_c INT, "
                    "PRIMARY KEY(col_a), "
                    "KEY(col_c))"
                    "ENGINE = NDB, "
                    "COMMENT=\"NDB_TABLE=FULLY_REPLICATED=1,TTL=10@col_b\"")
        cur.close()

    def teardown(self):
        cur = self.primary_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS sz1")
        cur.close()
        super().teardown()

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz1 VALUES(1, sysdate(), 1), (2, sysdate(), 2), (3, sysdate(), 3), (4, sysdate(), 4), (5, sysdate(), 5), (6, sysdate(), 6), (7, sysdate(), 7), (8, sysdate(), 8), (9, sysdate(), 9), (10, sysdate(), 10)")
        cur.execute("COMMIT")
        time.sleep(2)
        cur.execute("BEGIN")
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 10, "ASSERT"
        cur.execute("SELECT * FROM sz1 where col_a = 6")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM sz1 where col_a >= 6")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT"
        cur.execute("SELECT * FROM sz1 where col_c = 6")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM sz1 where col_c <= 6")
        results = cur.fetchall()
        assert len(results) == 6, "ASSERT"
        time.sleep(9)
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"

        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz1 VALUES(1, sysdate(), 1), (2, sysdate(), 2), (3, sysdate(), 3), (4, sysdate(), 4), (5, sysdate(), 5), (6, sysdate(), 6), (7, sysdate(), 7), (8, sysdate(), 8), (9, sysdate(), 9), (10, sysdate(), 10)")
        cur.execute("COMMIT")
        time.sleep(2)
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 10, "ASSERT"
        cur.close()

    def thread_b(self, conn):
        time.sleep(15)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("SELECT * FROM sz1 WHERE col_a <= 5 FOR SHARE")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT"
        time.sleep(7)
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_a = 3")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_a = 8")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_a >= 3")
        results = cur.fetchall()
        assert len(results) == 3, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_a <= 9")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT"

        cur.execute("SELECT * FROM sz1 WHERE col_c = 3")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 8")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c >= 3")
        results = cur.fetchall()
        assert len(results) == 3, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c <= 9")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT"
        cur.execute("COMMIT")

        cur.execute("BEGIN")
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_a = 3")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_a = 8")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_a >= 3")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_a <= 9")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"

        cur.execute("SELECT * FROM sz1 WHERE col_c = 3")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 8")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c >= 3")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c <= 9")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("COMMIT")
        cur.close()


@cat_read_locked.register
class TestCase48UniqueKeyForShare(ConcurrentTTLTest):
    """Case 48: Regular table with UNIQUE KEY and FOR SHARE

    Uses default sz table (has UNIQUE KEY on col_c).
    Thread A: INSERT, various SELECT with unique index, wait for TTL.
    Thread B: FOR SHARE locking with unique index interaction.
    """

    @property
    def name(self):
        return "case_48_unique_key_for_share"

    @property
    def description(self):
        return "Regular table with UNIQUE KEY and FOR SHARE locking"

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, sysdate(), 1), (2, sysdate(), 2), (3, sysdate(), 3), (4, sysdate(), 4), (5, sysdate(), 5), (6, sysdate(), 6), (7, sysdate(), 7), (8, sysdate(), 8), (9, sysdate(), 9), (10, sysdate(), 10)")
        cur.execute("COMMIT")
        time.sleep(2)
        cur.execute("BEGIN")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 10, "ASSERT"
        cur.execute("SELECT * FROM sz where col_a = 6")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM sz where col_a >= 6")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT"
        # = condition here will use the unique index on col_c
        # which acquires lock on the row implicitly.
        cur.execute("SELECT * FROM sz where col_c = 8")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        # <= condition here won't use the unique index on col_c...
        # so no lock acquires
        cur.execute("SELECT * FROM sz where col_c <= 8")
        results = cur.fetchall()
        assert len(results) == 8, "ASSERT"
        time.sleep(9)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        # Here is different with the case_28
        assert len(results) == 1, "ASSERT: Should see 1 row locked by unique index"
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"

        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, sysdate(), 1), (2, sysdate(), 2), (3, sysdate(), 3), (4, sysdate(), 4), (5, sysdate(), 5), (6, sysdate(), 6), (7, sysdate(), 7), (8, sysdate(), 8), (9, sysdate(), 9), (10, sysdate(), 10)")
        cur.execute("COMMIT")
        time.sleep(2)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 10, "ASSERT"
        cur.close()

    def thread_b(self, conn):
        time.sleep(15)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("SELECT * FROM sz WHERE col_a <= 5 FOR SHARE")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT"
        time.sleep(7)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT"
        cur.execute("SELECT * FROM sz WHERE col_a = 3")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM sz WHERE col_a = 8")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz WHERE col_a >= 3")
        results = cur.fetchall()
        assert len(results) == 3, "ASSERT"
        cur.execute("SELECT * FROM sz WHERE col_a <= 9")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT"

        cur.execute("SELECT * FROM sz WHERE col_c = 3")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM sz WHERE col_c = 8")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz WHERE col_c >= 3")
        results = cur.fetchall()
        assert len(results) == 3, "ASSERT"
        cur.execute("SELECT * FROM sz WHERE col_c <= 9")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT"
        cur.execute("COMMIT")

        cur.execute("BEGIN")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz WHERE col_a = 3")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz WHERE col_a = 8")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz WHERE col_a >= 3")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz WHERE col_a <= 9")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"

        cur.execute("SELECT * FROM sz WHERE col_c = 3")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz WHERE col_c = 8")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz WHERE col_c >= 3")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz WHERE col_c <= 9")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("COMMIT")
        cur.close()


@cat_read_locked.register
class TestCase29FullyReplicatedForShareAlt(ConcurrentTTLTest):
    """Case 29: FULLY_REPLICATED with different lock scenarios

    Uses sz1 table with FULLY_REPLICATED=1 and KEY(col_c).
    Similar to case 28 but with different timing/scenarios.
    """

    @property
    def name(self):
        return "case_29_fully_replicated_for_share_alt"

    @property
    def description(self):
        return "FULLY_REPLICATED with different lock scenarios"

    @property
    def uses_global_sz(self):
        return False

    def setup(self):
        super().setup()
        cur = self.primary_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS sz1")
        cur.execute("CREATE TABLE test.sz1 ("
                    "col_a INT, "
                    "col_b TIMESTAMP, "
                    "col_c INT, "
                    "PRIMARY KEY(col_a), "
                    "KEY(col_c))"
                    "ENGINE = NDB, "
                    "COMMENT=\"NDB_TABLE=FULLY_REPLICATED=1,TTL=10@col_b\"")
        cur.close()

    def teardown(self):
        cur = self.primary_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS sz1")
        cur.close()
        super().teardown()

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz1 VALUES(1, sysdate(), 1), (2, sysdate(), 2), (3, sysdate(), 3), (4, sysdate(), 4), (5, sysdate(), 5), (6, sysdate(), 6), (7, sysdate(), 7), (8, sysdate(), 8), (9, sysdate(), 9), (10, sysdate(), 10)")
        cur.execute("COMMIT")
        time.sleep(2)
        cur.execute("BEGIN")
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 10, "ASSERT"
        cur.execute("SELECT * FROM sz1 where col_a = 6")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM sz1 where col_a >= 6")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT"
        cur.execute("SELECT * FROM sz1 where col_c = 6")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM sz1 where col_c <= 6")
        results = cur.fetchall()
        assert len(results) == 6, "ASSERT"
        time.sleep(9)
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"

        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz1 VALUES(1, sysdate(), 1), (2, sysdate(), 2), (3, sysdate(), 3), (4, sysdate(), 4), (5, sysdate(), 5), (6, sysdate(), 6), (7, sysdate(), 7), (8, sysdate(), 8), (9, sysdate(), 9), (10, sysdate(), 10)")
        cur.execute("COMMIT")
        time.sleep(2)
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 10, "ASSERT"
        cur.close()

    def thread_b(self, conn):
        time.sleep(15)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("SELECT * FROM sz1 WHERE col_a <= 5 FOR SHARE")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT"
        time.sleep(7)
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_a = 3")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_a = 8")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_a >= 3")
        results = cur.fetchall()
        assert len(results) == 3, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_a <= 9")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT"

        cur.execute("SELECT * FROM sz1 WHERE col_c = 3")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 8")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c >= 3")
        results = cur.fetchall()
        assert len(results) == 3, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c <= 9")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT"
        cur.execute("COMMIT")

        cur.execute("BEGIN")
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_a = 3")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_a = 8")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_a >= 3")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_a <= 9")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"

        cur.execute("SELECT * FROM sz1 WHERE col_c = 3")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 8")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c >= 3")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c <= 9")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("COMMIT")
        cur.close()


# NOTE: Case 49 is disabled - FULLY_REPLICATED + UNIQUE INDEX causes binlog replication to get stuck
# with "Waiting for dependent transaction to commit" on replica during DROP TABLE.
# The original ttl_test.py also has this case commented out with the same note.
# @cat_read_locked.register
class TestCase49FullyReplicatedUniqueForShare(ConcurrentTTLTest):
    """Case 49: FULLY_REPLICATED + UNIQUE KEY with FOR SHARE

    Uses sz1 table with FULLY_REPLICATED=1 and UNIQUE KEY(col_c).
    Thread A: Various queries using unique index.
    Thread B: FOR SHARE locking with unique index interaction.

    DISABLED: This test causes replication to get stuck due to binlog ordering
    issues with FULLY_REPLICATED + UNIQUE INDEX combination.
    """

    @property
    def name(self):
        return "case_49_fully_replicated_unique_for_share"

    @property
    def description(self):
        return "FULLY_REPLICATED + UNIQUE KEY with FOR SHARE"

    @property
    def uses_global_sz(self):
        return False

    def setup(self):
        super().setup()
        cur = self.primary_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS sz1")
        cur.execute("CREATE TABLE test.sz1 ("
                    "col_a INT, "
                    "col_b TIMESTAMP, "
                    "col_c INT, "
                    "PRIMARY KEY(col_a), "
                    "UNIQUE KEY(col_c))"
                    "ENGINE = NDB, "
                    "COMMENT=\"NDB_TABLE=FULLY_REPLICATED=1,TTL=10@col_b\"")
        cur.close()

    def teardown(self):
        cur = self.primary_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS sz1")
        cur.close()
        super().teardown()

    def thread_a(self, conn):
        # Notice: this case has unique index on col_c
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz1 VALUES(1, sysdate(), 1), (2, sysdate(), 2), (3, sysdate(), 3), (4, sysdate(), 4), (5, sysdate(), 5), (6, sysdate(), 6), (7, sysdate(), 7), (8, sysdate(), 8), (9, sysdate(), 9), (10, sysdate(), 10)")
        cur.execute("COMMIT")
        time.sleep(2)
        cur.execute("BEGIN")
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 10, "ASSERT"
        cur.execute("SELECT * FROM sz1 where col_a = 6")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM sz1 where col_a >= 6")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT"
        # = condition here will use the unique index on col_c
        # which acquires lock on the row implicitly.
        cur.execute("SELECT * FROM sz1 where col_c = 8")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        # <= condition here won't use the unique index on col_c...
        # so no lock acquires
        cur.execute("SELECT * FROM sz1 where col_c <= 8")
        results = cur.fetchall()
        assert len(results) == 8, "ASSERT"
        time.sleep(9)
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        # Here is different with the case_48
        assert len(results) == 1, "ASSERT"
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"

        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz1 VALUES(1, sysdate(), 1), (2, sysdate(), 2), (3, sysdate(), 3), (4, sysdate(), 4), (5, sysdate(), 5), (6, sysdate(), 6), (7, sysdate(), 7), (8, sysdate(), 8), (9, sysdate(), 9), (10, sysdate(), 10)")
        cur.execute("COMMIT")
        time.sleep(2)
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 10, "ASSERT"
        cur.close()

    def thread_b(self, conn):
        time.sleep(15)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("SELECT * FROM sz1 WHERE col_a <= 5 FOR SHARE")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT"
        time.sleep(7)
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_a = 3")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_a = 8")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_a >= 3")
        results = cur.fetchall()
        assert len(results) == 3, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_a <= 9")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT"

        cur.execute("SELECT * FROM sz1 WHERE col_c = 3")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 8")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c >= 3")
        results = cur.fetchall()
        assert len(results) == 3, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c <= 9")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT"
        cur.execute("COMMIT")

        cur.execute("BEGIN")
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_a = 3")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_a = 8")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_a >= 3")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_a <= 9")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"

        cur.execute("SELECT * FROM sz1 WHERE col_c = 3")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 8")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c >= 3")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c <= 9")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("COMMIT")
        cur.close()


###############################################################################
# UNIQUE INDEX/TRIGGER Tests - Cases 47, 50, 53
###############################################################################

# Create a new category for UNIQUE INDEX tests
cat_unique_index = TestCategory(
    name="unique_index",
    description="TTL tests with unique index and triggers"
)
CATEGORIES["unique_index"] = cat_unique_index


@cat_unique_index.register
class TestCase47UniqueIndexReplication(ConcurrentTTLTest):
    """Case 47: Basic unique index test on replica

    Thread A: INSERT, wait for TTL, re-insert same pk with same unique value.
    Thread B: Verify on replica.
    """

    @property
    def name(self):
        return "case_47_unique_index_replication"

    @property
    def description(self):
        return "Unique index behavior with TTL and replication"

    @property
    def requires_replica(self):
        return True

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        cur.execute("COMMIT")
        time.sleep(11)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(2, SYSDATE(), 100)")
        cur.execute("COMMIT")
        time.sleep(11)
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(2, SYSDATE(), 100)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            assert col_a == 2, "ASSERT"
        cur.close()

    def thread_b(self, conn):
        time.sleep(5)
        cur = conn.cursor()
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            assert col_a == 1, "ASSERT"
        time.sleep(10)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            assert col_a == 2, "ASSERT"
        time.sleep(6)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        assert check_replication_health(conn) == True, "ASSERT"
        cur.close()


@cat_unique_index.register
class TestCase50AlterTableAddUniqueIndex(ConcurrentTTLTest):
    """Case 50: ALTER TABLE ADD UNIQUE INDEX

    Thread A: INSERT, wait for TTL, ADD UNIQUE INDEX, verify, then TTL=OFF.
    Thread B: Verify on replica after TTL=OFF.
    """

    @property
    def name(self):
        return "case_50_alter_add_unique_index"

    @property
    def description(self):
        return "ALTER TABLE ADD UNIQUE INDEX with TTL"

    @property
    def uses_global_sz(self):
        return False

    @property
    def requires_replica(self):
        return True

    def setup(self):
        super().setup()
        cur = self.primary_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS sz1")
        cur.execute("CREATE TABLE test.sz1 ("
                    "col_a INT, "
                    "col_b TIMESTAMP, "
                    "col_c INT, "
                    "PRIMARY KEY(col_a))"
                    "ENGINE = NDB, "
                    "COMMENT=\"NDB_TABLE=TTL=10@col_b\"")
        cur.close()

    def teardown(self):
        cur = self.primary_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS sz1")
        cur.close()
        super().teardown()

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz1 VALUES(1, SYSDATE(), 100)")
        cur.execute("INSERT INTO sz1 VALUES(2, SYSDATE(), 200)")
        cur.execute("INSERT INTO sz1 VALUES(3, DATE_ADD(SYSDATE(), INTERVAL 1 DAY), 300)")
        cur.execute("INSERT INTO sz1 VALUES(4, SYSDATE(), 400)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 4, "ASSERT"
        time.sleep(11)
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            assert col_a == 3, "ASSERT"

        cur.execute("ALTER TABLE sz1 ADD UNIQUE INDEX uk(col_c)")
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 100")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 200")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 300")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 400")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"

        cur.execute("ALTER TABLE sz1 comment=\"NDB_TABLE=TTL=OFF\"")
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 4, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 100")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 200")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 300")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 400")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.close()

    def thread_b(self, conn):
        time.sleep(15)
        cur = conn.cursor()
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 4, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 100")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 200")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 300")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 400")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"

        assert check_replication_health(conn) == True, "ASSERT"
        cur.close()


@cat_unique_index.register
class TestCase53AlterTableAddIndex(ConcurrentTTLTest):
    """Case 53: ALTER TABLE ADD INDEX (non-unique)

    Thread A: INSERT, wait for TTL, ADD INDEX, verify, then TTL=OFF.
    Thread B: Verify on replica after TTL=OFF.
    """

    @property
    def name(self):
        return "case_53_alter_add_index"

    @property
    def description(self):
        return "ALTER TABLE ADD INDEX (non-unique) with TTL"

    @property
    def uses_global_sz(self):
        return False

    @property
    def requires_replica(self):
        return True

    def setup(self):
        super().setup()
        cur = self.primary_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS sz1")
        cur.execute("CREATE TABLE test.sz1 ("
                    "col_a INT, "
                    "col_b TIMESTAMP, "
                    "col_c INT, "
                    "PRIMARY KEY(col_a))"
                    "ENGINE = NDB, "
                    "COMMENT=\"NDB_TABLE=TTL=10@col_b\"")
        cur.close()

    def teardown(self):
        cur = self.primary_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS sz1")
        cur.close()
        super().teardown()

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz1 VALUES(1, SYSDATE(), 100)")
        cur.execute("INSERT INTO sz1 VALUES(2, SYSDATE(), 200)")
        cur.execute("INSERT INTO sz1 VALUES(3, DATE_ADD(SYSDATE(), INTERVAL 1 DAY), 300)")
        cur.execute("INSERT INTO sz1 VALUES(4, SYSDATE(), 400)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 4, "ASSERT"
        time.sleep(11)
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            assert col_a == 3, "ASSERT"

        cur.execute("ALTER TABLE sz1 ADD INDEX uk(col_c)")
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 100")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 200")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 300")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 400")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"

        cur.execute("ALTER TABLE sz1 comment=\"NDB_TABLE=TTL=OFF\"")
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 4, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 100")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 200")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 300")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 400")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.close()

    def thread_b(self, conn):
        time.sleep(15)
        cur = conn.cursor()
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 4, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 100")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 200")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 300")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 400")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"

        assert check_replication_health(conn) == True, "ASSERT"
        cur.close()


###############################################################################
# DISK COLUMN Tests - Cases 51, 52
###############################################################################

# Create a new category for DISK COLUMN tests
cat_disk_column = TestCategory(
    name="disk_column",
    description="TTL tests with disk-based columns (requires tablespace ts_1)"
)
CATEGORIES["disk_column"] = cat_disk_column


@cat_disk_column.register
class TestCase51DiskColumnAddUniqueIndex(ConcurrentTTLTest):
    """Case 51: Disk column table with ADD UNIQUE INDEX

    Precondition: tablespace ts_1 must be pre-created.
    Thread A: INSERT with disk column, wait for TTL, ADD UNIQUE INDEX, TTL=OFF.
    Thread B: Verify on replica after TTL=OFF.
    """

    @property
    def name(self):
        return "case_51_disk_column_add_unique_index"

    @property
    def description(self):
        return "Disk column table with ADD UNIQUE INDEX"

    @property
    def uses_global_sz(self):
        return False

    @property
    def requires_replica(self):
        return True

    def setup(self):
        # Precondition: ts_1 is pre created
        super().setup()
        cur = self.primary_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS sz1")
        cur.execute("CREATE TABLE test.sz1 ("
                    "col_a INT, "
                    "col_b TIMESTAMP, "
                    "col_c INT, "
                    "col_d INT STORAGE DISK, "
                    "PRIMARY KEY(col_a))"
                    "ENGINE = NDB, TABLESPACE ts_1, "
                    "COMMENT=\"NDB_TABLE=TTL=10@col_b\"")
        cur.close()

    def teardown(self):
        cur = self.primary_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS sz1")
        cur.close()
        super().teardown()

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz1 VALUES(1, SYSDATE(), 100, 10000)")
        cur.execute("INSERT INTO sz1 VALUES(2, SYSDATE(), 200, 20000)")
        cur.execute("INSERT INTO sz1 VALUES(3, DATE_ADD(SYSDATE(), INTERVAL 1 DAY), 300, 30000)")
        cur.execute("INSERT INTO sz1 VALUES(4, SYSDATE(), 400, 40000)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 4, "ASSERT"
        time.sleep(11)
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            assert col_a == 3, "ASSERT"

        cur.execute("ALTER TABLE sz1 ADD UNIQUE INDEX uk(col_c)")
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 100")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 200")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 300")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 400")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"

        cur.execute("ALTER TABLE sz1 comment=\"NDB_TABLE=TTL=OFF\"")
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 4, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 100")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 200")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 300")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 400")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.close()

    def thread_b(self, conn):
        time.sleep(15)
        cur = conn.cursor()
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 4, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 100")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 200")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 300")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 400")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"

        assert check_replication_health(conn) == True, "ASSERT"
        cur.close()


@cat_disk_column.register
class TestCase52DiskColumnAlterRestrictions(ConcurrentTTLTest):
    """Case 52: Disk column table ALTER restrictions

    Precondition: tablespace ts_1 must be pre-created.
    Thread A: INSERT with disk column, try invalid ALTERs (expect errors).
    Thread B: Verify rows expire on replica.
    """

    @property
    def name(self):
        return "case_52_disk_column_alter_restrictions"

    @property
    def description(self):
        return "Disk column table ALTER restrictions"

    @property
    def uses_global_sz(self):
        return False

    @property
    def requires_replica(self):
        return True

    def setup(self):
        # Precondition: ts_1 is pre created
        super().setup()
        cur = self.primary_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS sz1")
        cur.execute("CREATE TABLE test.sz1 ("
                    "col_a INT, "
                    "col_b TIMESTAMP, "
                    "col_c INT, "
                    "col_d INT STORAGE DISK, "
                    "PRIMARY KEY(col_a))"
                    "ENGINE = NDB, TABLESPACE ts_1, "
                    "COMMENT=\"NDB_TABLE=TTL=10@col_b\"")
        cur.close()

    def teardown(self):
        cur = self.primary_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS sz1")
        cur.close()
        super().teardown()

    def thread_a(self, conn):
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz1 VALUES(1, SYSDATE(), 100, 10000)")
        cur.execute("INSERT INTO sz1 VALUES(2, SYSDATE(), 200, 20000)")
        cur.execute("INSERT INTO sz1 VALUES(3, DATE_ADD(SYSDATE(), INTERVAL 1 DAY), 300, 30000)")
        cur.execute("INSERT INTO sz1 VALUES(4, SYSDATE(), 400, 40000)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 4, "ASSERT"

        # Try invalid ALTER - changing TTL column to DISK (should fail with error 1478)
        try:
            cur.execute("ALTER TABLE sz1 CHANGE COLUMN col_b col_b INT STORAGE DISK")
            assert False, "Should have raised error"
        except Exception as e:
            if e.args[0] != 1478:
                raise

        # Try invalid ALTER - changing table storage (should fail with error 1478)
        try:
            cur.execute("ALTER TABLE sz1 STORAGE DISK")
            assert False, "Should have raised error"
        except Exception as e:
            if e.args[0] != 1478:
                raise

        cur.close()

    def thread_b(self, conn):
        time.sleep(11)
        cur = conn.cursor()
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"

        assert check_replication_health(conn) == True, "ASSERT"
        cur.close()


###############################################################################
# FOREIGN KEY Tests - Cases 54, 55, 56
###############################################################################

# Create a new category for FOREIGN KEY tests
cat_foreign_key = TestCategory(
    name="foreign_key",
    description="TTL tests with foreign key constraints"
)
CATEGORIES["foreign_key"] = cat_foreign_key


# NOTE: Case 54 is disabled - TTL tables with FK as child not yet supported
# @cat_foreign_key.register
class TestCase54ForeignKeyWithTTLTable(ConcurrentTTLTest):
    """Case 54: Foreign key with TTL table in the middle

    Creates parent, sz1 (TTL), and child tables with FK relationships.
    Tests CASCADE behavior with TTL expiration.
    """

    @property
    def name(self):
        return "case_54_foreign_key_with_ttl"

    @property
    def description(self):
        return "Foreign key with TTL table in the middle"

    @property
    def uses_global_sz(self):
        return False

    @property
    def requires_replica(self):
        return True

    def setup(self):
        super().setup()
        cur = self.primary_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS child")
        cur.execute("DROP TABLE IF EXISTS sz1")
        cur.execute("DROP TABLE IF EXISTS parent")
        cur.execute("CREATE TABLE parent ("
                    "col_a INT, "
                    "col_b_ui INT, "
                    "col_c_ui_upd INT, "
                    "col_d_ui_del INT, "
                    "PRIMARY KEY(col_a), "
                    "UNIQUE KEY(col_b_ui), "
                    "UNIQUE KEY(col_c_ui_upd), "
                    "UNIQUE KEY(col_d_ui_del)) "
                    "ENGINE = NDB")
        cur.execute("CREATE TABLE test.sz1 ("
                    "col_a INT, "
                    "col_t TIMESTAMP, "
                    "col_b_ui INT, "
                    "col_c_ui_upd INT, "
                    "col_d_ui_del INT, "
                    "col_e_ui INT, "
                    "col_f_ui_upd INT, "
                    "col_g_ui_del INT, "
                    "PRIMARY KEY(col_a), "
                    "UNIQUE KEY(col_e_ui), "
                    "UNIQUE KEY(col_f_ui_upd), "
                    "UNIQUE KEY(col_g_ui_del), "
                    "FOREIGN KEY(col_b_ui) REFERENCES parent(col_b_ui) ON UPDATE CASCADE ON DELETE CASCADE, "
                    "FOREIGN KEY(col_c_ui_upd) REFERENCES parent(col_c_ui_upd) ON UPDATE CASCADE ON DELETE CASCADE, "
                    "FOREIGN KEY(col_d_ui_del) REFERENCES parent(col_d_ui_del) ON UPDATE CASCADE ON DELETE CASCADE) "
                    "ENGINE = NDB, "
                    "COMMENT=\"NDB_TABLE=TTL=10@col_t\"")
        cur.execute("CREATE TABLE test.child ("
                    "col_a INT, "
                    "col_e_ui INT, "
                    "col_f_ui_upd INT, "
                    "col_g_ui_del INT, "
                    "PRIMARY KEY(col_a), "
                    "FOREIGN KEY(col_e_ui) REFERENCES sz1(col_e_ui) ON UPDATE CASCADE ON DELETE CASCADE, "
                    "FOREIGN KEY(col_f_ui_upd) REFERENCES sz1(col_f_ui_upd) ON UPDATE CASCADE ON DELETE CASCADE, "
                    "FOREIGN KEY(col_g_ui_del) REFERENCES sz1(col_g_ui_del) ON UPDATE CASCADE ON DELETE CASCADE) "
                    "ENGINE = NDB")
        cur.close()

    def teardown(self):
        cur = self.primary_conn.cursor()
        # Drop in correct order: child first (has FK to sz1), then sz1 (has FK to parent), then parent
        cur.execute("DROP TABLE IF EXISTS child")
        cur.execute("DROP TABLE IF EXISTS sz1")
        cur.execute("DROP TABLE IF EXISTS parent")
        cur.close()
        super().teardown()

    def thread_a(self, conn):
        cur = conn.cursor()
        # Note: original has 'exit' which is a no-op bug. We skip that.
        cur.execute("BEGIN")
        cur.execute("INSERT INTO parent VALUES(1, 11, 12, 13)")
        cur.execute("INSERT INTO parent VALUES(2, 21, 22, 23)")
        cur.execute("INSERT INTO parent VALUES(3, 31, 32, 33)")
        cur.execute("INSERT INTO parent VALUES(4, 41, 42, 43)")
        cur.execute("INSERT INTO parent VALUES(5, 51, 52, 53)")
        cur.execute("INSERT INTO parent VALUES(6, 61, 62, 63)")
        cur.execute("INSERT INTO sz1 VALUES(1, SYSDATE(), 11, 12, 13, 14, 15, 16)")
        cur.execute("INSERT INTO sz1 VALUES(2, SYSDATE(), 21, 22, 23, 24, 25, 26)")
        cur.execute("INSERT INTO sz1 VALUES(3, SYSDATE(), 31, 32, 33, 34, 35, 36)")
        cur.execute("INSERT INTO sz1 VALUES(4, SYSDATE(), 41, 42, 43, 44, 45, 46)")
        cur.execute("INSERT INTO sz1 VALUES(5, SYSDATE(), 51, 52, 53, 54, 55, 56)")
        cur.execute("INSERT INTO sz1 VALUES(6, SYSDATE(), 61, 62, 63, 64, 65, 66)")
        cur.execute("INSERT INTO child VALUES(1, 14, 15, 16)")
        cur.execute("INSERT INTO child VALUES(2, 24, 25, 26)")
        cur.execute("INSERT INTO child VALUES(3, 34, 35, 36)")
        cur.execute("INSERT INTO child VALUES(4, 44, 45, 46)")
        cur.execute("INSERT INTO child VALUES(5, 54, 55, 56)")
        cur.execute("INSERT INTO child VALUES(6, 64, 65, 66)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM parent")
        results = cur.fetchall()
        assert len(results) == 6, "ASSERT"
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 6, "ASSERT"
        cur.execute("SELECT * FROM child")
        results = cur.fetchall()
        assert len(results) == 6, "ASSERT"

        cur.execute("BEGIN")
        cur.execute("UPDATE parent SET col_c_ui_upd = 222 WHERE col_c_ui_upd = 22")
        cur.execute("DELETE FROM parent WHERE col_d_ui_del = 33")
        cur.execute("COMMIT")

        cur.execute("SELECT * FROM parent WHERE col_c_ui_upd = 222")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c_ui_upd = 222")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            assert col_a == 2, "ASSERT"

        cur.execute("SELECT * FROM sz1 WHERE col_d_ui_del = 33")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM child WHERE col_g_ui_del = 36")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"

        cur.execute("BEGIN")
        cur.execute("UPDATE sz1 SET col_f_ui_upd = 445 WHERE col_f_ui_upd = 45")
        cur.execute("DELETE FROM sz1 WHERE col_g_ui_del = 56")
        cur.execute("COMMIT")

        cur.execute("BEGIN")
        cur.execute("SELECT * FROM sz1 WHERE col_f_ui_upd = 445")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM child WHERE col_f_ui_upd = 445")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            assert col_a == 4, "ASSERT"
        cur.execute("COMMIT")

        cur.execute("BEGIN")
        cur.execute("SELECT * FROM sz1 WHERE col_g_ui_del = 56")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM child WHERE col_g_ui_del = 56")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("COMMIT")

        time.sleep(11)

        cur.execute("SELECT * FROM parent")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT1"
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT2"
        cur.execute("SELECT * FROM child")
        results = cur.fetchall()
        assert len(results) == 4, "ASSERT3"

        cur.execute("BEGIN")
        cur.execute("UPDATE parent SET col_c_ui_upd = 112 WHERE col_c_ui_upd = 12")
        cur.execute("DELETE FROM parent WHERE col_d_ui_del = 63")
        cur.execute("COMMIT")

        cur.execute("BEGIN")
        cur.execute("SELECT * FROM sz1 WHERE col_c_ui_upd = 112")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_d_ui_del = 63")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"

        cur.execute("SELECT * FROM child WHERE col_g_ui_del = 66")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("COMMIT")

        cur.execute("BEGIN")
        # crash
        cur.execute("INSERT INTO sz1 VALUES(6, SYSDATE(), 61, 62, 63, 664, 65, 66)")
        cur.execute("COMMIT")
        cur.close()

    def thread_b(self, conn):
        time.sleep(15)
        cur = conn.cursor()
        assert check_replication_health(conn) == True, "ASSERT"
        cur.close()


# NOTE: Case 55 is disabled - TTL tables with FK as parent not yet supported
# @cat_foreign_key.register
class TestCase55ForeignKeyRestrictions(ConcurrentTTLTest):
    """Case 55: Foreign key restrictions with TTL tables

    Tests that FK referencing TTL table is rejected (error 1215).
    """

    @property
    def name(self):
        return "case_55_foreign_key_restrictions"

    @property
    def description(self):
        return "Foreign key restrictions with TTL tables"

    @property
    def uses_global_sz(self):
        return False

    @property
    def requires_replica(self):
        return True

    def setup(self):
        super().setup()
        cur = self.primary_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS child")
        cur.execute("DROP TABLE IF EXISTS other")
        cur.execute("DROP TABLE IF EXISTS sz1")
        cur.execute("DROP TABLE IF EXISTS parent")
        cur.execute("CREATE TABLE parent ("
                    "col_a INT, "
                    "col_b_ui INT, "
                    "col_c_ui_upd INT, "
                    "col_d_ui_del INT, "
                    "PRIMARY KEY(col_a), "
                    "UNIQUE KEY(col_b_ui), "
                    "UNIQUE KEY(col_c_ui_upd), "
                    "UNIQUE KEY(col_d_ui_del)) "
                    "ENGINE = NDB")
        cur.execute("CREATE TABLE test.sz1 ("
                    "col_a INT, "
                    "col_t TIMESTAMP, "
                    "col_b_ui INT, "
                    "col_c_ui_upd INT, "
                    "col_d_ui_del INT, "
                    "col_e_ui INT, "
                    "col_f_ui_upd INT, "
                    "col_g_ui_del INT, "
                    "PRIMARY KEY(col_a), "
                    "UNIQUE KEY(col_e_ui), "
                    "UNIQUE KEY(col_f_ui_upd), "
                    "UNIQUE KEY(col_g_ui_del), "
                    "FOREIGN KEY(col_b_ui) REFERENCES parent(col_b_ui) ON UPDATE CASCADE ON DELETE CASCADE, "
                    "FOREIGN KEY(col_c_ui_upd) REFERENCES parent(col_c_ui_upd) ON UPDATE CASCADE ON DELETE CASCADE, "
                    "FOREIGN KEY(col_d_ui_del) REFERENCES parent(col_d_ui_del) ON UPDATE CASCADE ON DELETE CASCADE) "
                    "ENGINE = NDB, "
                    "COMMENT=\"NDB_TABLE=TTL=10@col_t\"")
        cur.execute("CREATE TABLE test.other ("
                    "col_a INT, "
                    "col_e_ui INT, "
                    "col_f_ui_upd INT, "
                    "col_g_ui_del INT, "
                    "PRIMARY KEY(col_a)) "
                    "ENGINE = NDB")

        # should return error 1215 - Cannot add FK referencing TTL table
        try:
            cur.execute("CREATE TABLE test.child ("
                        "col_a INT, "
                        "col_e_ui INT, "
                        "col_f_ui_upd INT, "
                        "col_g_ui_del INT, "
                        "PRIMARY KEY(col_a), "
                        "FOREIGN KEY(col_e_ui) REFERENCES sz1(col_e_ui) ON UPDATE CASCADE ON DELETE CASCADE, "
                        "FOREIGN KEY(col_f_ui_upd) REFERENCES sz1(col_f_ui_upd) ON UPDATE CASCADE ON DELETE CASCADE, "
                        "FOREIGN KEY(col_g_ui_del) REFERENCES sz1(col_g_ui_del) ON UPDATE CASCADE ON DELETE CASCADE) "
                        "ENGINE = NDB")
            assert False, "Should have raised error 1215"
        except Exception as e:
            if e.args[0] != 1215:
                raise
        cur.close()

    def teardown(self):
        cur = self.primary_conn.cursor()
        # Drop in correct order: other first, then sz1 (has FK to parent), then parent
        cur.execute("DROP TABLE IF EXISTS other")
        cur.execute("DROP TABLE IF EXISTS sz1")
        cur.execute("DROP TABLE IF EXISTS parent")
        cur.close()
        super().teardown()

    def thread_a(self, conn):
        cur = conn.cursor()
        # Note: original has 'exit' which is a no-op bug. We skip that.
        cur.execute("BEGIN")
        cur.execute("INSERT INTO parent VALUES(1, 11, 12, 13)")
        cur.execute("INSERT INTO parent VALUES(2, 21, 22, 23)")
        cur.execute("INSERT INTO parent VALUES(3, 31, 32, 33)")
        cur.execute("INSERT INTO parent VALUES(4, 41, 42, 43)")
        cur.execute("INSERT INTO parent VALUES(5, 51, 52, 53)")
        cur.execute("INSERT INTO parent VALUES(6, 61, 62, 63)")
        cur.execute("INSERT INTO sz1 VALUES(1, SYSDATE(), 11, 12, 13, 14, 15, 16)")
        cur.execute("INSERT INTO sz1 VALUES(2, SYSDATE(), 21, 22, 23, 24, 25, 26)")
        cur.execute("INSERT INTO sz1 VALUES(3, SYSDATE(), 31, 32, 33, 34, 35, 36)")
        cur.execute("INSERT INTO sz1 VALUES(4, SYSDATE(), 41, 42, 43, 44, 45, 46)")
        cur.execute("INSERT INTO sz1 VALUES(5, SYSDATE(), 51, 52, 53, 54, 55, 56)")
        cur.execute("INSERT INTO sz1 VALUES(6, SYSDATE(), 61, 62, 63, 64, 65, 66)")
        cur.execute("INSERT INTO other VALUES(1, 14, 15, 16)")
        cur.execute("INSERT INTO other VALUES(2, 24, 25, 26)")
        cur.execute("INSERT INTO other VALUES(3, 34, 35, 36)")
        cur.execute("INSERT INTO other VALUES(4, 44, 45, 46)")
        cur.execute("INSERT INTO other VALUES(5, 54, 55, 56)")
        cur.execute("INSERT INTO other VALUES(6, 64, 65, 66)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM parent")
        results = cur.fetchall()
        assert len(results) == 6, "ASSERT 1"
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 6, "ASSERT 2"
        cur.execute("SELECT * FROM other")
        results = cur.fetchall()
        assert len(results) == 6, "ASSERT 3"

        cur.execute("BEGIN")
        cur.execute("UPDATE parent SET col_c_ui_upd = 222 WHERE col_c_ui_upd = 22")
        cur.execute("DELETE FROM parent WHERE col_d_ui_del = 33")
        cur.execute("COMMIT")

        cur.execute("SELECT * FROM parent WHERE col_c_ui_upd = 222")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT 4"
        cur.execute("SELECT * FROM sz1 WHERE col_c_ui_upd = 222")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT 5"
        for row in results:
            col_a = row[0]
            assert col_a == 2, "ASSERT"

        cur.execute("SELECT * FROM sz1 WHERE col_d_ui_del = 33")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT 6"

        cur.execute("BEGIN")
        cur.execute("UPDATE sz1 SET col_f_ui_upd = 445 WHERE col_f_ui_upd = 45")
        cur.execute("DELETE FROM sz1 WHERE col_g_ui_del = 56")
        cur.execute("COMMIT")

        cur.execute("BEGIN")
        cur.execute("SELECT * FROM sz1 WHERE col_f_ui_upd = 445")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT 7"
        for row in results:
            col_a = row[0]
            assert col_a == 4, "ASSERT 8"
        cur.execute("COMMIT")

        cur.execute("BEGIN")
        cur.execute("SELECT * FROM sz1 WHERE col_g_ui_del = 56")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT 9"
        cur.execute("COMMIT")

        time.sleep(11)

        cur.execute("SELECT * FROM parent")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT 10"
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT 11"
        cur.execute("SELECT * FROM other")
        results = cur.fetchall()
        assert len(results) == 6, "ASSERT 12"

        cur.execute("BEGIN")
        cur.execute("UPDATE parent SET col_c_ui_upd = 112 WHERE col_c_ui_upd = 12")
        cur.execute("COMMIT")

        cur.execute("BEGIN")
        cur.execute("SELECT * FROM sz1 WHERE col_c_ui_upd = 112")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT 13"

        cur.execute("BEGIN")
        cur.execute("DELETE FROM parent WHERE col_d_ui_del = 63")
        cur.execute("COMMIT")

        cur.execute("BEGIN")
        cur.execute("SET ttl_expired_rows_visible_in_delete = 1")
        cur.execute("DELETE FROM sz1")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 4 and matched_rows == 4, "ASSERT"
        cur.execute("COMMIT")
        cur.execute("SET ttl_expired_rows_visible_in_delete = 0")

        cur.execute("BEGIN")
        cur.execute("DELETE FROM other")
        cur.execute("COMMIT")

        # Try to add FK referencing TTL table - should fail with 1215
        try:
            cur.execute("ALTER TABLE test.other "
                        "ADD FOREIGN KEY(col_e_ui) REFERENCES sz1(col_e_ui) ON UPDATE CASCADE ON DELETE CASCADE, "
                        "ADD FOREIGN KEY(col_f_ui_upd) REFERENCES sz1(col_f_ui_upd) ON UPDATE CASCADE ON DELETE CASCADE, "
                        "ADD FOREIGN KEY(col_g_ui_del) REFERENCES sz1(col_g_ui_del) ON UPDATE CASCADE ON DELETE CASCADE")
            assert False, "Should have raised error 1215"
        except Exception as e:
            if e.args[0] != 1215:
                raise
        cur.close()

    def thread_b(self, conn):
        time.sleep(15)
        cur = conn.cursor()
        assert check_replication_health(conn) == True, "ASSERT"
        cur.close()


@cat_foreign_key.register
class TestCase56ForeignKeyTTLParentRestrictions(ConcurrentTTLTest):
    """Case 56: Foreign key restrictions with TTL parent table

    Tests various FK restrictions when parent table has TTL.
    """

    @property
    def name(self):
        return "case_56_foreign_key_ttl_parent_restrictions"

    @property
    def description(self):
        return "Foreign key restrictions with TTL parent table"

    @property
    def uses_global_sz(self):
        return False

    def setup(self):
        super().setup()
        cur = self.primary_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS child")
        cur.execute("DROP TABLE IF EXISTS sz2")
        cur.execute("DROP TABLE IF EXISTS sz1")
        cur.execute("DROP TABLE IF EXISTS parentttl")
        cur.execute("DROP TABLE IF EXISTS parent")
        cur.execute("CREATE TABLE parent ("
                    "col_a INT, "
                    "col_t TIMESTAMP, "
                    "col_b_ui INT, "
                    "col_c_ui_upd INT, "
                    "col_d_ui_del INT, "
                    "PRIMARY KEY(col_a), "
                    "UNIQUE KEY(col_b_ui), "
                    "UNIQUE KEY(col_c_ui_upd), "
                    "UNIQUE KEY(col_d_ui_del)) "
                    "ENGINE = NDB")
        cur.execute("CREATE TABLE parentttl ("
                    "col_a INT, "
                    "col_t TIMESTAMP, "
                    "col_b_ui INT, "
                    "col_c_ui_upd INT, "
                    "col_d_ui_del INT, "
                    "PRIMARY KEY(col_a), "
                    "UNIQUE KEY(col_b_ui), "
                    "UNIQUE KEY(col_c_ui_upd), "
                    "UNIQUE KEY(col_d_ui_del)) "
                    "ENGINE = NDB, "
                    "comment=\"NDB_TABLE=TTL=10@col_t\"")
        cur.execute("CREATE TABLE sz1 ("
                    "col_a INT, "
                    "col_t TIMESTAMP, "
                    "col_b_ui INT, "
                    "col_c_ui_upd INT, "
                    "col_d_ui_del INT, "
                    "PRIMARY KEY(col_a)) "
                    "ENGINE = NDB")

        cur.execute("CREATE TABLE sz2 ("
                    "col_a INT, "
                    "col_t TIMESTAMP, "
                    "col_b_ui INT, "
                    "col_c_ui_upd INT, "
                    "col_d_ui_del INT, "
                    "PRIMARY KEY(col_a), "
                    "FOREIGN KEY(col_b_ui) REFERENCES parent(col_b_ui) ON UPDATE CASCADE ON DELETE CASCADE, "
                    "FOREIGN KEY(col_c_ui_upd) REFERENCES parent(col_c_ui_upd) ON UPDATE CASCADE ON DELETE CASCADE, "
                    "FOREIGN KEY(col_d_ui_del) REFERENCES parent(col_d_ui_del) ON UPDATE CASCADE ON DELETE CASCADE) "
                    "ENGINE = NDB")

        # should return error 1215 - Cannot add FK referencing TTL table
        try:
            cur.execute("CREATE TABLE child ("
                        "col_a INT, "
                        "col_t TIMESTAMP, "
                        "col_b_ui INT, "
                        "col_c_ui_upd INT, "
                        "col_d_ui_del INT, "
                        "PRIMARY KEY(col_a), "
                        "FOREIGN KEY(col_b_ui) REFERENCES parentttl(col_b_ui) ON UPDATE CASCADE ON DELETE CASCADE, "
                        "FOREIGN KEY(col_c_ui_upd) REFERENCES parentttl(col_c_ui_upd) ON UPDATE CASCADE ON DELETE CASCADE, "
                        "FOREIGN KEY(col_d_ui_del) REFERENCES parentttl(col_d_ui_del) ON UPDATE CASCADE ON DELETE CASCADE) "
                        "ENGINE = NDB")
            assert False, "Should have raised error 1215"
        except Exception as e:
            if e.args[0] != 1215:
                raise
        cur.close()

    def teardown(self):
        cur = self.primary_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS sz1")
        cur.execute("DROP TABLE IF EXISTS sz2")
        cur.execute("DROP TABLE IF EXISTS child")
        cur.execute("DROP TABLE IF EXISTS parent")
        cur.execute("DROP TABLE IF EXISTS parentttl")
        cur.close()
        super().teardown()

    def thread_a(self, conn):
        cur = conn.cursor()

        # Try ALTER to add FK referencing TTL table - should fail with 1215
        try:
            cur.execute("ALTER TABLE sz1 "
                        "ADD FOREIGN KEY(col_b_ui) REFERENCES parentttl(col_b_ui) ON UPDATE CASCADE ON DELETE CASCADE, "
                        "ADD FOREIGN KEY(col_c_ui_upd) REFERENCES parentttl(col_c_ui_upd) ON UPDATE CASCADE ON DELETE CASCADE, "
                        "ADD FOREIGN KEY(col_d_ui_del) REFERENCES parentttl(col_d_ui_del) ON UPDATE CASCADE ON DELETE CASCADE")
            assert False, "Should have raised error 1215"
        except Exception as e:
            if e.args[0] != 1215:
                raise

        # Try to add TTL to table that is FK parent - should fail with 1846
        try:
            cur.execute("ALTER TABLE parent COMMENT=\"NDB_TABLE=TTL=10@col_t\"")
            assert False, "Should have raised error 1846"
        except Exception as e:
            if e.args[0] != 1846:
                raise

        # Try to add TTL to table that is FK child - should fail with 1846
        try:
            cur.execute("ALTER TABLE sz2 COMMENT=\"NDB_TABLE=TTL=10@col_t\"")
            assert False, "Should have raised error 1846"
        except Exception as e:
            if e.args[0] != 1846:
                raise

        # Try to create TTL child table with FK to non-TTL parent - should fail with 1215
        try:
            cur.execute("CREATE TABLE child ("
                        "col_a INT, "
                        "col_t TIMESTAMP, "
                        "col_b_ui INT, "
                        "col_c_ui_upd INT, "
                        "col_d_ui_del INT, "
                        "PRIMARY KEY(col_a), "
                        "FOREIGN KEY(col_b_ui) REFERENCES parent(col_b_ui) ON UPDATE CASCADE ON DELETE CASCADE, "
                        "FOREIGN KEY(col_c_ui_upd) REFERENCES parent(col_c_ui_upd) ON UPDATE CASCADE ON DELETE CASCADE, "
                        "FOREIGN KEY(col_d_ui_del) REFERENCES parent(col_d_ui_del) ON UPDATE CASCADE ON DELETE CASCADE) "
                        "ENGINE = NDB, "
                        "comment=\"NDB_TABLE=TTL=10@col_t\"")
            assert False, "Should have raised error 1215"
        except Exception as e:
            if e.args[0] != 1215:
                raise

        # Try to create TTL child table with FK to TTL parent - should fail with 1215
        try:
            cur.execute("CREATE TABLE child ("
                        "col_a INT, "
                        "col_t TIMESTAMP, "
                        "col_b_ui INT, "
                        "col_c_ui_upd INT, "
                        "col_d_ui_del INT, "
                        "PRIMARY KEY(col_a), "
                        "FOREIGN KEY(col_b_ui) REFERENCES parentttl(col_b_ui) ON UPDATE CASCADE ON DELETE CASCADE, "
                        "FOREIGN KEY(col_c_ui_upd) REFERENCES parentttl(col_c_ui_upd) ON UPDATE CASCADE ON DELETE CASCADE, "
                        "FOREIGN KEY(col_d_ui_del) REFERENCES parentttl(col_d_ui_del) ON UPDATE CASCADE ON DELETE CASCADE) "
                        "ENGINE = NDB, "
                        "comment=\"NDB_TABLE=TTL=10@col_t\"")
            assert False, "Should have raised error 1215"
        except Exception as e:
            if e.args[0] != 1215:
                raise

        cur.close()

    def thread_b(self, conn):
        cur = conn.cursor()
        # Thread B does nothing in this test
        cur.close()


###############################################################################
# BACKUP RESTORE Tests - Case 46
###############################################################################

# Create a new category for BACKUP RESTORE tests
cat_backup_restore = TestCategory(
    name="backup_restore",
    description="TTL tests with backup and restore (STOPS REPLICA)"
)
CATEGORIES["backup_restore"] = cat_backup_restore


def _kill_connections_to_db(config, port, database):
    """Kill all MySQL connections using the given database.

    This prevents DROP DATABASE from blocking on metadata locks held by stale
    connections (e.g. from a previous killed test run or an exception in run()).
    """
    try:
        conn = pymysql.connect(
            host=config.host, port=port,
            user=config.user, password=config.password
        )
        cur = conn.cursor()
        cur.execute(
            "SELECT id FROM information_schema.processlist "
            f"WHERE db = '{database}'"
        )
        for row in cur.fetchall():
            try:
                cur.execute(f"KILL {row[0]}")
            except Exception:
                pass
        cur.close()
        conn.close()
    except Exception:
        pass


def _set_purge_enabled(config, enabled):
    """Enable or disable RDRS TTL purging via the REST API."""
    url = f"http://{config.rdrs_host}:{config.rdrs_port}/0.1.0/ttl-purge/config"
    data = json.dumps({"enabled": enabled}).encode()
    req = urllib.request.Request(url, data=data, method="PUT",
                                 headers={"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass  # RDRS may not be running; tests still work via NDB TTL read filter


def _start_backup(config, mgmd_port):
    """Run 'start backup' via ndb_mgm and return the backup ID.

    Returns the integer backup ID on success, or raises RuntimeError.
    """
    result = subprocess.run(
        [config.bin_dir + "/ndb_mgm",
         f"--ndb-connectstring=127.0.0.1:{mgmd_port}",
         "-e", "start backup"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"ndb_mgm start backup failed (rc={result.returncode}): "
            f"{result.stderr.decode()}"
        )
    output = result.stdout.decode()
    # Output contains a line like: "Backup 1 started ..." or "Backup 1 completed"
    m = re.search(r'Backup\s+(\d+)\s+', output)
    if not m:
        raise RuntimeError(f"Could not parse backup ID from: {output}")
    return int(m.group(1))


def _restore_backup(config, target_mgmd_port, backup_id, data_dir_1, data_dir_2,
                    include_databases=None):
    """Run the 5-step ndb_restore sequence. Raises RuntimeError on failure.

    include_databases: if set, only restore the listed databases (comma-separated
    string). This avoids conflicts with tables that already exist on the target
    cluster (e.g. the global test.sz table).
    """
    ndb_restore = config.bin_dir + "/ndb_restore"
    connectstring = f"--ndb-connectstring=127.0.0.1:{target_mgmd_port}"
    path_1 = f"{data_dir_1}/ndb_data/BACKUP/BACKUP-{backup_id}"
    path_2 = f"{data_dir_2}/ndb_data/BACKUP/BACKUP-{backup_id}"
    bid = f"--backupid={backup_id}"

    # Common args added to every step when filtering by database
    db_filter = [f"--include-databases={include_databases}"] if include_databases else []

    steps = [
        # 1. Restore metadata from node 2
        [ndb_restore, connectstring, "--nodeid=2", bid,
         f"--backup-path={path_1}", "--restore_meta", "--no-restore-disk-objects"] + db_filter,
        # 2. Disable indexes from node 2
        [ndb_restore, connectstring, "--nodeid=2", bid,
         f"--backup-path={path_1}", "--disable-indexes"] + db_filter,
        # 3. Restore data from node 2
        [ndb_restore, connectstring, "--nodeid=2", bid,
         f"--backup-path={path_1}", "--restore-data"] + db_filter,
        # 4. Restore data from node 3
        [ndb_restore, connectstring, "--nodeid=3", bid,
         f"--backup-path={path_2}", "--restore-data"] + db_filter,
        # 5. Rebuild indexes from node 2
        [ndb_restore, connectstring, "--nodeid=2", bid,
         f"--backup-path={path_1}", "--rebuild-indexes"] + db_filter,
    ]

    for i, cmd in enumerate(steps, 1):
        result = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"ndb_restore step {i} failed (rc={result.returncode}): "
                f"{result.stderr.decode()}"
            )


def _wait_for_schema_sync(config, port, database, table, timeout=60):
    """Connect to MySQL and retry until the restored schema is visible.

    Handles MySQL errors 1412 (table definition changed) and
    1049 (unknown database) which are transient after ndb_restore.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            conn = pymysql.connect(
                host=config.host, port=port,
                user=config.user, password=config.password
            )
            cur = conn.cursor()
            cur.execute(f"SELECT * FROM {database}.{table} LIMIT 1")
            cur.fetchall()
            cur.close()
            conn.close()
            return True
        except pymysql.MySQLError as e:
            errno = e.args[0]
            if errno in (1412, 1049):
                time.sleep(5)
                continue
            raise
    return False


@cat_backup_restore.register
class TestCase46BackupRestoreToPrimary(TTLTestBase):
    """Case 46a: Backup TTL tables from primary, drop, restore back to primary.

    Verifies: TTL metadata preserved, expired rows stay gone, valid rows survive.

    This test runs sequentially (not ConcurrentTTLTest) because the DROP DATABASE
    between backup and restore requires all connections to ttl_backup to be closed
    first, which is incompatible with the concurrent thread model.
    """

    @property
    def name(self):
        return "case_46a_backup_restore_to_primary"

    @property
    def description(self):
        return "Backup TTL tables, drop, restore to same cluster"

    def setup(self):
        # Stop replica to avoid conflicts from DROP+ndb_restore cycle
        replica_conn = pymysql.connect(
            host=self.config.host,
            port=self.config.replica_port,
            user=self.config.user,
            password=self.config.password
        )
        rcur = replica_conn.cursor()
        rcur.execute("STOP REPLICA")
        _kill_connections_to_db(self.config, self.config.replica_port, "ttl_backup")
        rcur.execute("DROP DATABASE IF EXISTS ttl_backup")
        rcur.close()
        replica_conn.close()

        _kill_connections_to_db(self.config, self.config.primary_port, "ttl_backup")
        conn = pymysql.connect(
            host=self.config.host,
            port=self.config.primary_port,
            user=self.config.user,
            password=self.config.password
        )
        cur = conn.cursor()
        cur.execute("DROP DATABASE IF EXISTS ttl_backup")
        cur.execute("CREATE DATABASE ttl_backup")
        cur.execute("CREATE TABLE ttl_backup.sz1 ("
                    "col_a INT, col_b TIMESTAMP, col_c INT, "
                    "PRIMARY KEY(col_a), UNIQUE KEY(col_c)) "
                    "ENGINE=NDB, COMMENT=\"NDB_TABLE=TTL=10@col_b\"")
        cur.execute("CREATE TABLE ttl_backup.sz2 ("
                    "col_a INT, col_b TIMESTAMP, col_c INT, "
                    "KEY(col_c)) "
                    "ENGINE=NDB, COMMENT=\"NDB_TABLE=TTL=10@col_b\"")
        cur.execute("CREATE TABLE ttl_backup.sz3 ("
                    "col_a INT, col_b TIMESTAMP, col_c INT, "
                    "PRIMARY KEY(col_a), KEY(col_c)) "
                    "ENGINE=NDB")
        cur.close()
        conn.close()

    def teardown(self):
        try:
            _kill_connections_to_db(self.config, self.config.primary_port, "ttl_backup")
            conn = pymysql.connect(
                host=self.config.host,
                port=self.config.primary_port,
                user=self.config.user,
                password=self.config.password
            )
            cur = conn.cursor()
            cur.execute("DROP DATABASE IF EXISTS ttl_backup")
            cur.close()
            conn.close()
        except Exception:
            pass
        # Re-establish replication
        try:
            primary_conn = pymysql.connect(
                host=self.config.host,
                port=self.config.primary_port,
                user=self.config.user,
                password=self.config.password
            )
            pcur = primary_conn.cursor()
            pcur.execute("SHOW BINARY LOG STATUS")
            binlog_row = pcur.fetchone()
            log_file = binlog_row[0]
            log_pos = binlog_row[1]
            pcur.close()
            primary_conn.close()

            _kill_connections_to_db(self.config, self.config.replica_port, "ttl_backup")
            conn = pymysql.connect(
                host=self.config.host,
                port=self.config.replica_port,
                user=self.config.user,
                password=self.config.password
            )
            cur = conn.cursor()
            cur.execute("DROP DATABASE IF EXISTS ttl_backup")
            cur.execute("STOP REPLICA")
            cur.execute("RESET REPLICA ALL")
            cur.execute(
                "CHANGE REPLICATION SOURCE TO "
                f"SOURCE_HOST='{self.config.host}', "
                f"SOURCE_PORT={self.config.primary_port}, "
                f"SOURCE_USER='{self.config.repl_user}', "
                f"SOURCE_PASSWORD='{self.config.repl_password}', "
                f"SOURCE_LOG_FILE='{log_file}', "
                f"SOURCE_LOG_POS={log_pos}"
            )
            cur.execute("START REPLICA")
            cur.close()
            conn.close()
        except Exception:
            pass

    def run(self):
        # --- Phase 1: Insert, wait for TTL, verify, backup ---
        conn = pymysql.connect(
            host=self.config.host,
            port=self.config.primary_port,
            user=self.config.user,
            password=self.config.password,
            database="ttl_backup"
        )
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz1 VALUES(1, SYSDATE(), 100)")
        cur.execute("INSERT INTO sz1 VALUES(2, SYSDATE(), 200)")
        cur.execute("INSERT INTO sz1 VALUES(3, DATE_ADD(SYSDATE(), INTERVAL 1 DAY), 300)")
        cur.execute("INSERT INTO sz1 VALUES(4, SYSDATE(), 400)")
        cur.execute("INSERT INTO sz2 VALUES(1, SYSDATE(), 100)")
        cur.execute("INSERT INTO sz2 VALUES(2, SYSDATE(), 200)")
        cur.execute("INSERT INTO sz2 VALUES(3, DATE_ADD(SYSDATE(), INTERVAL 1 DAY), 300)")
        cur.execute("INSERT INTO sz2 VALUES(4, SYSDATE(), 400)")
        cur.execute("INSERT INTO sz3 VALUES(1, SYSDATE(), 100)")
        cur.execute("INSERT INTO sz3 VALUES(2, SYSDATE(), 200)")
        cur.execute("INSERT INTO sz3 VALUES(3, DATE_ADD(SYSDATE(), INTERVAL 1 DAY), 300)")
        cur.execute("INSERT INTO sz3 VALUES(4, SYSDATE(), 400)")
        cur.execute("COMMIT")

        # Verify all 4 rows visible
        for tbl in ("sz1", "sz2", "sz3"):
            cur.execute(f"SELECT * FROM {tbl}")
            assert len(cur.fetchall()) == 4, f"Expected 4 rows in {tbl} before TTL"

        # Wait for TTL expiry (TTL=10s)
        time.sleep(11)

        # sz1, sz2: only the future row (col_c=300) should remain visible
        # (expired via NDB read filter; purge is disabled at suite level)
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 1, f"Expected 1 row in sz1 after TTL, got {len(results)}"
        assert results[0][2] == 300, f"Expected col_c=300, got {results[0][2]}"

        cur.execute("SELECT * FROM sz2")
        results = cur.fetchall()
        assert len(results) == 1, f"Expected 1 row in sz2 after TTL, got {len(results)}"
        assert results[0][2] == 300, f"Expected col_c=300, got {results[0][2]}"

        # sz3 (no TTL): all 4 rows remain
        cur.execute("SELECT * FROM sz3")
        results = cur.fetchall()
        assert len(results) == 4, f"Expected 4 rows in sz3 (no TTL), got {len(results)}"

        # Start backup
        backup_id = _start_backup(self.config, self.config.primary_mgmd_port)

        # Close connection to release all metadata locks before DROP
        cur.close()
        conn.close()

        # --- Phase 2: Drop, restore, verify ---
        drop_conn = pymysql.connect(
            host=self.config.host,
            port=self.config.primary_port,
            user=self.config.user,
            password=self.config.password
        )
        drop_cur = drop_conn.cursor()
        drop_cur.execute("DROP DATABASE IF EXISTS ttl_backup")
        drop_cur.close()
        drop_conn.close()

        _restore_backup(
            self.config,
            self.config.primary_mgmd_port,
            backup_id=backup_id,
            data_dir_1=self.config.data_dir_p_1,
            data_dir_2=self.config.data_dir_p_2,
            include_databases="ttl_backup",
        )

        time.sleep(10)

        _wait_for_schema_sync(
            self.config, self.config.primary_port, "ttl_backup", "sz1"
        )

        verify_conn = pymysql.connect(
            host=self.config.host,
            port=self.config.primary_port,
            user=self.config.user,
            password=self.config.password,
            database="ttl_backup"
        )
        cur = verify_conn.cursor()

        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 1, f"Restored sz1: expected 1 row, got {len(results)}"
        assert results[0][2] == 300, f"Restored sz1: expected col_c=300, got {results[0][2]}"

        cur.execute("SELECT * FROM sz2")
        results = cur.fetchall()
        assert len(results) == 1, f"Restored sz2: expected 1 row, got {len(results)}"
        assert results[0][2] == 300, f"Restored sz2: expected col_c=300, got {results[0][2]}"

        cur.execute("SELECT * FROM sz3")
        results = cur.fetchall()
        assert len(results) == 4, f"Restored sz3: expected 4 rows, got {len(results)}"

        cur.close()
        verify_conn.close()


@cat_backup_restore.register
class TestCase46BackupRestoreToReplica(TTLTestBase):
    """Case 46b: Backup TTL tables from primary, restore to replica cluster.

    Verifies: cross-cluster restore, TTL filtering works on replica.
    """

    @property
    def name(self):
        return "case_46b_backup_restore_to_replica"

    @property
    def description(self):
        return "Backup TTL tables from primary, restore to replica cluster"

    @property
    def requires_replica(self):
        return True

    def setup(self):
        # Stop replica to avoid conflicts
        replica_conn = pymysql.connect(
            host=self.config.host,
            port=self.config.replica_port,
            user=self.config.user,
            password=self.config.password
        )
        rcur = replica_conn.cursor()
        rcur.execute("STOP REPLICA")
        _kill_connections_to_db(self.config, self.config.replica_port, "ttl_backup")
        rcur.execute("DROP DATABASE IF EXISTS ttl_backup")
        rcur.close()
        replica_conn.close()

        # Create tables on primary
        _kill_connections_to_db(self.config, self.config.primary_port, "ttl_backup")
        conn = pymysql.connect(
            host=self.config.host,
            port=self.config.primary_port,
            user=self.config.user,
            password=self.config.password
        )
        cur = conn.cursor()
        cur.execute("DROP DATABASE IF EXISTS ttl_backup")
        cur.execute("CREATE DATABASE ttl_backup")
        cur.execute("CREATE TABLE ttl_backup.sz1 ("
                     "col_a INT, col_b TIMESTAMP, col_c INT, "
                     "PRIMARY KEY(col_a), UNIQUE KEY(col_c)) "
                     "ENGINE=NDB, COMMENT=\"NDB_TABLE=TTL=10@col_b\"")
        cur.execute("CREATE TABLE ttl_backup.sz2 ("
                     "col_a INT, col_b TIMESTAMP, col_c INT, "
                     "KEY(col_c)) "
                     "ENGINE=NDB, COMMENT=\"NDB_TABLE=TTL=10@col_b\"")
        cur.execute("CREATE TABLE ttl_backup.sz3 ("
                     "col_a INT, col_b TIMESTAMP, col_c INT, "
                     "PRIMARY KEY(col_a), KEY(col_c)) "
                     "ENGINE=NDB")
        cur.close()
        conn.close()

    def teardown(self):
        try:
            _kill_connections_to_db(self.config, self.config.primary_port, "ttl_backup")
            conn = pymysql.connect(
                host=self.config.host,
                port=self.config.primary_port,
                user=self.config.user,
                password=self.config.password
            )
            cur = conn.cursor()
            cur.execute("DROP DATABASE IF EXISTS ttl_backup")
            cur.close()
            conn.close()
        except Exception:
            pass
        # Re-establish replication: ndb_restore injected tables into the
        # replica NDB dictionary outside of replication, so we need to
        # reset the replica state and reconfigure the source from the
        # primary's current binlog position.
        try:
            # Get current binlog position from primary
            primary_conn = pymysql.connect(
                host=self.config.host,
                port=self.config.primary_port,
                user=self.config.user,
                password=self.config.password
            )
            pcur = primary_conn.cursor()
            pcur.execute("SHOW BINARY LOG STATUS")
            binlog_row = pcur.fetchone()
            log_file = binlog_row[0]
            log_pos = binlog_row[1]
            pcur.close()
            primary_conn.close()

            # Reset and reconfigure replica
            _kill_connections_to_db(self.config, self.config.replica_port, "ttl_backup")
            conn = pymysql.connect(
                host=self.config.host,
                port=self.config.replica_port,
                user=self.config.user,
                password=self.config.password
            )
            cur = conn.cursor()
            cur.execute("DROP DATABASE IF EXISTS ttl_backup")
            cur.execute("STOP REPLICA")
            cur.execute("RESET REPLICA ALL")
            cur.execute(
                "CHANGE REPLICATION SOURCE TO "
                f"SOURCE_HOST='{self.config.host}', "
                f"SOURCE_PORT={self.config.primary_port}, "
                f"SOURCE_USER='{self.config.repl_user}', "
                f"SOURCE_PASSWORD='{self.config.repl_password}', "
                f"SOURCE_LOG_FILE='{log_file}', "
                f"SOURCE_LOG_POS={log_pos}"
            )
            cur.execute("START REPLICA")
            cur.close()
            conn.close()
        except Exception:
            pass

    def run(self):
        # --- Phase 1: Insert, wait for TTL, verify, backup ---
        conn = pymysql.connect(
            host=self.config.host,
            port=self.config.primary_port,
            user=self.config.user,
            password=self.config.password,
            database="ttl_backup"
        )
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz1 VALUES(1, SYSDATE(), 100)")
        cur.execute("INSERT INTO sz1 VALUES(2, SYSDATE(), 200)")
        cur.execute("INSERT INTO sz1 VALUES(3, DATE_ADD(SYSDATE(), INTERVAL 1 DAY), 300)")
        cur.execute("INSERT INTO sz1 VALUES(4, SYSDATE(), 400)")
        cur.execute("INSERT INTO sz2 VALUES(1, SYSDATE(), 100)")
        cur.execute("INSERT INTO sz2 VALUES(2, SYSDATE(), 200)")
        cur.execute("INSERT INTO sz2 VALUES(3, DATE_ADD(SYSDATE(), INTERVAL 1 DAY), 300)")
        cur.execute("INSERT INTO sz2 VALUES(4, SYSDATE(), 400)")
        cur.execute("INSERT INTO sz3 VALUES(1, SYSDATE(), 100)")
        cur.execute("INSERT INTO sz3 VALUES(2, SYSDATE(), 200)")
        cur.execute("INSERT INTO sz3 VALUES(3, DATE_ADD(SYSDATE(), INTERVAL 1 DAY), 300)")
        cur.execute("INSERT INTO sz3 VALUES(4, SYSDATE(), 400)")
        cur.execute("COMMIT")

        # Verify all 4 rows visible
        for tbl in ("sz1", "sz2", "sz3"):
            cur.execute(f"SELECT * FROM {tbl}")
            assert len(cur.fetchall()) == 4, f"Expected 4 rows in {tbl} before TTL"

        # Wait for TTL expiry (TTL=10s)
        time.sleep(11)

        # sz1, sz2: only the future row (col_c=300) should remain
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 1, f"Expected 1 row in sz1 after TTL, got {len(results)}"
        assert results[0][2] == 300, f"Expected col_c=300, got {results[0][2]}"

        cur.execute("SELECT * FROM sz2")
        results = cur.fetchall()
        assert len(results) == 1, f"Expected 1 row in sz2 after TTL, got {len(results)}"
        assert results[0][2] == 300, f"Expected col_c=300, got {results[0][2]}"

        # sz3 (no TTL): all 4 rows remain
        cur.execute("SELECT * FROM sz3")
        results = cur.fetchall()
        assert len(results) == 4, f"Expected 4 rows in sz3 (no TTL), got {len(results)}"

        # Start backup on primary
        backup_id = _start_backup(self.config, self.config.primary_mgmd_port)

        # Close connection to release metadata locks
        cur.close()
        conn.close()

        # --- Phase 2: Restore to replica, verify ---
        _restore_backup(
            self.config,
            self.config.replica_mgmd_port,
            backup_id=backup_id,
            data_dir_1=self.config.data_dir_p_1,
            data_dir_2=self.config.data_dir_p_2,
            include_databases="ttl_backup",
        )

        time.sleep(10)

        _wait_for_schema_sync(
            self.config, self.config.replica_port, "ttl_backup", "sz1"
        )

        verify_conn = pymysql.connect(
            host=self.config.host,
            port=self.config.replica_port,
            user=self.config.user,
            password=self.config.password,
            database="ttl_backup"
        )
        cur = verify_conn.cursor()

        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 1, f"Replica sz1: expected 1 row, got {len(results)}"
        assert results[0][2] == 300, f"Replica sz1: expected col_c=300, got {results[0][2]}"

        cur.execute("SELECT * FROM sz2")
        results = cur.fetchall()
        assert len(results) == 1, f"Replica sz2: expected 1 row, got {len(results)}"
        assert results[0][2] == 300, f"Replica sz2: expected col_c=300, got {results[0][2]}"

        cur.execute("SELECT * FROM sz3")
        results = cur.fetchall()
        assert len(results) == 4, f"Replica sz3: expected 4 rows, got {len(results)}"

        cur.close()
        verify_conn.close()


@cat_backup_restore.register
class TestCase46cTTLMetadataPreservedAfterRestore(TTLTestBase):
    """Case 46c: TTL metadata is actually preserved after restore.

    Existing tests only check row counts. This test verifies that TTL is still
    *active* on restored tables by inserting new rows after restore, waiting for
    TTL expiry, and confirming those new rows become invisible.
    """

    @property
    def name(self):
        return "case_46c_ttl_metadata_preserved_after_restore"

    @property
    def description(self):
        return "TTL is still active on restored tables (insert-after-restore expires)"

    def setup(self):
        # Stop replica to avoid conflicts from DROP+ndb_restore cycle
        replica_conn = pymysql.connect(
            host=self.config.host,
            port=self.config.replica_port,
            user=self.config.user,
            password=self.config.password
        )
        rcur = replica_conn.cursor()
        rcur.execute("STOP REPLICA")
        _kill_connections_to_db(self.config, self.config.replica_port, "ttl_backup")
        rcur.execute("DROP DATABASE IF EXISTS ttl_backup")
        rcur.close()
        replica_conn.close()

        _kill_connections_to_db(self.config, self.config.primary_port, "ttl_backup")
        conn = pymysql.connect(
            host=self.config.host,
            port=self.config.primary_port,
            user=self.config.user,
            password=self.config.password
        )
        cur = conn.cursor()
        cur.execute("DROP DATABASE IF EXISTS ttl_backup")
        cur.execute("CREATE DATABASE ttl_backup")
        cur.execute("CREATE TABLE ttl_backup.sz1 ("
                    "col_a INT, col_b TIMESTAMP, col_c INT, "
                    "PRIMARY KEY(col_a), UNIQUE KEY(col_c)) "
                    "ENGINE=NDB, COMMENT=\"NDB_TABLE=TTL=10@col_b\"")
        cur.close()
        conn.close()

    def teardown(self):
        try:
            _kill_connections_to_db(self.config, self.config.primary_port, "ttl_backup")
            conn = pymysql.connect(
                host=self.config.host,
                port=self.config.primary_port,
                user=self.config.user,
                password=self.config.password
            )
            cur = conn.cursor()
            cur.execute("DROP DATABASE IF EXISTS ttl_backup")
            cur.close()
            conn.close()
        except Exception:
            pass
        # Re-establish replication
        try:
            primary_conn = pymysql.connect(
                host=self.config.host,
                port=self.config.primary_port,
                user=self.config.user,
                password=self.config.password
            )
            pcur = primary_conn.cursor()
            pcur.execute("SHOW BINARY LOG STATUS")
            binlog_row = pcur.fetchone()
            log_file = binlog_row[0]
            log_pos = binlog_row[1]
            pcur.close()
            primary_conn.close()

            _kill_connections_to_db(self.config, self.config.replica_port, "ttl_backup")
            conn = pymysql.connect(
                host=self.config.host,
                port=self.config.replica_port,
                user=self.config.user,
                password=self.config.password
            )
            cur = conn.cursor()
            cur.execute("DROP DATABASE IF EXISTS ttl_backup")
            cur.execute("STOP REPLICA")
            cur.execute("RESET REPLICA ALL")
            cur.execute(
                "CHANGE REPLICATION SOURCE TO "
                f"SOURCE_HOST='{self.config.host}', "
                f"SOURCE_PORT={self.config.primary_port}, "
                f"SOURCE_USER='{self.config.repl_user}', "
                f"SOURCE_PASSWORD='{self.config.repl_password}', "
                f"SOURCE_LOG_FILE='{log_file}', "
                f"SOURCE_LOG_POS={log_pos}"
            )
            cur.execute("START REPLICA")
            cur.close()
            conn.close()
        except Exception:
            pass

    def run(self):
        # --- Phase 1: Insert a future row, backup ---
        conn = pymysql.connect(
            host=self.config.host,
            port=self.config.primary_port,
            user=self.config.user,
            password=self.config.password,
            database="ttl_backup"
        )
        cur = conn.cursor()
        # One row with far-future TTL so it survives through the whole test
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz1 VALUES(1, DATE_ADD(SYSDATE(), INTERVAL 1 DAY), 100)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz1")
        assert len(cur.fetchall()) == 1, "Expected 1 row before backup"

        backup_id = _start_backup(self.config, self.config.primary_mgmd_port)
        cur.close()
        conn.close()

        # --- Phase 2: Drop, restore ---
        drop_conn = pymysql.connect(
            host=self.config.host,
            port=self.config.primary_port,
            user=self.config.user,
            password=self.config.password
        )
        drop_cur = drop_conn.cursor()
        drop_cur.execute("DROP DATABASE IF EXISTS ttl_backup")
        drop_cur.close()
        drop_conn.close()

        _restore_backup(
            self.config,
            self.config.primary_mgmd_port,
            backup_id=backup_id,
            data_dir_1=self.config.data_dir_p_1,
            data_dir_2=self.config.data_dir_p_2,
            include_databases="ttl_backup",
        )

        time.sleep(10)
        _wait_for_schema_sync(
            self.config, self.config.primary_port, "ttl_backup", "sz1"
        )

        # --- Phase 3: Insert new rows AFTER restore, verify TTL is active ---
        verify_conn = pymysql.connect(
            host=self.config.host,
            port=self.config.primary_port,
            user=self.config.user,
            password=self.config.password,
            database="ttl_backup"
        )
        cur = verify_conn.cursor()

        # Seed row from backup should still be visible
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 1, f"Expected 1 restored row, got {len(results)}"

        # Insert new rows: one that will expire (SYSDATE), one that won't (future)
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz1 VALUES(10, SYSDATE(), 1000)")
        cur.execute("INSERT INTO sz1 VALUES(11, DATE_ADD(SYSDATE(), INTERVAL 1 DAY), 1100)")
        cur.execute("COMMIT")

        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 3, f"Expected 3 rows after insert, got {len(results)}"

        # Wait for TTL expiry
        time.sleep(11)

        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        # Row 10 (SYSDATE) should have expired; rows 1 and 11 (future) survive
        assert len(results) == 2, (
            f"Expected 2 rows after TTL (new expired row gone), got {len(results)}"
        )
        col_a_values = sorted(r[0] for r in results)
        assert col_a_values == [1, 11], (
            f"Expected rows [1, 11] to survive, got {col_a_values}"
        )

        cur.close()
        verify_conn.close()


@cat_backup_restore.register
class TestCase46dNullTTLColumnSurvivesRestore(TTLTestBase):
    """Case 46d: Rows with NULL TTL column survive backup-restore.

    NULL in the TTL column means "never expire". Verify these rows are not lost
    or treated as expired during the backup-restore cycle.
    """

    @property
    def name(self):
        return "case_46d_null_ttl_column_survives_restore"

    @property
    def description(self):
        return "Rows with NULL TTL column survive backup-restore"

    def setup(self):
        # Stop replica to avoid conflicts from DROP+ndb_restore cycle
        replica_conn = pymysql.connect(
            host=self.config.host,
            port=self.config.replica_port,
            user=self.config.user,
            password=self.config.password
        )
        rcur = replica_conn.cursor()
        rcur.execute("STOP REPLICA")
        _kill_connections_to_db(self.config, self.config.replica_port, "ttl_backup")
        rcur.execute("DROP DATABASE IF EXISTS ttl_backup")
        rcur.close()
        replica_conn.close()

        _kill_connections_to_db(self.config, self.config.primary_port, "ttl_backup")
        conn = pymysql.connect(
            host=self.config.host,
            port=self.config.primary_port,
            user=self.config.user,
            password=self.config.password
        )
        cur = conn.cursor()
        cur.execute("DROP DATABASE IF EXISTS ttl_backup")
        cur.execute("CREATE DATABASE ttl_backup")
        # col_b is TIMESTAMP NULL so we can insert NULL
        cur.execute("CREATE TABLE ttl_backup.sz1 ("
                    "col_a INT, col_b TIMESTAMP NULL, col_c INT, "
                    "PRIMARY KEY(col_a)) "
                    "ENGINE=NDB, COMMENT=\"NDB_TABLE=TTL=10@col_b\"")
        cur.close()
        conn.close()

    def teardown(self):
        try:
            _kill_connections_to_db(self.config, self.config.primary_port, "ttl_backup")
            conn = pymysql.connect(
                host=self.config.host,
                port=self.config.primary_port,
                user=self.config.user,
                password=self.config.password
            )
            cur = conn.cursor()
            cur.execute("DROP DATABASE IF EXISTS ttl_backup")
            cur.close()
            conn.close()
        except Exception:
            pass
        # Re-establish replication
        try:
            primary_conn = pymysql.connect(
                host=self.config.host,
                port=self.config.primary_port,
                user=self.config.user,
                password=self.config.password
            )
            pcur = primary_conn.cursor()
            pcur.execute("SHOW BINARY LOG STATUS")
            binlog_row = pcur.fetchone()
            log_file = binlog_row[0]
            log_pos = binlog_row[1]
            pcur.close()
            primary_conn.close()

            _kill_connections_to_db(self.config, self.config.replica_port, "ttl_backup")
            conn = pymysql.connect(
                host=self.config.host,
                port=self.config.replica_port,
                user=self.config.user,
                password=self.config.password
            )
            cur = conn.cursor()
            cur.execute("DROP DATABASE IF EXISTS ttl_backup")
            cur.execute("STOP REPLICA")
            cur.execute("RESET REPLICA ALL")
            cur.execute(
                "CHANGE REPLICATION SOURCE TO "
                f"SOURCE_HOST='{self.config.host}', "
                f"SOURCE_PORT={self.config.primary_port}, "
                f"SOURCE_USER='{self.config.repl_user}', "
                f"SOURCE_PASSWORD='{self.config.repl_password}', "
                f"SOURCE_LOG_FILE='{log_file}', "
                f"SOURCE_LOG_POS={log_pos}"
            )
            cur.execute("START REPLICA")
            cur.close()
            conn.close()
        except Exception:
            pass

    def run(self):
        conn = pymysql.connect(
            host=self.config.host,
            port=self.config.primary_port,
            user=self.config.user,
            password=self.config.password,
            database="ttl_backup"
        )
        cur = conn.cursor()

        # Insert rows: some with NULL TTL, some with past TTL, one with future TTL
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz1 VALUES(1, NULL, 100)")             # NULL -> never expire
        cur.execute("INSERT INTO sz1 VALUES(2, NULL, 200)")             # NULL -> never expire
        cur.execute("INSERT INTO sz1 VALUES(3, SYSDATE(), 300)")        # will expire
        cur.execute("INSERT INTO sz1 VALUES(4, DATE_ADD(SYSDATE(), INTERVAL 1 DAY), 400)")  # future
        cur.execute("COMMIT")

        cur.execute("SELECT * FROM sz1")
        assert len(cur.fetchall()) == 4, "Expected 4 rows before TTL"

        # Wait for TTL expiry of row 3
        time.sleep(11)

        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        # Row 3 expired; rows 1, 2 (NULL), and 4 (future) survive
        assert len(results) == 3, f"Expected 3 rows after TTL, got {len(results)}"
        col_a_values = sorted(r[0] for r in results)
        assert col_a_values == [1, 2, 4], f"Expected [1, 2, 4], got {col_a_values}"

        # Backup
        backup_id = _start_backup(self.config, self.config.primary_mgmd_port)
        cur.close()
        conn.close()

        # Drop, restore
        _kill_connections_to_db(self.config, self.config.primary_port, "ttl_backup")
        drop_conn = pymysql.connect(
            host=self.config.host,
            port=self.config.primary_port,
            user=self.config.user,
            password=self.config.password
        )
        drop_cur = drop_conn.cursor()
        drop_cur.execute("DROP DATABASE IF EXISTS ttl_backup")
        drop_cur.close()
        drop_conn.close()

        _restore_backup(
            self.config,
            self.config.primary_mgmd_port,
            backup_id=backup_id,
            data_dir_1=self.config.data_dir_p_1,
            data_dir_2=self.config.data_dir_p_2,
            include_databases="ttl_backup",
        )

        time.sleep(10)
        _wait_for_schema_sync(
            self.config, self.config.primary_port, "ttl_backup", "sz1"
        )

        # Verify NULL rows survived
        verify_conn = pymysql.connect(
            host=self.config.host,
            port=self.config.primary_port,
            user=self.config.user,
            password=self.config.password,
            database="ttl_backup"
        )
        cur = verify_conn.cursor()

        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 3, f"Restored: expected 3 rows, got {len(results)}"
        col_a_values = sorted(r[0] for r in results)
        assert col_a_values == [1, 2, 4], (
            f"Restored: expected [1, 2, 4], got {col_a_values}"
        )

        # Verify the NULL values are actually NULL
        cur.execute("SELECT col_a, col_b FROM sz1 WHERE col_b IS NULL")
        null_rows = cur.fetchall()
        assert len(null_rows) == 2, (
            f"Expected 2 rows with NULL TTL column, got {len(null_rows)}"
        )
        null_col_a = sorted(r[0] for r in null_rows)
        assert null_col_a == [1, 2], f"Expected NULL rows [1, 2], got {null_col_a}"

        cur.close()
        verify_conn.close()


@cat_backup_restore.register
class TestCase46fAlterTTLAfterRestore(TTLTestBase):
    """Case 46f: ALTER TTL after restore.

    Restore a TTL table, then ALTER TABLE ... COMMENT='NDB_TABLE=TTL=...' to
    change or disable TTL. Verifies the restored table's metadata is mutable.
    """

    @property
    def name(self):
        return "case_46f_alter_ttl_after_restore"

    @property
    def description(self):
        return "ALTER TTL (disable, re-enable, change duration) works after restore"

    def setup(self):
        # Stop replica to avoid conflicts from DROP+ndb_restore cycle
        replica_conn = pymysql.connect(
            host=self.config.host,
            port=self.config.replica_port,
            user=self.config.user,
            password=self.config.password
        )
        rcur = replica_conn.cursor()
        rcur.execute("STOP REPLICA")
        _kill_connections_to_db(self.config, self.config.replica_port, "ttl_backup")
        rcur.execute("DROP DATABASE IF EXISTS ttl_backup")
        rcur.close()
        replica_conn.close()

        _kill_connections_to_db(self.config, self.config.primary_port, "ttl_backup")
        conn = pymysql.connect(
            host=self.config.host,
            port=self.config.primary_port,
            user=self.config.user,
            password=self.config.password
        )
        cur = conn.cursor()
        cur.execute("DROP DATABASE IF EXISTS ttl_backup")
        cur.execute("CREATE DATABASE ttl_backup")
        cur.execute("CREATE TABLE ttl_backup.sz1 ("
                    "col_a INT, col_b TIMESTAMP, col_c INT, "
                    "PRIMARY KEY(col_a)) "
                    "ENGINE=NDB, COMMENT=\"NDB_TABLE=TTL=10@col_b\"")
        cur.close()
        conn.close()

    def teardown(self):
        try:
            _kill_connections_to_db(self.config, self.config.primary_port, "ttl_backup")
            conn = pymysql.connect(
                host=self.config.host,
                port=self.config.primary_port,
                user=self.config.user,
                password=self.config.password
            )
            cur = conn.cursor()
            cur.execute("DROP DATABASE IF EXISTS ttl_backup")
            cur.close()
            conn.close()
        except Exception:
            pass
        # Re-establish replication
        try:
            primary_conn = pymysql.connect(
                host=self.config.host,
                port=self.config.primary_port,
                user=self.config.user,
                password=self.config.password
            )
            pcur = primary_conn.cursor()
            pcur.execute("SHOW BINARY LOG STATUS")
            binlog_row = pcur.fetchone()
            log_file = binlog_row[0]
            log_pos = binlog_row[1]
            pcur.close()
            primary_conn.close()

            _kill_connections_to_db(self.config, self.config.replica_port, "ttl_backup")
            conn = pymysql.connect(
                host=self.config.host,
                port=self.config.replica_port,
                user=self.config.user,
                password=self.config.password
            )
            cur = conn.cursor()
            cur.execute("DROP DATABASE IF EXISTS ttl_backup")
            cur.execute("STOP REPLICA")
            cur.execute("RESET REPLICA ALL")
            cur.execute(
                "CHANGE REPLICATION SOURCE TO "
                f"SOURCE_HOST='{self.config.host}', "
                f"SOURCE_PORT={self.config.primary_port}, "
                f"SOURCE_USER='{self.config.repl_user}', "
                f"SOURCE_PASSWORD='{self.config.repl_password}', "
                f"SOURCE_LOG_FILE='{log_file}', "
                f"SOURCE_LOG_POS={log_pos}"
            )
            cur.execute("START REPLICA")
            cur.close()
            conn.close()
        except Exception:
            pass

    def run(self):
        # --- Phase 1: Insert rows, backup ---
        conn = pymysql.connect(
            host=self.config.host,
            port=self.config.primary_port,
            user=self.config.user,
            password=self.config.password,
            database="ttl_backup"
        )
        cur = conn.cursor()

        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz1 VALUES(1, SYSDATE(), 100)")
        cur.execute("INSERT INTO sz1 VALUES(2, DATE_ADD(SYSDATE(), INTERVAL 1 DAY), 200)")
        cur.execute("COMMIT")

        cur.execute("SELECT * FROM sz1")
        assert len(cur.fetchall()) == 2, "Expected 2 rows before backup"

        backup_id = _start_backup(self.config, self.config.primary_mgmd_port)
        cur.close()
        conn.close()

        # --- Phase 2: Drop, restore ---
        _kill_connections_to_db(self.config, self.config.primary_port, "ttl_backup")
        drop_conn = pymysql.connect(
            host=self.config.host,
            port=self.config.primary_port,
            user=self.config.user,
            password=self.config.password
        )
        drop_cur = drop_conn.cursor()
        drop_cur.execute("DROP DATABASE IF EXISTS ttl_backup")
        drop_cur.close()
        drop_conn.close()

        _restore_backup(
            self.config,
            self.config.primary_mgmd_port,
            backup_id=backup_id,
            data_dir_1=self.config.data_dir_p_1,
            data_dir_2=self.config.data_dir_p_2,
            include_databases="ttl_backup",
        )

        time.sleep(10)
        _wait_for_schema_sync(
            self.config, self.config.primary_port, "ttl_backup", "sz1"
        )

        # --- Phase 3: Verify and ALTER TTL ---
        verify_conn = pymysql.connect(
            host=self.config.host,
            port=self.config.primary_port,
            user=self.config.user,
            password=self.config.password,
            database="ttl_backup"
        )
        cur = verify_conn.cursor()

        # Wait for row 1 to expire under original TTL=10
        time.sleep(11)
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 1, f"Expected 1 row after TTL expiry, got {len(results)}"
        assert results[0][0] == 2, f"Expected col_a=2, got {results[0][0]}"

        # --- ALTER 1: Disable TTL ---
        cur.execute("ALTER TABLE sz1 COMMENT=\"NDB_TABLE=TTL=OFF\"")
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        # With TTL=OFF, all physically-present rows become visible
        assert len(results) == 2, (
            f"Expected 2 rows with TTL=OFF, got {len(results)}"
        )

        # --- ALTER 2: Re-enable TTL with original duration ---
        cur.execute("ALTER TABLE sz1 COMMENT=\"NDB_TABLE=TTL=10@col_b\"")
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        # Row 1 is expired again under TTL=10; only row 2 (future) visible
        assert len(results) == 1, (
            f"Expected 1 row after re-enabling TTL=10, got {len(results)}"
        )
        assert results[0][0] == 2, f"Expected col_a=2, got {results[0][0]}"

        # --- ALTER 3: Change TTL to a very long duration ---
        cur.execute("ALTER TABLE sz1 COMMENT=\"NDB_TABLE=TTL=86400@col_b\"")
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        # With TTL=86400 (1 day), row 1 (inserted ~20s ago) is within TTL again
        assert len(results) == 2, (
            f"Expected 2 rows with TTL=86400, got {len(results)}"
        )

        cur.close()
        verify_conn.close()


@cat_backup_restore.register
class TestCase46gDiskColumnBackupRestore(TTLTestBase):
    """Case 46g: Backup-restore with disk columns + TTL.

    Cases 51/52 test disk columns with TTL, but not through a backup-restore
    cycle. The restore_meta step uses --no-restore-disk-objects, which could
    affect TTL tables with disk columns. This test verifies the full cycle.

    Precondition: tablespace ts_1 must be pre-created on the primary cluster.
    """

    @property
    def name(self):
        return "case_46g_disk_column_backup_restore"

    @property
    def description(self):
        return "Disk columns + TTL survive backup-restore cycle"

    def setup(self):
        # Stop replica to avoid conflicts from DROP+ndb_restore cycle
        replica_conn = pymysql.connect(
            host=self.config.host,
            port=self.config.replica_port,
            user=self.config.user,
            password=self.config.password
        )
        rcur = replica_conn.cursor()
        rcur.execute("STOP REPLICA")
        _kill_connections_to_db(self.config, self.config.replica_port, "ttl_backup")
        rcur.execute("DROP DATABASE IF EXISTS ttl_backup")
        rcur.close()
        replica_conn.close()

        _kill_connections_to_db(self.config, self.config.primary_port, "ttl_backup")
        conn = pymysql.connect(
            host=self.config.host,
            port=self.config.primary_port,
            user=self.config.user,
            password=self.config.password
        )
        cur = conn.cursor()
        cur.execute("DROP DATABASE IF EXISTS ttl_backup")
        cur.execute("CREATE DATABASE ttl_backup")
        cur.execute("CREATE TABLE ttl_backup.sz1 ("
                    "col_a INT, col_b TIMESTAMP, col_c INT, "
                    "col_d INT STORAGE DISK, "
                    "PRIMARY KEY(col_a)) "
                    "ENGINE=NDB, TABLESPACE ts_1, "
                    "COMMENT=\"NDB_TABLE=TTL=10@col_b\"")
        cur.close()
        conn.close()

    def teardown(self):
        try:
            _kill_connections_to_db(self.config, self.config.primary_port, "ttl_backup")
            conn = pymysql.connect(
                host=self.config.host,
                port=self.config.primary_port,
                user=self.config.user,
                password=self.config.password
            )
            cur = conn.cursor()
            cur.execute("DROP DATABASE IF EXISTS ttl_backup")
            cur.close()
            conn.close()
        except Exception:
            pass
        # Re-establish replication
        try:
            primary_conn = pymysql.connect(
                host=self.config.host,
                port=self.config.primary_port,
                user=self.config.user,
                password=self.config.password
            )
            pcur = primary_conn.cursor()
            pcur.execute("SHOW BINARY LOG STATUS")
            binlog_row = pcur.fetchone()
            log_file = binlog_row[0]
            log_pos = binlog_row[1]
            pcur.close()
            primary_conn.close()

            _kill_connections_to_db(self.config, self.config.replica_port, "ttl_backup")
            conn = pymysql.connect(
                host=self.config.host,
                port=self.config.replica_port,
                user=self.config.user,
                password=self.config.password
            )
            cur = conn.cursor()
            cur.execute("DROP DATABASE IF EXISTS ttl_backup")
            cur.execute("STOP REPLICA")
            cur.execute("RESET REPLICA ALL")
            cur.execute(
                "CHANGE REPLICATION SOURCE TO "
                f"SOURCE_HOST='{self.config.host}', "
                f"SOURCE_PORT={self.config.primary_port}, "
                f"SOURCE_USER='{self.config.repl_user}', "
                f"SOURCE_PASSWORD='{self.config.repl_password}', "
                f"SOURCE_LOG_FILE='{log_file}', "
                f"SOURCE_LOG_POS={log_pos}"
            )
            cur.execute("START REPLICA")
            cur.close()
            conn.close()
        except Exception:
            pass

    def run(self):
        # --- Phase 1: Insert rows with disk column data, wait for TTL, backup ---
        conn = pymysql.connect(
            host=self.config.host,
            port=self.config.primary_port,
            user=self.config.user,
            password=self.config.password,
            database="ttl_backup"
        )
        cur = conn.cursor()

        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz1 VALUES(1, SYSDATE(), 100, 10000)")
        cur.execute("INSERT INTO sz1 VALUES(2, SYSDATE(), 200, 20000)")
        cur.execute("INSERT INTO sz1 VALUES(3, DATE_ADD(SYSDATE(), INTERVAL 1 DAY), 300, 30000)")
        cur.execute("INSERT INTO sz1 VALUES(4, SYSDATE(), 400, 40000)")
        cur.execute("COMMIT")

        cur.execute("SELECT * FROM sz1")
        assert len(cur.fetchall()) == 4, "Expected 4 rows before TTL"

        # Wait for TTL expiry
        time.sleep(11)

        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 1, f"Expected 1 row after TTL, got {len(results)}"
        assert results[0][0] == 3, f"Expected col_a=3, got {results[0][0]}"
        assert results[0][3] == 30000, f"Expected col_d=30000, got {results[0][3]}"

        # Backup
        backup_id = _start_backup(self.config, self.config.primary_mgmd_port)
        cur.close()
        conn.close()

        # --- Phase 2: Drop, restore ---
        _kill_connections_to_db(self.config, self.config.primary_port, "ttl_backup")
        drop_conn = pymysql.connect(
            host=self.config.host,
            port=self.config.primary_port,
            user=self.config.user,
            password=self.config.password
        )
        drop_cur = drop_conn.cursor()
        drop_cur.execute("DROP DATABASE IF EXISTS ttl_backup")
        drop_cur.close()
        drop_conn.close()

        _restore_backup(
            self.config,
            self.config.primary_mgmd_port,
            backup_id=backup_id,
            data_dir_1=self.config.data_dir_p_1,
            data_dir_2=self.config.data_dir_p_2,
            include_databases="ttl_backup",
        )

        time.sleep(10)
        _wait_for_schema_sync(
            self.config, self.config.primary_port, "ttl_backup", "sz1"
        )

        # --- Phase 3: Verify restored data ---
        verify_conn = pymysql.connect(
            host=self.config.host,
            port=self.config.primary_port,
            user=self.config.user,
            password=self.config.password,
            database="ttl_backup"
        )
        cur = verify_conn.cursor()

        # Only the future row should be visible (TTL read filter)
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 1, (
            f"Restored disk+TTL table: expected 1 visible row, got {len(results)}"
        )
        assert results[0][0] == 3, f"Expected col_a=3, got {results[0][0]}"
        assert results[0][3] == 30000, (
            f"Disk column data lost: expected col_d=30000, got {results[0][3]}"
        )

        # Verify TTL is still active: insert a new row, wait, check it expires
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz1 VALUES(10, SYSDATE(), 1000, 100000)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz1")
        assert len(cur.fetchall()) == 2, "Expected 2 rows after new insert"

        time.sleep(11)

        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 1, (
            f"Expected 1 row after new row expires, got {len(results)}"
        )
        assert results[0][0] == 3, f"Expected col_a=3 to survive, got {results[0][0]}"

        # Verify expired rows are physically present with TTL=OFF
        cur.execute("ALTER TABLE sz1 COMMENT=\"NDB_TABLE=TTL=OFF\"")
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        # All physically-present rows: original 4 from backup + 1 new = 5
        assert len(results) == 5, (
            f"Expected 5 rows with TTL=OFF (all physical rows), got {len(results)}"
        )
        # Verify disk column values are intact for all rows
        col_d_values = sorted(r[3] for r in results)
        assert col_d_values == [10000, 20000, 30000, 40000, 100000], (
            f"Disk column values incorrect: {col_d_values}"
        )

        cur.close()
        verify_conn.close()


@cat_backup_restore.register
class TestCase46hMultiTableBackupRestore(TTLTestBase):
    """Case 46h: Multiple tables with different TTL configs through backup-restore.

    ndb_restore processes tables sequentially. This test verifies that each
    table retains its own TTL configuration independently after restore —
    different TTL durations don't get mixed up or lost between tables.
    """

    @property
    def name(self):
        return "case_46h_multi_table_backup_restore"

    @property
    def description(self):
        return "Multiple tables with different TTL configs survive backup-restore"

    def setup(self):
        # Stop replica to avoid conflicts from DROP+ndb_restore cycle
        replica_conn = pymysql.connect(
            host=self.config.host,
            port=self.config.replica_port,
            user=self.config.user,
            password=self.config.password
        )
        rcur = replica_conn.cursor()
        rcur.execute("STOP REPLICA")
        _kill_connections_to_db(self.config, self.config.replica_port, "ttl_backup")
        rcur.execute("DROP DATABASE IF EXISTS ttl_backup")
        rcur.close()
        replica_conn.close()

        _kill_connections_to_db(self.config, self.config.primary_port, "ttl_backup")
        conn = pymysql.connect(
            host=self.config.host,
            port=self.config.primary_port,
            user=self.config.user,
            password=self.config.password
        )
        cur = conn.cursor()
        cur.execute("DROP DATABASE IF EXISTS ttl_backup")
        cur.execute("CREATE DATABASE ttl_backup")
        # sz1: short TTL (10s), PK + UNIQUE KEY
        cur.execute("CREATE TABLE ttl_backup.sz1 ("
                    "col_a INT, col_b TIMESTAMP, col_c INT, "
                    "PRIMARY KEY(col_a), UNIQUE KEY(col_c)) "
                    "ENGINE=NDB, COMMENT=\"NDB_TABLE=TTL=10@col_b\"")
        # sz2: long TTL (86400s = 1 day), PK only
        cur.execute("CREATE TABLE ttl_backup.sz2 ("
                    "col_a INT, col_b TIMESTAMP, col_c INT, "
                    "PRIMARY KEY(col_a)) "
                    "ENGINE=NDB, COMMENT=\"NDB_TABLE=TTL=86400@col_b\"")
        # sz3: no TTL (control table)
        cur.execute("CREATE TABLE ttl_backup.sz3 ("
                    "col_a INT, col_b TIMESTAMP, col_c INT, "
                    "PRIMARY KEY(col_a)) "
                    "ENGINE=NDB")
        cur.close()
        conn.close()

    def teardown(self):
        try:
            _kill_connections_to_db(self.config, self.config.primary_port, "ttl_backup")
            conn = pymysql.connect(
                host=self.config.host,
                port=self.config.primary_port,
                user=self.config.user,
                password=self.config.password
            )
            cur = conn.cursor()
            cur.execute("DROP DATABASE IF EXISTS ttl_backup")
            cur.close()
            conn.close()
        except Exception:
            pass
        # Re-establish replication
        try:
            primary_conn = pymysql.connect(
                host=self.config.host,
                port=self.config.primary_port,
                user=self.config.user,
                password=self.config.password
            )
            pcur = primary_conn.cursor()
            pcur.execute("SHOW BINARY LOG STATUS")
            binlog_row = pcur.fetchone()
            log_file = binlog_row[0]
            log_pos = binlog_row[1]
            pcur.close()
            primary_conn.close()

            _kill_connections_to_db(self.config, self.config.replica_port, "ttl_backup")
            conn = pymysql.connect(
                host=self.config.host,
                port=self.config.replica_port,
                user=self.config.user,
                password=self.config.password
            )
            cur = conn.cursor()
            cur.execute("DROP DATABASE IF EXISTS ttl_backup")
            cur.execute("STOP REPLICA")
            cur.execute("RESET REPLICA ALL")
            cur.execute(
                "CHANGE REPLICATION SOURCE TO "
                f"SOURCE_HOST='{self.config.host}', "
                f"SOURCE_PORT={self.config.primary_port}, "
                f"SOURCE_USER='{self.config.repl_user}', "
                f"SOURCE_PASSWORD='{self.config.repl_password}', "
                f"SOURCE_LOG_FILE='{log_file}', "
                f"SOURCE_LOG_POS={log_pos}"
            )
            cur.execute("START REPLICA")
            cur.close()
            conn.close()
        except Exception:
            pass

    def run(self):
        # --- Phase 1: Insert seed rows, backup ---
        conn = pymysql.connect(
            host=self.config.host,
            port=self.config.primary_port,
            user=self.config.user,
            password=self.config.password,
            database="ttl_backup"
        )
        cur = conn.cursor()

        cur.execute("BEGIN")
        # sz1 (TTL=10): one future row that survives the whole test
        cur.execute("INSERT INTO sz1 VALUES(1, DATE_ADD(SYSDATE(), INTERVAL 1 DAY), 100)")
        # sz2 (TTL=86400): one future row
        cur.execute("INSERT INTO sz2 VALUES(1, DATE_ADD(SYSDATE(), INTERVAL 2 DAY), 100)")
        # sz3 (no TTL): one row
        cur.execute("INSERT INTO sz3 VALUES(1, SYSDATE(), 100)")
        cur.execute("COMMIT")

        for tbl in ("sz1", "sz2", "sz3"):
            cur.execute(f"SELECT * FROM {tbl}")
            assert len(cur.fetchall()) == 1, f"Expected 1 row in {tbl} before backup"

        backup_id = _start_backup(self.config, self.config.primary_mgmd_port)
        cur.close()
        conn.close()

        # --- Phase 2: Drop, restore ---
        _kill_connections_to_db(self.config, self.config.primary_port, "ttl_backup")
        drop_conn = pymysql.connect(
            host=self.config.host,
            port=self.config.primary_port,
            user=self.config.user,
            password=self.config.password
        )
        drop_cur = drop_conn.cursor()
        drop_cur.execute("DROP DATABASE IF EXISTS ttl_backup")
        drop_cur.close()
        drop_conn.close()

        _restore_backup(
            self.config,
            self.config.primary_mgmd_port,
            backup_id=backup_id,
            data_dir_1=self.config.data_dir_p_1,
            data_dir_2=self.config.data_dir_p_2,
            include_databases="ttl_backup",
        )

        time.sleep(10)
        _wait_for_schema_sync(
            self.config, self.config.primary_port, "ttl_backup", "sz1"
        )

        # --- Phase 3: Insert new rows after restore, verify per-table TTL ---
        verify_conn = pymysql.connect(
            host=self.config.host,
            port=self.config.primary_port,
            user=self.config.user,
            password=self.config.password,
            database="ttl_backup"
        )
        cur = verify_conn.cursor()

        # Verify seed rows survived restore
        for tbl in ("sz1", "sz2", "sz3"):
            cur.execute(f"SELECT * FROM {tbl}")
            assert len(cur.fetchall()) == 1, (
                f"Expected 1 seed row in {tbl} after restore"
            )

        # Insert a SYSDATE() row into each table
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz1 VALUES(10, SYSDATE(), 1000)")
        cur.execute("INSERT INTO sz2 VALUES(10, SYSDATE(), 1000)")
        cur.execute("INSERT INTO sz3 VALUES(10, SYSDATE(), 1000)")
        cur.execute("COMMIT")

        for tbl in ("sz1", "sz2", "sz3"):
            cur.execute(f"SELECT * FROM {tbl}")
            assert len(cur.fetchall()) == 2, (
                f"Expected 2 rows in {tbl} after new insert"
            )

        # Wait for TTL=10 expiry
        time.sleep(11)

        # sz1 (TTL=10): new row expired, only seed row remains
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 1, (
            f"sz1 (TTL=10): expected 1 row (new row expired), got {len(results)}"
        )
        assert results[0][0] == 1, f"sz1: expected seed row col_a=1, got {results[0][0]}"

        # sz2 (TTL=86400): new row still within TTL window, both rows visible
        cur.execute("SELECT * FROM sz2")
        results = cur.fetchall()
        assert len(results) == 2, (
            f"sz2 (TTL=86400): expected 2 rows (new row within window), got {len(results)}"
        )

        # sz3 (no TTL): both rows visible
        cur.execute("SELECT * FROM sz3")
        results = cur.fetchall()
        assert len(results) == 2, (
            f"sz3 (no TTL): expected 2 rows, got {len(results)}"
        )

        cur.close()
        verify_conn.close()


###############################################################################
# Test Runner
###############################################################################

class TTLTestRunner:
    """Main test runner"""

    def __init__(self, config):
        self.config = config

    def run_category(self, category_name):
        """Run a specific category"""
        if category_name not in CATEGORIES:
            raise ValueError(f"Unknown category: {category_name}")

        cat = CATEGORIES[category_name]
        print(f"\n{'='*60}")
        print(f"Category: {cat.name} - {cat.description}")
        print(f"{'='*60}")

        result = cat.run(self.config)

        print(f"\nResults: {result.passed}/{result.total} passed", end="")
        if result.failed > 0:
            print(f", {result.failed} failed", end="")
        if result.skipped > 0:
            print(f", {result.skipped} skipped", end="")
        print()

        return result

    def run_all(self, exclude_purge=True):
        """Run all categories"""
        results = {}
        for name, cat in CATEGORIES.items():
            if exclude_purge and name == "purge" and not self.config.purge_enabled:
                print(f"\n[SKIPPED] Category: {name} (use --enable-purge)")
                continue
            results[name] = self.run_category(name)
        return results

    def run_test(self, test_name):
        """Run a specific test by name"""
        for cat in CATEGORIES.values():
            for test_class in cat.tests:
                test = test_class(self.config)
                if test.name == test_name:
                    print(f"\nRunning test: {test_name}")
                    result = test.execute()
                    print(f"[{result.status}] {result.name}", end="")
                    if result.message:
                        print(f" - {result.message}", end="")
                    print(f" ({result.duration:.2f}s)")
                    return result
        raise ValueError(f"Unknown test: {test_name}")

    def list_tests(self):
        """List all available tests"""
        print("\nAvailable Tests:")
        print("="*60)
        for name, cat in CATEGORIES.items():
            print(f"\n[{name}] {cat.description}")
            for test_class in cat.tests:
                test = test_class(self.config)
                flags = []
                if test.requires_replica:
                    flags.append("replica")
                if test.requires_debug_sync:
                    flags.append("debug_sync")
                if test.requires_purge:
                    flags.append("purge")
                flag_str = f" ({', '.join(flags)})" if flags else ""
                print(f"  - {test.name}{flag_str}")
                print(f"      {test.description}")


###############################################################################
# Main Entry Point
###############################################################################

def setup_global_tables(config):
    """Create the global test.sz table used by most tests"""
    try:
        conn = pymysql.connect(
            host=config.host,
            port=config.primary_port,
            user=config.user,
            password=config.password
        )
        cur = conn.cursor()
        cur.execute("DROP DATABASE IF EXISTS test")
        cur.execute("CREATE DATABASE IF NOT EXISTS test")
        cur.execute("CREATE TABLE test.sz ("
                    "col_a INT, "
                    "col_b TIMESTAMP, "
                    "col_c INT, "
                    "PRIMARY KEY(col_a), "
                    "UNIQUE KEY(col_c))"
                    "ENGINE = NDB, "
                    "COMMENT=\"NDB_TABLE=TTL=66@col_b\"")
        time.sleep(5)
        cur.execute("ALTER TABLE test.sz "
                    "COMMENT=\"NDB_TABLE=TTL=10@col_b\"")
        cur.close()
        conn.close()
        time.sleep(1)
        print("Global test.sz table created successfully")
        return True
    except Exception as e:
        print(f"Failed to create global test tables: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="TTL Test Suite for RonDB")
    parser.add_argument("--category", "-c", help="Run specific category")
    parser.add_argument("--test", "-t", help="Run specific test")
    parser.add_argument("--list", "-l", action="store_true", help="List all tests")
    parser.add_argument("--enable-purge", action="store_true", help="Enable purge tests")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument("--stop-on-failure", action="store_true", help="Stop on first failure")
    parser.add_argument("--config", default="/tmp/ttl_config.py", help="Config file path")
    parser.add_argument("--skip-setup", action="store_true", help="Skip global table setup")

    args = parser.parse_args()

    config = TestConfig.load(args.config)
    config.purge_enabled = args.enable_purge
    config.verbose = args.verbose
    config.stop_on_failure = args.stop_on_failure

    # Setup global tables unless skipped or just listing
    if not args.list and not args.skip_setup:
        if not setup_global_tables(config):
            return 1

    runner = TTLTestRunner(config)

    if args.list:
        runner.list_tests()
        return 0

    # Disable RDRS TTL purging for the duration of the test suite.
    # Tests in this suite rely on the NDB read-side TTL filter to hide
    # expired rows, and do not expect the purger to physically delete them.
    _set_purge_enabled(config, False)

    try:
        if args.test:
            result = runner.run_test(args.test)
            return 0 if result.status == TestStatus.PASSED else 1

        if args.category:
            result = runner.run_category(args.category)
            return 0 if result.failed == 0 else 1

        # Run all categories
        results = runner.run_all()

        # Print summary
        print("\n" + "="*60)
        print("SUMMARY")
        print("="*60)
        total_passed = sum(r.passed for r in results.values())
        total_failed = sum(r.failed for r in results.values())
        total_skipped = sum(r.skipped for r in results.values())
        total = sum(r.total for r in results.values())

        for name, result in results.items():
            status = "PASS" if result.failed == 0 else "FAIL"
            print(f"  [{status}] {name}: {result.passed}/{result.total}")

        print(f"\nTotal: {total_passed}/{total} passed", end="")
        if total_failed > 0:
            print(f", {total_failed} failed", end="")
        if total_skipped > 0:
            print(f", {total_skipped} skipped", end="")
        print()

        return 0 if total_failed == 0 else 1
    finally:
        # Re-enable RDRS TTL purging
        _set_purge_enabled(config, True)


if __name__ == "__main__":
    sys.exit(main())
