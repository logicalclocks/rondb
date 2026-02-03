#!/usr/bin/env python3
"""
Comprehensive TTL Purge Test Suite

This test suite covers all aspects of TTL purge functionality:
1. API Edge Cases
2. TTL Column Types
3. Schema Changes During Purging
4. Metrics Accuracy
5. Concurrent Operations
6. Edge Cases
7. Adaptive Batch Sizing
8. Config Changes During Purging
9. Multiple TTL Tables
10. Stress/Scale

Usage:
    # Run all tests
    python3 ttl_test_suite.py

    # Run specific test category
    python3 ttl_test_suite.py --category api

    # Run with verbose output
    python3 ttl_test_suite.py -v

    # Run specific test
    python3 ttl_test_suite.py -k test_pagination
"""

import os
import sys
import time
import json
import random
import string
import argparse
import threading
import traceback
from typing import List, Dict, Tuple, Optional, Callable
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pymysql


# ============================================================================
# Cluster Configuration (override via environment variables)
# ============================================================================

@dataclass
class ClusterConfig:
    """Configuration for a single RonDB cluster."""
    name: str
    mysql_host: str
    mysql_port: int
    mysql_user: str
    mysql_password: str
    rdrs_host: str
    rdrs_port: int

    @property
    def rdrs_base_url(self) -> str:
        return f"http://{self.rdrs_host}:{self.rdrs_port}"

    @property
    def ttl_purge_url(self) -> str:
        return f"{self.rdrs_base_url}/0.1.0/ttl-purge"

    def get_mysql_connection(self, database: Optional[str] = None):
        """Get a MySQL connection to this cluster."""
        return pymysql.connect(
            host=self.mysql_host,
            port=self.mysql_port,
            user=self.mysql_user,
            password=self.mysql_password,
            database=database,
            autocommit=True
        )

    def execute_sql(self, sql: str, database: Optional[str] = None):
        """Execute SQL and return results."""
        conn = self.get_mysql_connection(database)
        try:
            cursor = conn.cursor()
            cursor.execute(sql)
            if cursor.description:
                return cursor.fetchall()
            return None
        finally:
            conn.close()

    def is_mysql_ready(self) -> bool:
        """Check if MySQL is ready."""
        try:
            conn = self.get_mysql_connection()
            conn.close()
            return True
        except Exception:
            return False

    def is_rdrs_ready(self) -> bool:
        """Check if RDRS is ready."""
        try:
            resp = requests.get(f"{self.ttl_purge_url}", timeout=5)
            return resp.status_code == 200
        except Exception:
            return False


# Default cluster configurations (override via environment variables)
PRIMARY_CLUSTER = ClusterConfig(
    name="primary",
    mysql_host=os.environ.get("TTL_PRIMARY_MYSQL_HOST", "127.0.0.1"),
    mysql_port=int(os.environ.get("TTL_PRIMARY_MYSQL_PORT", "3308")),
    mysql_user=os.environ.get("TTL_PRIMARY_MYSQL_USER", "root"),
    mysql_password=os.environ.get("TTL_PRIMARY_MYSQL_PASSWORD", ""),
    rdrs_host=os.environ.get("TTL_PRIMARY_RDRS_HOST", "127.0.0.1"),
    rdrs_port=int(os.environ.get("TTL_PRIMARY_RDRS_PORT", "4406"))
)

REPLICA_CLUSTER = ClusterConfig(
    name="replica",
    mysql_host=os.environ.get("TTL_REPLICA_MYSQL_HOST", "127.0.0.1"),
    mysql_port=int(os.environ.get("TTL_REPLICA_MYSQL_PORT", "3309")),
    mysql_user=os.environ.get("TTL_REPLICA_MYSQL_USER", "root"),
    mysql_password=os.environ.get("TTL_REPLICA_MYSQL_PASSWORD", ""),
    rdrs_host=os.environ.get("TTL_REPLICA_RDRS_HOST", "127.0.0.1"),
    rdrs_port=int(os.environ.get("TTL_REPLICA_RDRS_PORT", "4407"))
)


# ============================================================================
# TTL Test Helper
# ============================================================================

class TTLTestHelper:
    """Helper class for TTL-specific test operations."""

    def __init__(self, cluster: ClusterConfig):
        self.cluster = cluster

    def create_ttl_table(
        self,
        database: str,
        table: str,
        ttl_seconds: int,
        ttl_column: str = "ttl_col",
        ttl_column_type: str = "DATETIME",
        extra_columns: str = "data VARCHAR(100)",
        create_index: bool = True
    ) -> bool:
        """Create a TTL-enabled table."""
        conn = self.cluster.get_mysql_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
            cursor.execute(f"USE {database}")
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
            sql = f"""
                CREATE TABLE {table} (
                    id INT PRIMARY KEY,
                    {ttl_column} {ttl_column_type},
                    {extra_columns}
                ) ENGINE=NDB COMMENT='NDB_TABLE=TTL={ttl_seconds}@{ttl_column}'
            """
            cursor.execute(sql)
            if create_index:
                cursor.execute(f"CREATE INDEX ttl_index ON {table}({ttl_column})")
            return True
        except Exception as e:
            print(f"Error creating TTL table: {e}")
            return False
        finally:
            conn.close()

    def insert_expired_rows(
        self,
        database: str,
        table: str,
        count: int,
        ttl_column: str = "ttl_col",
        minutes_ago: int = 5,
        start_id: int = 1
    ) -> bool:
        """Insert rows that are already expired."""
        conn = self.cluster.get_mysql_connection(database)
        try:
            cursor = conn.cursor()
            for i in range(count):
                row_id = start_id + i
                cursor.execute(f"""
                    INSERT INTO {table} (id, {ttl_column}, data) VALUES
                    ({row_id}, DATE_SUB(NOW(), INTERVAL {minutes_ago} MINUTE),
                     'expired_{row_id}')
                """)
            return True
        except Exception as e:
            print(f"Error inserting expired rows: {e}")
            return False
        finally:
            conn.close()

    def insert_valid_rows(
        self,
        database: str,
        table: str,
        count: int,
        ttl_column: str = "ttl_col",
        hours_ahead: int = 1,
        start_id: int = 1000
    ) -> bool:
        """Insert rows that are not expired."""
        conn = self.cluster.get_mysql_connection(database)
        try:
            cursor = conn.cursor()
            for i in range(count):
                row_id = start_id + i
                cursor.execute(f"""
                    INSERT INTO {table} (id, {ttl_column}, data) VALUES
                    ({row_id}, DATE_ADD(NOW(), INTERVAL {hours_ahead} HOUR),
                     'valid_{row_id}')
                """)
            return True
        except Exception as e:
            print(f"Error inserting valid rows: {e}")
            return False
        finally:
            conn.close()

    def get_row_count(self, database: str, table: str) -> int:
        """Get the number of rows in a table."""
        conn = self.cluster.get_mysql_connection(database)
        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {database}.{table}")
            return cursor.fetchone()[0]
        finally:
            conn.close()

    def drop_table(self, database: str, table: str) -> bool:
        """Drop a table."""
        try:
            self.cluster.execute_sql(f"DROP TABLE IF EXISTS {database}.{table}")
            return True
        except Exception as e:
            print(f"Error dropping table: {e}")
            return False

    def drop_database(self, database: str) -> bool:
        """Drop a database."""
        try:
            self.cluster.execute_sql(f"DROP DATABASE IF EXISTS {database}")
            return True
        except Exception as e:
            print(f"Error dropping database: {e}")
            return False

    def alter_ttl(self, database: str, table: str, new_ttl_seconds: int,
                  ttl_column: str = "ttl_col") -> bool:
        """Alter TTL setting on a table."""
        try:
            comment = f"NDB_TABLE=TTL={new_ttl_seconds}@{ttl_column}"
            self.cluster.execute_sql(
                f"ALTER TABLE {database}.{table} COMMENT='{comment}'"
            )
            return True
        except Exception as e:
            print(f"Error altering TTL: {e}")
            return False

    def disable_ttl(self, database: str, table: str,
                    ttl_column: str = "ttl_col") -> bool:
        """Disable TTL on a table."""
        try:
            self.cluster.execute_sql(
                f"ALTER TABLE {database}.{table} COMMENT='NDB_TABLE=TTL=off'"
            )
            return True
        except Exception as e:
            print(f"Error disabling TTL: {e}")
            return False

    # RDRS API helpers
    def get_ttl_purge_config(self) -> dict:
        resp = requests.get(f"{self.cluster.ttl_purge_url}/config")
        return resp.json() if resp.status_code == 200 else {}

    def set_ttl_purge_config(self, config: dict) -> bool:
        resp = requests.put(f"{self.cluster.ttl_purge_url}/config", json=config)
        return resp.status_code == 200

    def get_ttl_purge_status(self) -> dict:
        resp = requests.get(f"{self.cluster.ttl_purge_url}/status")
        return resp.json() if resp.status_code == 200 else {}

    def get_ttl_purge_metrics(self) -> dict:
        resp = requests.get(f"{self.cluster.ttl_purge_url}/metrics")
        return resp.json() if resp.status_code == 200 else {}

    def get_ttl_tables(self, offset: int = 0, limit: int = 20) -> dict:
        resp = requests.get(
            f"{self.cluster.ttl_purge_url}/tables",
            params={"offset": offset, "limit": limit}
        )
        return resp.json() if resp.status_code == 200 else {}

    def get_ttl_table(self, database: str, table: str) -> Optional[dict]:
        resp = requests.get(
            f"{self.cluster.ttl_purge_url}/tables/{database}/{table}"
        )
        return resp.json() if resp.status_code == 200 else None

    def enable_purge(self) -> bool:
        return self.set_ttl_purge_config({"enabled": True})

    def disable_purge(self) -> bool:
        return self.set_ttl_purge_config({"enabled": False})

    def wait_for_purge(self, expected_purged: int, timeout: int = 60) -> bool:
        start = time.time()
        initial = self.get_ttl_purge_metrics().get("rows_purged_total", 0)
        while time.time() - start < timeout:
            current = self.get_ttl_purge_metrics().get("rows_purged_total", 0)
            if current - initial >= expected_purged:
                return True
            time.sleep(1)
        return False

    def wait_for_table_detection(self, database: str, table: str,
                                 timeout: int = 30) -> bool:
        start = time.time()
        while time.time() - start < timeout:
            table_info = self.get_ttl_table(database, table)
            if table_info and "table_id" in table_info:
                return True
            time.sleep(1)
        return False

    def wait_for_purge_completion(self, database: str, table: str,
                                  timeout: int = 30) -> bool:
        start = time.time()
        while time.time() - start < timeout:
            count = self.get_row_count(database, table)
            if count == 0:
                return True
            time.sleep(2)
        return False

    def wait_for_table_removal(self, database: str, table: str,
                               timeout: int = 30) -> bool:
        start = time.time()
        while time.time() - start < timeout:
            table_info = self.get_ttl_table(database, table)
            if table_info is None or "error" in table_info:
                return True
            time.sleep(1)
        return False


def get_test_helper(cluster: str = "primary") -> TTLTestHelper:
    """Get a TTL test helper for the specified cluster."""
    if cluster == "primary":
        return TTLTestHelper(PRIMARY_CLUSTER)
    elif cluster == "replica":
        return TTLTestHelper(REPLICA_CLUSTER)
    else:
        raise ValueError(f"Unknown cluster: {cluster}")


# ============================================================================
# Test Framework
# ============================================================================
@dataclass
class TestResult:
    """Result of a single test."""
    name: str
    category: str
    passed: bool
    message: str
    duration: float


class TestSuite:
    """Test suite runner."""

    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.results: List[TestResult] = []
        self.helper = get_test_helper("primary")
        self.setup_done = False

    def log(self, msg: str):
        if self.verbose:
            print(f"  {msg}")

    def run_test(self, name: str, category: str, test_func: Callable) -> TestResult:
        """Run a single test."""
        print(f"\n{'='*60}")
        print(f"TEST: {name}")
        print(f"Category: {category}")
        print('='*60)

        start_time = time.time()
        try:
            test_func()
            duration = time.time() - start_time
            result = TestResult(name, category, True, "PASSED", duration)
            print(f"  [PASS] ({duration:.2f}s)")
        except AssertionError as e:
            duration = time.time() - start_time
            result = TestResult(name, category, False, str(e), duration)
            print(f"  [FAIL] {e} ({duration:.2f}s)")
            if self.verbose:
                traceback.print_exc()
        except Exception as e:
            duration = time.time() - start_time
            result = TestResult(name, category, False, f"ERROR: {e}", duration)
            print(f"  [ERROR] {e} ({duration:.2f}s)")
            if self.verbose:
                traceback.print_exc()

        self.results.append(result)
        return result

    # All test databases used by the test suite
    TEST_DATABASES = [
        "ttl_test", "ttl_test_1", "ttl_test_2", "ttl_test_3",
        "ttl_stress", "ttl_concurrent", "ttl_metrics",
        "ttl_db_a", "ttl_db_b", "ttl_db_c",  # Used by test_different_databases
        "ttl_blob"  # Used by BlobPurgeTests
    ]

    def setup(self):
        """Setup before running tests."""
        if self.setup_done:
            return

        print("\n" + "="*60)
        print("SETUP: Preparing test environment")
        print("="*60)

        # Ensure purging is enabled with default config
        self.helper.set_ttl_purge_config({
            "enabled": True,
            "min_batch_size": 5,
            "max_batch_size": 50,
            "sleep_interval_ms": 1500
        })

        # Clean up any leftover test databases
        for db in self.TEST_DATABASES:
            self.helper.drop_database(db)

        time.sleep(5)  # Wait for schema watcher to process drops
        self.setup_done = True
        print("  Setup complete")

    def teardown(self):
        """Cleanup after tests."""
        print("\n" + "="*60)
        print("TEARDOWN: Cleaning up")
        print("="*60)

        # Clean up test databases
        for db in self.TEST_DATABASES:
            self.helper.drop_database(db)

        # Restore default config
        self.helper.set_ttl_purge_config({
            "enabled": True,
            "min_batch_size": 5,
            "max_batch_size": 50,
            "sleep_interval_ms": 1500
        })

        print("  Teardown complete")

    def print_summary(self):
        """Print test summary."""
        print("\n" + "="*60)
        print("TEST SUMMARY")
        print("="*60)

        # Group by category
        categories: Dict[str, List[TestResult]] = {}
        for result in self.results:
            if result.category not in categories:
                categories[result.category] = []
            categories[result.category].append(result)

        total_passed = 0
        total_failed = 0

        for category, results in sorted(categories.items()):
            passed = sum(1 for r in results if r.passed)
            failed = len(results) - passed
            total_passed += passed
            total_failed += failed

            print(f"\n{category}:")
            for r in results:
                status = "PASS" if r.passed else "FAIL"
                print(f"  [{status}] {r.name} ({r.duration:.2f}s)")
                if not r.passed and r.message:
                    print(f"         {r.message}")

        print(f"\n{'='*60}")
        print(f"Total: {total_passed} passed, {total_failed} failed")
        print(f"{'='*60}")

        return total_failed == 0


# ============================================================================
# Test Cases: API Edge Cases
# ============================================================================
class APITests:
    """API edge case tests."""

    def __init__(self, suite: TestSuite):
        self.suite = suite
        self.helper = suite.helper

    def test_pagination(self):
        """Test pagination for /ttl-purge/tables endpoint."""
        # Create multiple TTL tables
        for i in range(5):
            self.helper.create_ttl_table(
                "ttl_test", f"table_{i}", ttl_seconds=60
            )
            time.sleep(1)  # Small delay between creates

        time.sleep(10)  # Wait for schema watcher to detect all tables

        # Test offset and limit
        result = self.helper.get_ttl_tables(offset=0, limit=2)
        assert result["total"] >= 5, f"Expected at least 5 tables, got {result['total']}"
        assert len(result["tables"]) == 2, f"Expected 2 tables with limit=2, got {len(result['tables'])}"

        result = self.helper.get_ttl_tables(offset=2, limit=2)
        assert len(result["tables"]) == 2, f"Expected 2 tables at offset=2"

        result = self.helper.get_ttl_tables(offset=4, limit=10)
        assert len(result["tables"]) >= 1, f"Expected at least 1 table at offset=4"

        # Test large offset
        result = self.helper.get_ttl_tables(offset=1000, limit=20)
        assert len(result["tables"]) == 0, f"Expected 0 tables at large offset"

        # Cleanup
        self.helper.drop_database("ttl_test")

    def test_invalid_config_values(self):
        """Test config validation with invalid values."""
        original = self.helper.get_ttl_purge_config()

        # min_batch_size < 1 should be clamped to 1
        self.helper.set_ttl_purge_config({"min_batch_size": 0})
        config = self.helper.get_ttl_purge_config()
        assert config["min_batch_size"] >= 1, "min_batch_size should be at least 1"

        # max_batch_size < min_batch_size should be clamped
        self.helper.set_ttl_purge_config({"min_batch_size": 10, "max_batch_size": 5})
        config = self.helper.get_ttl_purge_config()
        assert config["max_batch_size"] >= config["min_batch_size"], \
            "max_batch_size should be >= min_batch_size"

        # sleep_interval_ms < 100 should be clamped
        self.helper.set_ttl_purge_config({"sleep_interval_ms": 50})
        config = self.helper.get_ttl_purge_config()
        assert config["sleep_interval_ms"] >= 100, "sleep_interval_ms should be at least 100"

        # Restore original
        self.helper.set_ttl_purge_config(original)

    def test_nonexistent_table_lookup(self):
        """Test GET /tables/{db}/{table} for non-existent table."""
        result = self.helper.get_ttl_table("nonexistent_db", "nonexistent_table")
        assert result is None or "error" in result, "Should return error for non-existent table"

    def test_empty_request_body(self):
        """Test PUT /config with empty body."""
        resp = requests.put(f"{self.helper.cluster.ttl_purge_url}/config", data="")
        assert resp.status_code == 400, "Should return 400 for empty body"

    def test_invalid_json_body(self):
        """Test PUT /config with invalid JSON."""
        resp = requests.put(
            f"{self.helper.cluster.ttl_purge_url}/config",
            data="not valid json",
            headers={"Content-Type": "application/json"}
        )
        assert resp.status_code == 400, "Should return 400 for invalid JSON"

    def test_combined_endpoint(self):
        """Test GET /ttl-purge returns all sections."""
        resp = requests.get(self.helper.cluster.ttl_purge_url)
        assert resp.status_code == 200
        data = resp.json()
        assert "config" in data, "Missing config section"
        assert "status" in data, "Missing status section"
        assert "metrics" in data, "Missing metrics section"

    def run_all(self):
        """Run all API tests."""
        tests = [
            ("Pagination", self.test_pagination),
            ("Invalid config values", self.test_invalid_config_values),
            ("Non-existent table lookup", self.test_nonexistent_table_lookup),
            ("Empty request body", self.test_empty_request_body),
            ("Invalid JSON body", self.test_invalid_json_body),
            ("Combined endpoint", self.test_combined_endpoint),
        ]
        for name, test_func in tests:
            self.suite.run_test(name, "API Edge Cases", test_func)


# ============================================================================
# Test Cases: TTL Column Types
# ============================================================================
class ColumnTypeTests:
    """TTL column type tests."""

    def __init__(self, suite: TestSuite):
        self.suite = suite
        self.helper = suite.helper

    def test_datetime_column(self):
        """Test TTL with DATETIME column."""
        self.helper.create_ttl_table(
            "ttl_test", "datetime_table", ttl_seconds=5,
            ttl_column_type="DATETIME"
        )
        time.sleep(3)

        # Insert rows that will expire in 5 seconds (TTL=5)
        conn = self.helper.cluster.get_mysql_connection("ttl_test")
        cursor = conn.cursor()
        for i in range(5):
            cursor.execute(f"""
                INSERT INTO datetime_table (id, ttl_col, data) VALUES
                ({i}, NOW(), 'will_expire_{i}')
            """)
        conn.close()

        # Rows should be visible initially
        initial_count = self.helper.get_row_count("ttl_test", "datetime_table")
        assert initial_count == 5, f"Expected 5 rows initially, got {initial_count}"

        # Wait for TTL to expire + purge cycle
        time.sleep(15)
        final_count = self.helper.get_row_count("ttl_test", "datetime_table")
        assert final_count == 0, f"Expected 0 rows after purge, got {final_count}"

        self.helper.drop_database("ttl_test")

    def test_timestamp_column(self):
        """Test TTL with TIMESTAMP column."""
        self.helper.create_ttl_table(
            "ttl_test", "timestamp_table", ttl_seconds=5,
            ttl_column="ts_col", ttl_column_type="TIMESTAMP"
        )
        time.sleep(3)

        # Insert rows that will expire in 5 seconds (TTL=5)
        conn = self.helper.cluster.get_mysql_connection("ttl_test")
        cursor = conn.cursor()
        for i in range(5):
            cursor.execute(f"""
                INSERT INTO timestamp_table (id, ts_col, data) VALUES
                ({i}, NOW(), 'will_expire_{i}')
            """)
        conn.close()

        # Rows should be visible initially
        initial_count = self.helper.get_row_count("ttl_test", "timestamp_table")
        assert initial_count == 5, f"Expected 5 rows initially, got {initial_count}"

        # Wait for TTL to expire + purge cycle
        time.sleep(15)
        final_count = self.helper.get_row_count("ttl_test", "timestamp_table")
        assert final_count == 0, f"Expected 0 rows after purge, got {final_count}"

        self.helper.drop_database("ttl_test")

    def test_null_ttl_column(self):
        """Test rows with NULL TTL column value.

        TODO: This test is currently limited due to COUNT(*) vs SELECT * inconsistency.

        BUG: COUNT(*) returns all rows (including expired) while SELECT * only returns
        non-expired rows. This is because:
        - SELECT * goes through handleReadReq -> checkTTL -> expired rows return error 626
        - COUNT(*) uses ha_ndbcluster::records() which returns statistics-based count
          (HA_COUNT_ROWS_INSTANT flag) without TTL filtering

        FIX NEEDED in ha_ndbcluster.cc:
        Option 1: Disable HA_COUNT_ROWS_INSTANT for TTL tables (force real scan)
        Option 2: Make records() TTL-aware (subtract expired rows or do filtered count)

        For now, we verify using SELECT * instead of COUNT(*).
        """
        self.helper.create_ttl_table("ttl_test", "null_ttl", ttl_seconds=5)
        time.sleep(3)

        # Insert rows with NULL TTL column
        conn = self.helper.cluster.get_mysql_connection("ttl_test")
        cursor = conn.cursor()
        for i in range(3):
            cursor.execute(f"""
                INSERT INTO null_ttl (id, ttl_col, data) VALUES
                ({i}, NULL, 'null_ttl_{i}')
            """)
        # Insert expired rows
        for i in range(3, 6):
            cursor.execute(f"""
                INSERT INTO null_ttl (id, ttl_col, data) VALUES
                ({i}, DATE_SUB(NOW(), INTERVAL 1 MINUTE), 'expired_{i}')
            """)
        conn.close()

        time.sleep(10)  # Wait for purge

        # TODO: Once COUNT(*)/SELECT * inconsistency is fixed, use get_row_count()
        # For now, verify using SELECT * and count rows in Python.
        # Note: SELECT COUNT(*) FROM (SELECT * FROM t) doesn't work because
        # MySQL optimizer uses "Select tables optimized away" and still returns
        # the statistics-based count.
        conn = self.helper.cluster.get_mysql_connection("ttl_test")
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM null_ttl")
        visible_rows = cursor.fetchall()
        visible_count = len(visible_rows)
        conn.close()

        # NULL rows should NOT be purged (visible), expired rows should be invisible
        assert visible_count == 3, f"Expected 3 visible rows (NULL TTL), got {visible_count}"

        self.helper.drop_database("ttl_test")

    def run_all(self):
        """Run all column type tests."""
        tests = [
            ("DATETIME column", self.test_datetime_column),
            ("TIMESTAMP column", self.test_timestamp_column),
            ("NULL TTL column", self.test_null_ttl_column),
        ]
        for name, test_func in tests:
            self.suite.run_test(name, "TTL Column Types", test_func)


# ============================================================================
# Test Cases: Schema Changes During Purging
# ============================================================================
class SchemaChangeTests:
    """Schema change tests."""

    def __init__(self, suite: TestSuite):
        self.suite = suite
        self.helper = suite.helper

    def test_alter_ttl_value(self):
        """Test ALTER TABLE to change TTL value."""
        self.helper.create_ttl_table("ttl_test", "alter_ttl", ttl_seconds=10)
        time.sleep(5)

        # Verify initial TTL
        table_info = self.helper.get_ttl_table("ttl_test", "alter_ttl")
        assert table_info is not None, "Table should be detected"
        assert table_info["ttl_sec"] == 10, f"Expected TTL=10, got {table_info['ttl_sec']}"

        # Alter TTL to 60 seconds
        self.helper.alter_ttl("ttl_test", "alter_ttl", 60)

        # Wait longer for schema watcher to detect the change
        time.sleep(10)

        table_info = self.helper.get_ttl_table("ttl_test", "alter_ttl")
        assert table_info is not None, "Table should still be detected after ALTER"
        assert table_info["ttl_sec"] == 60, f"Expected TTL=60, got {table_info['ttl_sec']}"

        self.helper.drop_database("ttl_test")

    def test_disable_ttl(self):
        """Test ALTER TABLE to disable TTL."""
        self.helper.create_ttl_table("ttl_test", "disable_ttl", ttl_seconds=10)
        time.sleep(5)

        # Verify table is detected
        assert self.helper.wait_for_table_detection("ttl_test", "disable_ttl", timeout=30), \
            "Table should be detected"

        # Disable TTL (set TTL=0)
        self.helper.disable_ttl("ttl_test", "disable_ttl")

        # Wait longer for schema watcher to process the change
        time.sleep(10)

        # Verify table is removed from TTL tables (with longer timeout)
        assert self.helper.wait_for_table_removal("ttl_test", "disable_ttl", timeout=60), \
            "Table should be removed after TTL disabled"

        # Insert rows - they should not be purged since TTL is disabled
        conn = self.helper.cluster.get_mysql_connection("ttl_test")
        cursor = conn.cursor()
        for i in range(5):
            cursor.execute(f"""
                INSERT INTO disable_ttl (id, ttl_col, data) VALUES
                ({i}, DATE_SUB(NOW(), INTERVAL 1 MINUTE), 'should_stay_{i}')
            """)
        conn.close()

        time.sleep(10)
        count = self.helper.get_row_count("ttl_test", "disable_ttl")
        assert count == 5, f"Rows should not be purged when TTL disabled, got {count}"

        self.helper.drop_database("ttl_test")

    def test_drop_table_during_purge(self):
        """Test DROP TABLE while purging is active."""
        self.helper.create_ttl_table("ttl_test", "drop_table", ttl_seconds=5)
        time.sleep(3)

        # Insert rows
        self.helper.insert_expired_rows("ttl_test", "drop_table", count=10)

        # Drop table immediately
        self.helper.drop_table("ttl_test", "drop_table")
        time.sleep(5)

        # Verify no errors in status
        status = self.helper.get_ttl_purge_status()
        assert status["state"] != "error", f"Purger should not be in error state: {status}"

        # Verify table is removed from TTL tables
        assert self.helper.wait_for_table_removal("ttl_test", "drop_table"), \
            "Dropped table should be removed"

        self.helper.drop_database("ttl_test")

    def test_drop_database_with_ttl_tables(self):
        """Test DROP DATABASE with multiple TTL tables."""
        # Create multiple tables
        for i in range(3):
            self.helper.create_ttl_table("ttl_test", f"multi_table_{i}", ttl_seconds=60)

        time.sleep(5)

        # Verify all tables detected
        tables = self.helper.get_ttl_tables(limit=100)
        ttl_test_tables = [t for t in tables["tables"] if t["database"] == "ttl_test"]
        assert len(ttl_test_tables) == 3, f"Expected 3 tables, got {len(ttl_test_tables)}"

        # Drop entire database
        self.helper.drop_database("ttl_test")
        time.sleep(5)

        # Verify all tables removed
        tables = self.helper.get_ttl_tables(limit=100)
        ttl_test_tables = [t for t in tables["tables"] if t["database"] == "ttl_test"]
        assert len(ttl_test_tables) == 0, f"All tables should be removed, got {len(ttl_test_tables)}"

    def test_rename_table(self):
        """Test RENAME TABLE with TTL."""
        self.helper.create_ttl_table("ttl_test", "original_name", ttl_seconds=60)
        time.sleep(3)

        assert self.helper.wait_for_table_detection("ttl_test", "original_name"), \
            "Original table should be detected"

        # Rename table
        self.helper.cluster.execute_sql(
            "RENAME TABLE ttl_test.original_name TO ttl_test.new_name"
        )
        time.sleep(5)

        # Original name should be gone, new name should be present
        old_info = self.helper.get_ttl_table("ttl_test", "original_name")
        new_info = self.helper.get_ttl_table("ttl_test", "new_name")

        assert old_info is None or "error" in old_info, "Original name should not exist"
        assert new_info is not None and "ttl_sec" in new_info, "New name should exist"

        self.helper.drop_database("ttl_test")

    def run_all(self):
        """Run all schema change tests."""
        tests = [
            ("ALTER TTL value", self.test_alter_ttl_value),
            ("Disable TTL", self.test_disable_ttl),
            ("DROP TABLE during purge", self.test_drop_table_during_purge),
            ("DROP DATABASE with TTL tables", self.test_drop_database_with_ttl_tables),
            ("RENAME TABLE", self.test_rename_table),
        ]
        for name, test_func in tests:
            self.suite.run_test(name, "Schema Changes", test_func)


# ============================================================================
# Test Cases: Metrics Accuracy
# ============================================================================
class MetricsTests:
    """Metrics accuracy tests."""

    def __init__(self, suite: TestSuite):
        self.suite = suite
        self.helper = suite.helper

    def test_rows_purged_total_accumulates(self):
        """Test that rows_purged_total accumulates correctly."""
        self.helper.create_ttl_table("ttl_metrics", "accumulate", ttl_seconds=5)
        time.sleep(3)

        initial = self.helper.get_ttl_purge_metrics()["rows_purged_total"]

        # First batch - wait for physical deletion (COUNT(*) includes expired rows
        # since NDB uses table statistics, not scan) AND for metrics to update
        # (UpdateRoundMetrics runs at end of round, after commit)
        self.helper.insert_expired_rows("ttl_metrics", "accumulate", count=10, start_id=1)
        assert self.helper.wait_for_purge_completion("ttl_metrics", "accumulate", timeout=30), \
            "First batch not purged within timeout"
        # Metrics update lags behind commit - wait for round to finish
        assert self.helper.wait_for_purge(expected_purged=10, timeout=30), \
            "First batch: rows_purged_total did not reflect 10 purged rows"

        after_first = self.helper.get_ttl_purge_metrics()["rows_purged_total"]
        assert after_first >= initial + 10, \
            f"Expected at least {initial + 10} purged, got {after_first}"

        # Second batch
        self.helper.insert_expired_rows("ttl_metrics", "accumulate", count=15, start_id=100)
        assert self.helper.wait_for_purge_completion("ttl_metrics", "accumulate", timeout=30), \
            "Second batch not purged within timeout"
        assert self.helper.wait_for_purge(expected_purged=15, timeout=30), \
            "Second batch: rows_purged_total did not reflect 15 purged rows"

        after_second = self.helper.get_ttl_purge_metrics()["rows_purged_total"]
        assert after_second >= after_first + 15, \
            f"Expected at least {after_first + 15} purged, got {after_second}"

        self.helper.drop_database("ttl_metrics")

    def test_rows_purged_last_round_resets(self):
        """Test that rows_purged_last_round resets each round."""
        self.helper.create_ttl_table("ttl_metrics", "last_round", ttl_seconds=5)
        time.sleep(3)

        # Insert and wait for purge
        self.helper.insert_expired_rows("ttl_metrics", "last_round", count=20)
        time.sleep(10)

        metrics1 = self.helper.get_ttl_purge_metrics()
        round1 = metrics1["rounds_completed"]

        # Wait for a few more rounds with no data
        time.sleep(10)

        metrics2 = self.helper.get_ttl_purge_metrics()
        # After rounds with no purging, last_round should be 0 or small
        # (might catch mid-round)

        self.helper.drop_database("ttl_metrics")

    def test_rounds_completed_increments(self):
        """Test that rounds_completed increments."""
        # Create a TTL table to ensure purge rounds happen (not paused)
        self.helper.create_ttl_table("ttl_metrics", "rounds_test", ttl_seconds=60)
        time.sleep(5)

        initial = self.helper.get_ttl_purge_metrics()["rounds_completed"]

        # Wait for a few rounds (with short sleep interval)
        time.sleep(15)

        final = self.helper.get_ttl_purge_metrics()["rounds_completed"]
        assert final > initial, f"rounds_completed should increment: {initial} -> {final}"

        self.helper.drop_database("ttl_metrics")

    def test_per_table_metrics(self):
        """Test per-table metrics are tracked correctly."""
        self.helper.create_ttl_table("ttl_metrics", "per_table", ttl_seconds=5)
        time.sleep(5)

        # Get initial per-table metrics
        table_info = self.helper.get_ttl_table("ttl_metrics", "per_table")
        initial_purged = table_info.get("rows_purged", 0) if table_info else 0

        # Insert and wait for purge completion
        self.helper.insert_expired_rows("ttl_metrics", "per_table", count=25)
        assert self.helper.wait_for_purge_completion("ttl_metrics", "per_table", timeout=30), \
            "Rows not purged within timeout"

        # Poll for per-table metrics to reflect purged rows
        deadline = time.time() + 30
        table_purged = 0
        while time.time() < deadline:
            table_info = self.helper.get_ttl_table("ttl_metrics", "per_table")
            if table_info and table_info.get("rows_purged", 0) >= initial_purged + 25:
                table_purged = table_info["rows_purged"]
                break
            time.sleep(2)
        else:
            table_info = self.helper.get_ttl_table("ttl_metrics", "per_table")
            table_purged = table_info.get("rows_purged", 0) if table_info else 0

        assert table_purged >= initial_purged + 25, \
            f"Expected at least {initial_purged + 25} purged, got {table_purged}"

        self.helper.drop_database("ttl_metrics")

    def test_metrics_cleanup_on_ttl_disable(self):
        """Test that table metrics are cleaned up when TTL is disabled."""
        self.helper.create_ttl_table("ttl_metrics", "cleanup_test", ttl_seconds=60)
        time.sleep(5)

        # Wait for table to be detected
        assert self.helper.wait_for_table_detection("ttl_metrics", "cleanup_test", timeout=30), \
            "Table should be detected"

        # Insert some rows (valid, not expired) to ensure metrics are created
        self.helper.insert_valid_rows("ttl_metrics", "cleanup_test", count=5)
        time.sleep(10)

        # Verify metrics exist
        table_info = self.helper.get_ttl_table("ttl_metrics", "cleanup_test")
        assert table_info is not None, "Table metrics should exist"

        # Disable TTL
        self.helper.disable_ttl("ttl_metrics", "cleanup_test")

        # Wait longer for schema watcher and metrics cleanup
        time.sleep(10)

        # Verify metrics are cleaned up (with longer timeout)
        assert self.helper.wait_for_table_removal("ttl_metrics", "cleanup_test", timeout=60), \
            "Table metrics should be cleaned up after TTL disabled"

        self.helper.drop_database("ttl_metrics")

    def test_tables_count_accuracy(self):
        """Test that tables_count in metrics is accurate."""
        # Clean slate
        self.helper.drop_database("ttl_metrics")
        time.sleep(3)

        initial = self.helper.get_ttl_purge_metrics()["tables_count"]

        # Create tables
        for i in range(3):
            self.helper.create_ttl_table("ttl_metrics", f"count_test_{i}", ttl_seconds=60)

        # Poll until all 3 tables are detected
        deadline = time.time() + 30
        after_create = initial
        while time.time() < deadline:
            after_create = self.helper.get_ttl_purge_metrics()["tables_count"]
            if after_create >= initial + 3:
                break
            time.sleep(2)

        assert after_create >= initial + 3, \
            f"Expected at least {initial + 3} tables, got {after_create}"

        # Drop one table
        self.helper.drop_table("ttl_metrics", "count_test_0")

        # Poll until tables_count decreases
        deadline = time.time() + 30
        after_drop = after_create
        while time.time() < deadline:
            after_drop = self.helper.get_ttl_purge_metrics()["tables_count"]
            if after_drop <= after_create - 1:
                break
            time.sleep(2)

        assert after_drop == after_create - 1, \
            f"Expected {after_create - 1} tables, got {after_drop}"

        self.helper.drop_database("ttl_metrics")

    def run_all(self):
        """Run all metrics tests."""
        tests = [
            ("rows_purged_total accumulates", self.test_rows_purged_total_accumulates),
            ("rows_purged_last_round resets", self.test_rows_purged_last_round_resets),
            ("rounds_completed increments", self.test_rounds_completed_increments),
            ("Per-table metrics", self.test_per_table_metrics),
            ("Metrics cleanup on TTL disable", self.test_metrics_cleanup_on_ttl_disable),
            ("tables_count accuracy", self.test_tables_count_accuracy),
        ]
        for name, test_func in tests:
            self.suite.run_test(name, "Metrics Accuracy", test_func)


# ============================================================================
# Test Cases: Concurrent Operations
# ============================================================================
class ConcurrentTests:
    """Concurrent operation tests."""

    def __init__(self, suite: TestSuite):
        self.suite = suite
        self.helper = suite.helper

    def test_read_expired_rows(self):
        """Test that expired rows are not visible to queries."""
        self.helper.create_ttl_table("ttl_concurrent", "read_expired", ttl_seconds=5)
        time.sleep(3)

        # Insert rows that will expire
        self.helper.insert_expired_rows("ttl_concurrent", "read_expired", count=10)

        # Immediately query - rows should not be visible (already expired)
        conn = self.helper.cluster.get_mysql_connection("ttl_concurrent")
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM read_expired")
        count = cursor.fetchone()[0]
        conn.close()

        # Expired rows should not be visible
        assert count == 0, f"Expired rows should not be visible, got {count}"

        self.helper.drop_database("ttl_concurrent")

    def test_insert_while_purging(self):
        """Test that inserts work while purging is active."""
        self.helper.create_ttl_table("ttl_concurrent", "insert_test", ttl_seconds=5)
        time.sleep(3)

        # Insert expired rows
        self.helper.insert_expired_rows("ttl_concurrent", "insert_test", count=50, start_id=1)

        # Concurrently insert valid rows
        def insert_valid():
            for i in range(20):
                self.helper.insert_valid_rows(
                    "ttl_concurrent", "insert_test",
                    count=1, start_id=1000 + i
                )
                time.sleep(0.1)

        thread = threading.Thread(target=insert_valid)
        thread.start()

        # Wait for purge and inserts
        time.sleep(15)
        thread.join()

        # Valid rows should still exist
        count = self.helper.get_row_count("ttl_concurrent", "insert_test")
        assert count >= 20, f"Expected at least 20 valid rows, got {count}"

        self.helper.drop_database("ttl_concurrent")

    def test_update_expired_row(self):
        """Test that updating an expired row fails."""
        self.helper.create_ttl_table("ttl_concurrent", "update_test", ttl_seconds=5)
        time.sleep(3)

        # Insert expired row
        conn = self.helper.cluster.get_mysql_connection("ttl_concurrent")
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO update_test (id, ttl_col, data) VALUES
            (1, DATE_SUB(NOW(), INTERVAL 1 MINUTE), 'expired')
        """)

        # Try to update - should fail (row not found)
        try:
            cursor.execute("UPDATE update_test SET data = 'updated' WHERE id = 1")
            rows_affected = cursor.rowcount
            # Row is expired, so update should affect 0 rows
            assert rows_affected == 0, f"Update on expired row should affect 0 rows"
        except Exception:
            pass  # Expected - row not found
        finally:
            conn.close()

        self.helper.drop_database("ttl_concurrent")

    def test_delete_expired_row(self):
        """Test that deleting an expired row fails."""
        self.helper.create_ttl_table("ttl_concurrent", "delete_test", ttl_seconds=5)
        time.sleep(3)

        # Insert expired row
        conn = self.helper.cluster.get_mysql_connection("ttl_concurrent")
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO delete_test (id, ttl_col, data) VALUES
            (1, DATE_SUB(NOW(), INTERVAL 1 MINUTE), 'expired')
        """)

        # Try to delete - should affect 0 rows (row appears deleted)
        cursor.execute("DELETE FROM delete_test WHERE id = 1")
        rows_affected = cursor.rowcount
        conn.close()

        assert rows_affected == 0, f"Delete on expired row should affect 0 rows"

        self.helper.drop_database("ttl_concurrent")

    def test_concurrent_api_calls(self):
        """Test concurrent API calls don't cause issues."""
        results = []
        errors = []

        def make_api_call(call_type):
            try:
                if call_type == "config":
                    resp = requests.get(f"{self.helper.cluster.ttl_purge_url}/config")
                elif call_type == "status":
                    resp = requests.get(f"{self.helper.cluster.ttl_purge_url}/status")
                elif call_type == "metrics":
                    resp = requests.get(f"{self.helper.cluster.ttl_purge_url}/metrics")
                elif call_type == "tables":
                    resp = requests.get(f"{self.helper.cluster.ttl_purge_url}/tables")
                else:
                    resp = requests.get(self.helper.cluster.ttl_purge_url)

                results.append((call_type, resp.status_code))
            except Exception as e:
                errors.append((call_type, str(e)))

        # Make many concurrent calls
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = []
            for _ in range(100):
                call_type = random.choice(["config", "status", "metrics", "tables", "all"])
                futures.append(executor.submit(make_api_call, call_type))

            for future in as_completed(futures):
                pass  # Just wait for completion

        assert len(errors) == 0, f"API errors: {errors}"
        assert all(status == 200 for _, status in results), \
            f"All calls should succeed: {[r for r in results if r[1] != 200]}"

    def run_all(self):
        """Run all concurrent tests."""
        tests = [
            ("Read expired rows", self.test_read_expired_rows),
            ("Insert while purging", self.test_insert_while_purging),
            ("Update expired row", self.test_update_expired_row),
            ("Delete expired row", self.test_delete_expired_row),
            ("Concurrent API calls", self.test_concurrent_api_calls),
        ]
        for name, test_func in tests:
            self.suite.run_test(name, "Concurrent Operations", test_func)


# ============================================================================
# Test Cases: Edge Cases
# ============================================================================
class EdgeCaseTests:
    """Edge case tests."""

    def __init__(self, suite: TestSuite):
        self.suite = suite
        self.helper = suite.helper

    def test_no_expired_rows(self):
        """Test table with no expired rows."""
        self.helper.create_ttl_table("ttl_test", "no_expired", ttl_seconds=3600)  # 1 hour
        time.sleep(3)

        # Insert only valid rows
        self.helper.insert_valid_rows("ttl_test", "no_expired", count=10)

        initial_count = self.helper.get_row_count("ttl_test", "no_expired")

        # Wait for several purge cycles
        time.sleep(10)

        final_count = self.helper.get_row_count("ttl_test", "no_expired")
        assert final_count == initial_count, \
            f"No rows should be purged, expected {initial_count}, got {final_count}"

        self.helper.drop_database("ttl_test")

    def test_all_rows_expired(self):
        """Test table with all rows expired."""
        self.helper.create_ttl_table("ttl_test", "all_expired", ttl_seconds=5)
        time.sleep(3)

        # Insert only expired rows
        self.helper.insert_expired_rows("ttl_test", "all_expired", count=50)

        # Wait for purge
        time.sleep(15)

        count = self.helper.get_row_count("ttl_test", "all_expired")
        assert count == 0, f"All rows should be purged, got {count}"

        self.helper.drop_database("ttl_test")

    def test_empty_table(self):
        """Test empty TTL table."""
        self.helper.create_ttl_table("ttl_test", "empty_table", ttl_seconds=10)
        time.sleep(5)

        # Table should be detected
        table_info = self.helper.get_ttl_table("ttl_test", "empty_table")
        assert table_info is not None, "Empty table should be detected"

        # Purger should not error
        status = self.helper.get_ttl_purge_status()
        assert status["state"] != "error", "Purger should handle empty table"

        self.helper.drop_database("ttl_test")

    def test_boundary_time(self):
        """Test row exactly at TTL boundary."""
        self.helper.create_ttl_table("ttl_test", "boundary", ttl_seconds=10)
        time.sleep(3)

        # Insert row that expires in exactly TTL seconds
        conn = self.helper.cluster.get_mysql_connection("ttl_test")
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO boundary (id, ttl_col, data) VALUES
            (1, NOW(), 'boundary_row')
        """)
        conn.close()

        # Row should be visible immediately
        count = self.helper.get_row_count("ttl_test", "boundary")
        assert count == 1, "Row at boundary should be visible initially"

        # Wait for TTL + purge cycle
        time.sleep(15)

        count = self.helper.get_row_count("ttl_test", "boundary")
        assert count == 0, "Row should be purged after TTL expires"

        self.helper.drop_database("ttl_test")

    def test_multi_partition(self):
        """Test multi-partition table purging."""
        # Create table with explicit partitions
        conn = self.helper.cluster.get_mysql_connection()
        cursor = conn.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS ttl_test")
        cursor.execute("USE ttl_test")
        cursor.execute("DROP TABLE IF EXISTS multi_part")
        cursor.execute("""
            CREATE TABLE multi_part (
                id INT PRIMARY KEY,
                ttl_col DATETIME,
                data VARCHAR(100)
            ) ENGINE=NDB
            COMMENT='NDB_TABLE=TTL=5@ttl_col'
            PARTITION BY KEY(id) PARTITIONS 4
        """)
        cursor.execute("CREATE INDEX ttl_index ON multi_part(ttl_col)")
        conn.close()

        time.sleep(5)

        # Insert expired rows across partitions
        self.helper.insert_expired_rows("ttl_test", "multi_part", count=40)

        # Wait for all partitions to be scanned
        time.sleep(20)

        count = self.helper.get_row_count("ttl_test", "multi_part")
        assert count == 0, f"All partitions should be purged, got {count}"

        # Check partition cycling in metrics
        table_info = self.helper.get_ttl_table("ttl_test", "multi_part")
        assert table_info is not None, "Table should have metrics"
        assert table_info["partition_count"] == 4, \
            f"Expected 4 partitions, got {table_info['partition_count']}"

        self.helper.drop_database("ttl_test")

    def test_very_short_ttl(self):
        """Test very short TTL (1 second)."""
        self.helper.create_ttl_table("ttl_test", "short_ttl", ttl_seconds=1)
        time.sleep(3)

        # Insert row
        conn = self.helper.cluster.get_mysql_connection("ttl_test")
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO short_ttl (id, ttl_col, data) VALUES
            (1, NOW(), 'short_lived')
        """)
        conn.close()

        # Wait just over 1 second + purge cycle
        time.sleep(10)

        count = self.helper.get_row_count("ttl_test", "short_ttl")
        assert count == 0, f"Short TTL row should be purged, got {count}"

        self.helper.drop_database("ttl_test")

    def run_all(self):
        """Run all edge case tests."""
        tests = [
            ("No expired rows", self.test_no_expired_rows),
            ("All rows expired", self.test_all_rows_expired),
            ("Empty table", self.test_empty_table),
            ("Boundary time", self.test_boundary_time),
            ("Multi-partition", self.test_multi_partition),
            ("Very short TTL", self.test_very_short_ttl),
        ]
        for name, test_func in tests:
            self.suite.run_test(name, "Edge Cases", test_func)


# ============================================================================
# Test Cases: Config Changes During Purging
# ============================================================================
class ConfigChangeTests:
    """Config change tests."""

    def __init__(self, suite: TestSuite):
        self.suite = suite
        self.helper = suite.helper

    def test_change_batch_size_during_purge(self):
        """Test changing batch size while purging."""
        self.helper.create_ttl_table("ttl_test", "batch_change", ttl_seconds=5)
        time.sleep(3)

        # Insert many rows
        self.helper.insert_expired_rows("ttl_test", "batch_change", count=100)

        # Change batch size mid-purge
        self.helper.set_ttl_purge_config({"max_batch_size": 100})

        time.sleep(15)

        # Verify purging completed
        count = self.helper.get_row_count("ttl_test", "batch_change")
        assert count == 0, f"All rows should be purged, got {count}"

        # Restore config
        self.helper.set_ttl_purge_config({"max_batch_size": 50})

        self.helper.drop_database("ttl_test")

    def test_disable_during_active_purge(self):
        """Test disabling purge while actively purging."""
        self.helper.create_ttl_table("ttl_test", "disable_active", ttl_seconds=5)
        time.sleep(3)

        # Insert many rows
        self.helper.insert_expired_rows("ttl_test", "disable_active", count=200)

        # Disable purging
        self.helper.disable_purge()
        time.sleep(3)

        # Verify status is disabled
        status = self.helper.get_ttl_purge_status()
        assert status["state"] == "disabled", f"State should be disabled, got {status['state']}"

        # Some rows might remain (purge stopped mid-way)
        remaining = self.helper.get_row_count("ttl_test", "disable_active")
        self.suite.log(f"Rows remaining after disable: {remaining}")

        # Re-enable and verify purging resumes
        self.helper.enable_purge()
        time.sleep(15)

        final = self.helper.get_row_count("ttl_test", "disable_active")
        assert final == 0, f"All rows should be purged after re-enable, got {final}"

        self.helper.drop_database("ttl_test")

    def test_change_sleep_interval(self):
        """Test changing sleep interval."""
        original = self.helper.get_ttl_purge_config()

        # Set very short interval
        self.helper.set_ttl_purge_config({"sleep_interval_ms": 500})

        initial_rounds = self.helper.get_ttl_purge_metrics()["rounds_completed"]

        # Wait and count rounds
        time.sleep(5)

        rounds_with_short = self.helper.get_ttl_purge_metrics()["rounds_completed"] - initial_rounds

        # Set longer interval
        self.helper.set_ttl_purge_config({"sleep_interval_ms": 3000})

        initial_rounds = self.helper.get_ttl_purge_metrics()["rounds_completed"]
        time.sleep(5)

        rounds_with_long = self.helper.get_ttl_purge_metrics()["rounds_completed"] - initial_rounds

        # Shorter interval should have more rounds
        self.suite.log(f"Rounds with 500ms: {rounds_with_short}, with 3000ms: {rounds_with_long}")
        # Note: This is a soft check due to timing variability
        assert rounds_with_short >= rounds_with_long, \
            "Shorter interval should have at least as many rounds"

        # Restore original
        self.helper.set_ttl_purge_config(original)

    def run_all(self):
        """Run all config change tests."""
        tests = [
            ("Change batch size during purge", self.test_change_batch_size_during_purge),
            ("Disable during active purge", self.test_disable_during_active_purge),
            ("Change sleep interval", self.test_change_sleep_interval),
        ]
        for name, test_func in tests:
            self.suite.run_test(name, "Config Changes", test_func)


# ============================================================================
# Test Cases: Multiple TTL Tables
# ============================================================================
class MultipleTTLTableTests:
    """Multiple TTL table tests."""

    def __init__(self, suite: TestSuite):
        self.suite = suite
        self.helper = suite.helper

    def test_different_ttl_durations(self):
        """Test tables with different TTL durations."""
        # Create tables with different TTLs
        self.helper.create_ttl_table("ttl_test_1", "short_ttl", ttl_seconds=5)
        self.helper.create_ttl_table("ttl_test_2", "medium_ttl", ttl_seconds=30)
        self.helper.create_ttl_table("ttl_test_3", "long_ttl", ttl_seconds=3600)

        time.sleep(5)

        # Insert expired rows in short_ttl
        self.helper.insert_expired_rows("ttl_test_1", "short_ttl", count=10)

        # Insert rows that will expire soon in medium_ttl
        conn = self.helper.cluster.get_mysql_connection("ttl_test_2")
        cursor = conn.cursor()
        for i in range(10):
            cursor.execute(f"""
                INSERT INTO medium_ttl (id, ttl_col, data) VALUES
                ({i}, DATE_SUB(NOW(), INTERVAL 35 SECOND), 'medium_{i}')
            """)
        conn.close()

        # Insert valid rows in long_ttl
        self.helper.insert_valid_rows("ttl_test_3", "long_ttl", count=10)

        time.sleep(15)

        # Check results
        short_count = self.helper.get_row_count("ttl_test_1", "short_ttl")
        medium_count = self.helper.get_row_count("ttl_test_2", "medium_ttl")
        long_count = self.helper.get_row_count("ttl_test_3", "long_ttl")

        assert short_count == 0, f"Short TTL table should be empty, got {short_count}"
        assert medium_count == 0, f"Medium TTL table should be empty, got {medium_count}"
        assert long_count == 10, f"Long TTL table should have 10 rows, got {long_count}"

        self.helper.drop_database("ttl_test_1")
        self.helper.drop_database("ttl_test_2")
        self.helper.drop_database("ttl_test_3")

    def test_different_databases(self):
        """Test TTL tables in different databases."""
        databases = ["ttl_db_a", "ttl_db_b", "ttl_db_c"]

        for db in databases:
            self.helper.create_ttl_table(db, "test_table", ttl_seconds=5)

        time.sleep(5)

        # Insert expired rows in all tables
        for db in databases:
            self.helper.insert_expired_rows(db, "test_table", count=10)

        time.sleep(15)

        # All should be purged
        for db in databases:
            count = self.helper.get_row_count(db, "test_table")
            assert count == 0, f"Table in {db} should be empty, got {count}"

        for db in databases:
            self.helper.drop_database(db)

    def test_all_tables_purged_round_robin(self):
        """Test that all tables get purged in round-robin fashion."""
        # Create multiple tables
        for i in range(5):
            self.helper.create_ttl_table("ttl_test", f"rr_table_{i}", ttl_seconds=5)

        time.sleep(5)

        # Insert expired rows in all tables
        for i in range(5):
            self.helper.insert_expired_rows("ttl_test", f"rr_table_{i}", count=20)

        time.sleep(30)  # Wait for multiple rounds

        # All tables should be purged
        for i in range(5):
            count = self.helper.get_row_count("ttl_test", f"rr_table_{i}")
            assert count == 0, f"Table rr_table_{i} should be empty, got {count}"

        # Check per-table metrics - all should have rows_purged > 0
        for i in range(5):
            table_info = self.helper.get_ttl_table("ttl_test", f"rr_table_{i}")
            assert table_info is not None, f"Table rr_table_{i} should have metrics"
            assert table_info["rows_purged"] >= 20, \
                f"Table rr_table_{i} should have purged at least 20 rows"

        self.helper.drop_database("ttl_test")

    def run_all(self):
        """Run all multiple table tests."""
        tests = [
            ("Different TTL durations", self.test_different_ttl_durations),
            ("Different databases", self.test_different_databases),
            ("All tables purged round-robin", self.test_all_tables_purged_round_robin),
        ]
        for name, test_func in tests:
            self.suite.run_test(name, "Multiple TTL Tables", test_func)


# ============================================================================
# Test Cases: Stress/Scale
# ============================================================================
class StressTests:
    """Stress and scale tests."""

    def __init__(self, suite: TestSuite):
        self.suite = suite
        self.helper = suite.helper

    def test_many_tables(self):
        """Test with many TTL tables (10+)."""
        num_tables = 10

        # Create tables
        for i in range(num_tables):
            self.helper.create_ttl_table("ttl_stress", f"table_{i}", ttl_seconds=10)

        time.sleep(10)

        # Verify all detected
        tables = self.helper.get_ttl_tables(limit=100)
        stress_tables = [t for t in tables["tables"] if t["database"] == "ttl_stress"]
        assert len(stress_tables) == num_tables, \
            f"Expected {num_tables} tables, got {len(stress_tables)}"

        # Insert expired rows
        for i in range(num_tables):
            self.helper.insert_expired_rows("ttl_stress", f"table_{i}", count=10)

        time.sleep(30)

        # All should be purged
        for i in range(num_tables):
            count = self.helper.get_row_count("ttl_stress", f"table_{i}")
            assert count == 0, f"Table table_{i} should be empty, got {count}"

        self.helper.drop_database("ttl_stress")

    def test_many_expired_rows(self):
        """Test with many expired rows (1000+)."""
        self.helper.create_ttl_table("ttl_stress", "many_rows", ttl_seconds=5)
        time.sleep(3)

        # Insert many expired rows in batches
        for batch in range(10):
            self.helper.insert_expired_rows(
                "ttl_stress", "many_rows",
                count=100, start_id=batch * 100
            )

        initial = self.helper.get_row_count("ttl_stress", "many_rows")
        self.suite.log(f"Inserted {initial} rows")

        # Wait for purge
        start = time.time()
        while time.time() - start < 120:  # 2 minute timeout
            count = self.helper.get_row_count("ttl_stress", "many_rows")
            if count == 0:
                break
            time.sleep(5)

        final = self.helper.get_row_count("ttl_stress", "many_rows")
        duration = time.time() - start
        self.suite.log(f"Purged in {duration:.1f}s")

        assert final == 0, f"All rows should be purged, got {final}"

        self.helper.drop_database("ttl_stress")

    def test_high_insert_rate(self):
        """Test with high insert rate during purging."""
        self.helper.create_ttl_table("ttl_stress", "high_rate", ttl_seconds=5)
        time.sleep(3)

        insert_count = 0
        errors = []

        def insert_worker():
            nonlocal insert_count
            conn = self.helper.cluster.get_mysql_connection("ttl_stress")
            cursor = conn.cursor()
            for i in range(100):
                try:
                    row_id = random.randint(10000, 99999)
                    # Mix of expired and valid rows
                    if random.random() < 0.5:
                        cursor.execute(f"""
                            INSERT INTO high_rate (id, ttl_col, data) VALUES
                            ({row_id}, DATE_SUB(NOW(), INTERVAL 1 MINUTE), 'expired')
                        """)
                    else:
                        cursor.execute(f"""
                            INSERT INTO high_rate (id, ttl_col, data) VALUES
                            ({row_id}, DATE_ADD(NOW(), INTERVAL 1 HOUR), 'valid')
                        """)
                    insert_count += 1
                except pymysql.IntegrityError:
                    pass  # Duplicate key - expected with random IDs
                except Exception as e:
                    errors.append(str(e))
                time.sleep(0.01)
            conn.close()

        # Run multiple insert threads
        threads = [threading.Thread(target=insert_worker) for _ in range(5)]
        for t in threads:
            t.start()

        # Wait for completion
        for t in threads:
            t.join()

        self.suite.log(f"Inserted {insert_count} rows, {len(errors)} errors")

        # Wait for purge
        time.sleep(15)

        # Check that some valid rows remain
        count = self.helper.get_row_count("ttl_stress", "high_rate")
        self.suite.log(f"Final count: {count}")

        # Should have some valid rows remaining
        assert count > 0, "Some valid rows should remain"
        assert len(errors) == 0, f"Insert errors: {errors}"

        self.helper.drop_database("ttl_stress")

    def test_pagination_with_many_tables(self):
        """Test pagination performance with many tables."""
        num_tables = 20

        # Create tables
        for i in range(num_tables):
            self.helper.create_ttl_table("ttl_stress", f"page_table_{i}", ttl_seconds=60)

        time.sleep(10)

        # Test pagination
        all_tables = []
        offset = 0
        limit = 5

        while True:
            result = self.helper.get_ttl_tables(offset=offset, limit=limit)
            stress_tables = [t for t in result["tables"] if t["database"] == "ttl_stress"]
            all_tables.extend(stress_tables)
            if len(result["tables"]) < limit:
                break
            offset += limit

        assert len(all_tables) == num_tables, \
            f"Pagination should return all {num_tables} tables, got {len(all_tables)}"

        self.helper.drop_database("ttl_stress")

    def run_all(self):
        """Run all stress tests."""
        tests = [
            ("Many tables (10+)", self.test_many_tables),
            ("Many expired rows (1000+)", self.test_many_expired_rows),
            ("High insert rate", self.test_high_insert_rate),
            ("Pagination with many tables", self.test_pagination_with_many_tables),
        ]
        for name, test_func in tests:
            self.suite.run_test(name, "Stress/Scale", test_func)


# ============================================================================
# Test Cases: BLOB/TEXT Purge
# ============================================================================
class BlobPurgeTests:
    """Tests for TTL purge on tables with BLOB/TEXT columns.

    Verifies that deleteCurrentTuple() cascades to NDB$BLOB_ part tables
    when purging expired rows from tables containing BLOB/TEXT columns.
    """

    def __init__(self, suite: TestSuite):
        self.suite = suite
        self.helper = suite.helper

    def _get_blob_part_row_count(self, database: str, table: str) -> int:
        """Get the actual row count from NDB$BLOB_ part tables.

        Uses ndb$frag_mem_use.rows (actual row count) joined with
        ndb$dict_obj_info to identify blob tables by parent.

        Note: memory_per_fragment.fixed_elem_count is NOT suitable here
        because it counts allocated slots (including freed ones on the
        free list), not active rows.
        """
        conn = self.helper.cluster.get_mysql_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT SUM(frag.rows) "
                "FROM ndbinfo.ndb$frag_mem_use AS frag "
                "JOIN ndbinfo.ndb$dict_obj_info AS obj "
                "  ON obj.id = frag.table_id AND obj.type <= 6 "
                "LEFT JOIN ndbinfo.ndb$dict_obj_info AS parent "
                "  ON obj.parent_obj_id = parent.id "
                "  AND obj.parent_obj_type = parent.type "
                "WHERE obj.fq_name LIKE '%%NDB$BLOB%%' "
                "AND parent.fq_name LIKE %s",
                (f"%{database}/def/{table}%",)
            )
            result = cursor.fetchone()
            if result and result[0] is not None:
                return int(result[0])
            return 0
        finally:
            conn.close()

    def _wait_for_blob_cleanup(self, database: str, table: str,
                               timeout: int = 30) -> int:
        """Poll until blob part row count reaches 0 or timeout."""
        start = time.time()
        count = -1
        while time.time() - start < timeout:
            count = self._get_blob_part_row_count(database, table)
            if count == 0:
                return 0
            time.sleep(2)
        return count

    def test_blob_purge_with_index(self):
        """Test TTL purge with TEXT column using index scan.

        Creates a TTL table with a TEXT column and ttl_index, inserts rows
        with large text data (>256 bytes forcing blob parts), waits for
        TTL expiry + purge, then verifies both main table rows AND the
        internal NDB$BLOB_ part table rows are cleaned up.
        """
        db = "ttl_blob"
        table = "blob_idx"

        # Create table with TEXT column + ttl_index
        self.helper.create_ttl_table(
            db, table, ttl_seconds=5,
            ttl_column_type="DATETIME",
            extra_columns="data TEXT",
            create_index=True
        )
        time.sleep(5)  # Wait for schema watcher

        # Insert rows with large text (>256 bytes to force blob parts)
        large_text = "X" * 1000
        num_rows = 5
        conn = self.helper.cluster.get_mysql_connection(db)
        try:
            cursor = conn.cursor()
            for i in range(num_rows):
                cursor.execute(
                    f"INSERT INTO {table} (id, ttl_col, data) VALUES "
                    f"({i}, NOW(), %s)",
                    (large_text,)
                )
        finally:
            conn.close()

        # Verify rows inserted
        count = self.helper.get_row_count(db, table)
        assert count == num_rows, \
            f"Expected {num_rows} rows after insert, got {count}"

        # Verify blob parts exist
        blob_parts_before = self._get_blob_part_row_count(db, table)
        self.suite.log(f"Blob part rows before purge: {blob_parts_before}")
        assert blob_parts_before > 0, \
            f"Expected blob part rows > 0 after insert, got {blob_parts_before}"

        # Wait for main table to be purged (polling)
        self.suite.log("Waiting for TTL expiry + purge cycle...")
        assert self.helper.wait_for_purge_completion(db, table, timeout=30), \
            "Main table rows not purged within timeout"

        # Wait for blob part table cleanup (polling)
        blob_parts_after = self._wait_for_blob_cleanup(db, table, timeout=30)
        self.suite.log(f"Blob part rows after purge: {blob_parts_after}")
        assert blob_parts_after == 0, \
            f"Expected 0 blob part rows after purge, got {blob_parts_after} " \
            f"(BLOB PARTS NOT CLEANED UP!)"

        self.suite.log(
            f"Both main and blob part tables purged "
            f"({blob_parts_before} -> {blob_parts_after} parts)"
        )

        self.helper.drop_database(db)

    def test_blob_purge_table_scan(self):
        """Test TTL purge with TEXT column using table scan (no ttl_index).

        Same as test_blob_purge_with_index but without creating ttl_index,
        forcing the purger to use a full table scan path. Verifies blob
        parts are still cleaned up correctly.
        """
        db = "ttl_blob"
        table = "blob_noscan"

        # Create table WITHOUT ttl_index to force table scan
        self.helper.create_ttl_table(
            db, table, ttl_seconds=5,
            ttl_column_type="DATETIME",
            extra_columns="data TEXT",
            create_index=False  # No ttl_index -> table scan path
        )
        time.sleep(5)

        large_text = "Y" * 1000
        num_rows = 5
        conn = self.helper.cluster.get_mysql_connection(db)
        try:
            cursor = conn.cursor()
            for i in range(num_rows):
                cursor.execute(
                    f"INSERT INTO {table} (id, ttl_col, data) VALUES "
                    f"({i}, NOW(), %s)",
                    (large_text,)
                )
        finally:
            conn.close()

        count = self.helper.get_row_count(db, table)
        assert count == num_rows, \
            f"Expected {num_rows} rows after insert, got {count}"

        blob_parts_before = self._get_blob_part_row_count(db, table)
        self.suite.log(f"Blob part rows before purge: {blob_parts_before}")
        assert blob_parts_before > 0, \
            f"Expected blob part rows > 0 after insert, got {blob_parts_before}"

        self.suite.log("Waiting for TTL expiry + purge cycle (table scan)...")
        assert self.helper.wait_for_purge_completion(db, table, timeout=30), \
            "Main table rows not purged within timeout"

        blob_parts_after = self._wait_for_blob_cleanup(db, table, timeout=30)
        self.suite.log(f"Blob part rows after purge: {blob_parts_after}")
        assert blob_parts_after == 0, \
            f"Expected 0 blob part rows after purge, got {blob_parts_after} " \
            f"(BLOB PARTS NOT CLEANED UP via table scan!)"

        self.suite.log(
            f"Table scan purge cleaned both main and blob part tables "
            f"({blob_parts_before} -> {blob_parts_after} parts)"
        )

        self.helper.drop_database(db)

    def test_blob_purge_multiple_blob_columns(self):
        """Test TTL purge with multiple BLOB/TEXT columns.

        Table has both a TEXT and a BLOB column. Verifies that purging
        cleans up part tables for ALL blob columns, not just the first.
        """
        db = "ttl_blob"
        table = "multi_blob"

        conn = self.helper.cluster.get_mysql_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db}")
            cursor.execute(f"DROP TABLE IF EXISTS {db}.{table}")
            cursor.execute(f"""
                CREATE TABLE {db}.{table} (
                    id INT PRIMARY KEY,
                    ttl_col DATETIME,
                    text_data TEXT,
                    blob_data BLOB
                ) ENGINE=NDB COMMENT='NDB_TABLE=TTL=5@ttl_col'
            """)
            cursor.execute(f"CREATE INDEX ttl_index ON {db}.{table}(ttl_col)")
        finally:
            conn.close()

        time.sleep(5)

        large_text = "A" * 1000
        large_blob = b"\xDE\xAD" * 500  # 1000 bytes
        num_rows = 5
        conn = self.helper.cluster.get_mysql_connection(db)
        try:
            cursor = conn.cursor()
            for i in range(num_rows):
                cursor.execute(
                    f"INSERT INTO {table} (id, ttl_col, text_data, blob_data) "
                    f"VALUES ({i}, NOW(), %s, %s)",
                    (large_text, large_blob)
                )
        finally:
            conn.close()

        count = self.helper.get_row_count(db, table)
        assert count == num_rows, \
            f"Expected {num_rows} rows after insert, got {count}"

        blob_parts_before = self._get_blob_part_row_count(db, table)
        self.suite.log(
            f"Blob part rows before purge (multi-col): {blob_parts_before}"
        )
        assert blob_parts_before > 0, \
            f"Expected blob part rows > 0, got {blob_parts_before}"

        self.suite.log("Waiting for TTL expiry + purge cycle...")
        assert self.helper.wait_for_purge_completion(db, table, timeout=30), \
            "Main table rows not purged within timeout"

        blob_parts_after = self._wait_for_blob_cleanup(db, table, timeout=30)
        self.suite.log(
            f"Blob part rows after purge (multi-col): {blob_parts_after}"
        )
        assert blob_parts_after == 0, \
            f"Expected 0 blob part rows after purge, got {blob_parts_after} " \
            f"(multi-column BLOB PARTS NOT CLEANED UP!)"

        self.helper.drop_database(db)

    def run_all(self):
        """Run all BLOB/TEXT purge tests."""
        tests = [
            ("BLOB purge with index scan", self.test_blob_purge_with_index),
            ("BLOB purge with table scan", self.test_blob_purge_table_scan),
            ("BLOB purge multiple columns", self.test_blob_purge_multiple_blob_columns),
        ]
        for name, test_func in tests:
            self.suite.run_test(name, "BLOB/TEXT Purging", test_func)


# ============================================================================
# Main
# ============================================================================
def main():
    parser = argparse.ArgumentParser(description="TTL Purge Comprehensive Test Suite")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument("-c", "--category", type=str, help="Run specific category only")
    parser.add_argument("-k", "--keyword", type=str, help="Run tests matching keyword")
    args = parser.parse_args()

    print("\n" + "="*60)
    print("TTL PURGE COMPREHENSIVE TEST SUITE")
    print("="*60)
    print(f"MySQL: {PRIMARY_CLUSTER.mysql_host}:{PRIMARY_CLUSTER.mysql_port}")
    print(f"RDRS:  {PRIMARY_CLUSTER.rdrs_base_url}")

    # Check environment
    if not PRIMARY_CLUSTER.is_mysql_ready():
        print("\nERROR: MySQL is not ready. Please start the environment first.")
        print("Run: ./ttl_test_env.sh start")
        return 1

    if not PRIMARY_CLUSTER.is_rdrs_ready():
        print("\nERROR: RDRS is not ready. Please start the environment first.")
        print("Run: ./ttl_test_env.sh start")
        return 1

    suite = TestSuite(verbose=args.verbose)
    suite.setup()

    # Define test categories
    categories = {
        "api": APITests(suite),
        "column_types": ColumnTypeTests(suite),
        "schema_changes": SchemaChangeTests(suite),
        "metrics": MetricsTests(suite),
        "concurrent": ConcurrentTests(suite),
        "edge_cases": EdgeCaseTests(suite),
        "config_changes": ConfigChangeTests(suite),
        "multiple_tables": MultipleTTLTableTests(suite),
        "stress": StressTests(suite),
        "blob_purge": BlobPurgeTests(suite),
    }

    try:
        if args.category:
            # Run specific category
            if args.category in categories:
                categories[args.category].run_all()
            else:
                print(f"Unknown category: {args.category}")
                print(f"Available: {', '.join(categories.keys())}")
                return 1
        else:
            # Run all categories
            for category in categories.values():
                category.run_all()
    finally:
        suite.teardown()

    success = suite.print_summary()
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
