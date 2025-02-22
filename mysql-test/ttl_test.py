#   Copyright (c) 2024, 2025, Hopsworks and/or its affiliates.
#
#   This program is free software; you can redistribute it and/or modify
#   it under the terms of the GNU General Public License, version 2.0,
#   as published by the Free Software Foundation.
#
#   This program is designed to work with certain software (including
#   but not limited to OpenSSL) that is licensed under separate terms,
#   as designated in a particular file or component or in included license
#   documentation.  The authors of MySQL hereby grant you an additional
#   permission to link the program and your derivative works with the
#   separately licensed software that they have either included with
#   the program or referenced in the documentation.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License, version 2.0, for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program; if not, write to the Free Software
#   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

import threading
import pymysql
import time
import subprocess
import os
import importlib.util

# Default env path
BIN_DIR="/home/zhao/workspace/kernelmaker/rondb-bin/bin"

MYSQLD_PORT_P=3306
DATA_DIR_P_1="/home/zhao/workspace/rondb-run/ndbmtd_1"
DATA_DIR_P_2="/home/zhao/workspace/rondb-run/ndbmtd_2"

MGMD_PORT_R=1188
MYSQLD_PORT_R=3308

# Specify your own env path
config_path="/tmp/ttl_config.py"

def case_1_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(5)
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 100, "ASSERT"
            #print(f"ThdA: [{col_a}, {col_b.strftime('%Y-%m-%d %H:%M:%S')}, {col_c}]")
        time.sleep(6)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_1_thdB(conn):
    global B_succ
    try:
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 200)")
        assert False, "ASSERT"
    except pymysql.err.IntegrityError as e:
        assert e.args[0] == 1062, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return

    try:
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 100, "ASSERT"
        time.sleep(6)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_2_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(11)
        cur.execute("COMMIT")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT"
        time.sleep(4)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_2_thdB(conn):
    global B_succ
    try:
        time.sleep(5)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 200)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT"
        time.sleep(5)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_3_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(13)
        cur.execute("COMMIT")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_3_thdB(conn):
    global B_succ
    try:
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 200)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_4_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(5)
        cur.execute("ROLLBACK")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT"
        time.sleep(7)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_4_thdB(conn):
    global B_succ
    try:
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 200)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT"
        time.sleep(8)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_5_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(11)
        cur.execute("ROLLBACK")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT"
        time.sleep(10)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_5_thdB(conn):
    global B_succ
    try:
        time.sleep(5)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 200)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT"
        time.sleep(6)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_6_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(13)
        cur.execute("ROLLBACK")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_6_thdB(conn):
    global B_succ
    try:
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 200)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_7_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(5)
        cur.execute("COMMIT")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 201, "ASSERT"
        time.sleep(7)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_7_thdB(conn):
    global B_succ
    try:
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 200) ON DUPLICATE KEY UPDATE col_c = 201, col_b = NOW()")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 201, "ASSERT"
        time.sleep(8)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return

    cur.close()
    B_succ = True


def case_8_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(11)
        cur.execute("COMMIT")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT"
        time.sleep(4)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_8_thdB(conn):
    global B_succ
    try:
        time.sleep(5)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 200) ON DUPLICATE KEY UPDATE col_c = 201, col_b = NOW()")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT"
        time.sleep(5)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_9_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(13)
        cur.execute("COMMIT")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_9_thdB(conn):
    global B_succ
    try:
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 200) ON DUPLICATE KEY UPDATE col_c = 201, col_b = NOW()")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_10_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(5)
        cur.execute("ROLLBACK")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT"
        time.sleep(7)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_10_thdB(conn):
    global B_succ
    try:
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 200) ON DUPLICATE KEY UPDATE col_c = 201, col_b = NOW()")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT"
        time.sleep(8)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_11_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(11)
        cur.execute("ROLLBACK")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT"
        time.sleep(4)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_11_thdB(conn):
    global B_succ
    try:
        time.sleep(5)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 200) ON DUPLICATE KEY UPDATE col_c = 201, col_b = NOW()")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT"
        time.sleep(5)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_12_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(14)
        cur.execute("ROLLBACK")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_12_thdB(conn):
    global B_succ
    try:
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 200) ON DUPLICATE KEY UPDATE col_c = 201, col_b = NOW()")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_13_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(5)
        cur.execute("COMMIT")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT"
        time.sleep(5)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_13_thdB(conn):
    global B_succ
    try:
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("UPDATE sz SET col_c = 200 WHERE col_a = 1")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 1 and matched_rows == 1, "ASSERT"
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT"
        time.sleep(6)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_14_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(11)
        cur.execute("COMMIT")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_14_thdB(conn):
    global B_succ
    try:
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("UPDATE sz SET col_c = 200 WHERE col_a = 1")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 0 and matched_rows == 0, "ASSERT"
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_15_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(13)
        cur.execute("COMMIT")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_15_thdB(conn):
    global B_succ
    try:
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("UPDATE sz SET col_c = 200 WHERE col_a = 1")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 0 and matched_rows == 0, "ASSERT"
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_16_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(5)
        cur.execute("ROLLBACK")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_16_thdB(conn):
    global B_succ
    try:
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("UPDATE sz SET col_c = 200 WHERE col_a = 1")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 0 and matched_rows == 0, "ASSERT"
        cur.execute("COMMIT")
        time.sleep(6)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_17_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(11)
        cur.execute("ROLLBACK")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_17_thdB(conn):
    global B_succ
    try:
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("UPDATE sz SET col_c = 200 WHERE col_a = 1")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 0 and matched_rows == 0, "ASSERT"
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_18_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(13)
        cur.execute("ROLLBACK")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_18_thdB(conn):
    global B_succ
    try:
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("UPDATE sz SET col_c = 200 WHERE col_a = 1")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 0 and matched_rows == 0, "ASSERT"
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_19_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(5)
        cur.execute("COMMIT")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_19_thdB(conn):
    global B_succ
    try:
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("DELETE FROM sz WHERE col_a = 1")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 1 and matched_rows == 1, "ASSERT"
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_20_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(11)
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_20_thdB(conn):
    global B_succ
    try:
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("DELETE FROM sz WHERE col_a = 1")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 0 and matched_rows == 0, "ASSERT"
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_21_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(5)
        cur.execute("ROLLBACK")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_21_thdB(conn):
    global B_succ
    try:
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("DELETE FROM sz WHERE col_a = 1")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 0 and matched_rows == 0, "ASSERT"
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_22_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(11)
        cur.execute("ROLLBACK")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_22_thdB(conn):
    global B_succ
    try:
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("DELETE FROM sz WHERE col_a = 1")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 0 and matched_rows == 0, "ASSERT"
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_23_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(5)
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        time.sleep(6)
        cur.execute("SET DEBUG_SYNC = 'now SIGNAL go_ahead'");
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_23_thdB(conn):
    global B_succ
    try:
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("SET DEBUG_SYNC = 'ttl_wait_for_row_get_expired_after_reading_4 WAIT_FOR go_ahead'");
        cur.execute("DELETE FROM sz")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        #print(f"c: {changed_rows}, m: {matched_rows}")
        assert changed_rows == 1 and matched_rows == 1, "ASSERT"
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_24_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(7)
        cur.execute("COMMIT")
        time.sleep(4)
        cur.execute("SET DEBUG_SYNC = 'now SIGNAL go_ahead'");
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT1"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 201, "ASSERT2"
        cur.execute("SELECT * FROM sz")
        time.sleep(4)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT3"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_24_thdB(conn):
    global B_succ
    try:
        time.sleep(5)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("SET DEBUG_SYNC = 'ttl_wait_for_row_get_expired_after_reading_1 WAIT_FOR go_ahead'");
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 200) ON DUPLICATE KEY UPDATE col_c = 201, col_b = NOW()")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 2 and matched_rows == 2, "ASSERT"
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT1"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 201, "ASSERT2"
        time.sleep(5)
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT3"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return

    cur.close()
    B_succ = True


def case_25_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(7)
        cur.execute("COMMIT")
        time.sleep(4)
        cur.execute("SET DEBUG_SYNC = 'now SIGNAL go_ahead'");
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT1"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 201, "ASSERT2"
        cur.execute("SELECT * FROM sz")
        time.sleep(3)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_25_thdB(conn):
    global B_succ
    try:
        time.sleep(5)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("SET DEBUG_SYNC = 'ttl_wait_for_row_get_expired_after_reading_2 WAIT_FOR go_ahead'");
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 200) ON DUPLICATE KEY UPDATE col_c = 201, col_b = NOW()")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 2 and matched_rows == 2, "ASSERT"
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT1"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 201, "ASSERT2"
        time.sleep(5)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return

    cur.close()
    B_succ = True

def case_26_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 101)")
        cur.execute("INSERT INTO sz VALUES(2, SYSDATE(), 102)")
        cur.execute("INSERT INTO sz VALUES(3, SYSDATE(), 103)")
        cur.execute("INSERT INTO sz VALUES(4, SYSDATE(), 104)")
        cur.execute("INSERT INTO sz VALUES(5, SYSDATE(), 105)")
        cur.execute("INSERT INTO sz VALUES(6, SYSDATE(), 106)")
        cur.execute("COMMIT")

        time.sleep(11)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            #assert col_a == 1 and col_c == 201, "ASSERT2"
            print(f"[{col_a}, {col_b}, {col_c}]")
        print(f"---")
        cur.execute("SELECT * FROM sz WHERE col_a > 2")
        results = cur.fetchall()
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            #assert col_a == 1 and col_c == 201, "ASSERT2"
            print(f"[{col_a}, {col_b}, {col_c}]")
        print(f"---")
        cur.execute("SELECT * FROM sz WHERE col_c > 102")
        results = cur.fetchall()
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            #assert col_a == 1 and col_c == 201, "ASSERT2"
            print(f"[{col_a}, {col_b}, {col_c}]")
        print(f"---")
        cur.execute("SELECT * FROM sz WHERE col_a > 2 and col_c <= 105")
        results = cur.fetchall()
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            #assert col_a == 1 and col_c == 201, "ASSERT2"
            print(f"[{col_a}, {col_b}, {col_c}]")
        print(f"---")
        cur.execute("SELECT * FROM sz WHERE col_a > 2 and col_c <= 105 ORDER BY col_a")
        results = cur.fetchall()
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            #assert col_a == 1 and col_c == 201, "ASSERT2"
            print(f"[{col_a}, {col_b}, {col_c}]")
        print(f"---")
        cur.execute("SELECT * FROM sz WHERE col_a > 2 and col_c <= 105 ORDER BY col_c")
        # cur.execute("SELECT * FROM sz WHERE col_a >= 3")
        # cur.execute("SELECT * FROM sz WHERE col_a >= 3 and col_a <= 5")
        # cur.execute("SELECT * FROM sz WHERE col_a >= 3 and col_c >= 104")
        # cur.execute("UPDATE sz SET col_c = 666 WHERE col_a >= 3 and col_c >= 104")
        # cur.execute("SET DEBUG_SYNC = 'ttl_wait_for_row_get_expired_after_reading_3 WAIT_FOR go_ahead'");
        # cur.execute("UPDATE sz SET col_c = 666 WHERE col_a <= 1")
        results = cur.fetchall()
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            #assert col_a == 1 and col_c == 201, "ASSERT2"
            print(f"[{col_a}, {col_b}, {col_c}]")
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_26_thdB(conn):
    global B_succ
    try:
        cur = conn.cursor()
        #time.sleep(12)
        #cur.execute("SET DEBUG_SYNC = 'now SIGNAL go_ahead'");
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return

    cur.close()
    B_succ = True


def case_27_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 101)")
        cur.execute("INSERT INTO sz VALUES(2, SYSDATE(), 102)")
        cur.execute("INSERT INTO sz VALUES(3, SYSDATE(), 103)")
        cur.execute("INSERT INTO sz VALUES(4, SYSDATE(), 104)")
        cur.execute("INSERT INTO sz VALUES(5, SYSDATE(), 105)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT"
        time.sleep(11)
        cur.execute("BEGIN")
        cur.execute("DELETE FROM sz")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 0 and matched_rows == 0, "ASSERT"
        cur.execute("SET ttl_expired_rows_visible_in_delete = ON")
        cur.execute("DELETE FROM sz")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 5 and matched_rows == 5, "ASSERT"
        cur.execute("COMMIT")

        cur.execute("SET ttl_expired_rows_visible_in_delete = OFF")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 101)")
        cur.execute("INSERT INTO sz VALUES(2, SYSDATE(), 102)")
        cur.execute("INSERT INTO sz VALUES(3, SYSDATE(), 103)")
        cur.execute("INSERT INTO sz VALUES(4, SYSDATE(), 104)")
        cur.execute("INSERT INTO sz VALUES(5, SYSDATE(), 105)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT"
        time.sleep(11)
        cur.execute("BEGIN")
        cur.execute("DELETE FROM sz WHERE col_a > 1")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 0 and matched_rows == 0, "ASSERT"
        cur.execute("SET ttl_expired_rows_visible_in_delete = ON")
        cur.execute("DELETE FROM sz WHERE col_a > 1")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 4 and matched_rows == 4, "ASSERT"
        cur.execute("COMMIT")

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
        assert len(results) == 5, "ASSERT"
        time.sleep(11)
        cur.execute("BEGIN")
        cur.execute("DELETE FROM sz WHERE col_a > 1 and col_c < 104")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 0 and matched_rows == 0, "ASSERT"
        cur.execute("SET ttl_expired_rows_visible_in_delete = ON")
        cur.execute("DELETE FROM sz WHERE col_a > 1 and col_c < 104")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 2 and matched_rows == 2, "ASSERT"
        cur.execute("COMMIT")

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
        assert len(results) == 5, "ASSERT"
        time.sleep(11)
        cur.execute("BEGIN")
        cur.execute("DELETE FROM sz WHERE col_a > 1 or col_c < 104")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 0 and matched_rows == 0, "ASSERT"
        cur.execute("SET ttl_expired_rows_visible_in_delete = ON")
        cur.execute("DELETE FROM sz WHERE col_a > 1 or col_c < 104")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 5 and matched_rows == 5, "ASSERT"
        cur.execute("COMMIT")

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
        assert len(results) == 5, "ASSERT"
        time.sleep(11)
        cur.execute("BEGIN")
        cur.execute("DELETE FROM sz WHERE col_c > 101 and col_c < 104")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 0 and matched_rows == 0, "ASSERT"
        cur.execute("SET ttl_expired_rows_visible_in_delete = ON")
        cur.execute("DELETE FROM sz WHERE col_c > 101 and col_c < 104")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 2 and matched_rows == 2, "ASSERT"
        cur.execute("COMMIT")

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
        assert len(results) == 5, "ASSERT"
        time.sleep(11)
        cur.execute("BEGIN")
        cur.execute("DELETE FROM sz WHERE col_c > 101 or col_c < 104")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 0 and matched_rows == 0, "ASSERT"
        cur.execute("SET ttl_expired_rows_visible_in_delete = ON")
        cur.execute("DELETE FROM sz WHERE col_c > 101 or col_c < 104")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 5 and matched_rows == 5, "ASSERT"
        cur.execute("COMMIT")

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
        assert len(results) == 5, "ASSERT"
        time.sleep(11)
        cur.execute("BEGIN")
        cur.execute("DELETE FROM sz WHERE col_c = 103")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 0 and matched_rows == 0, "ASSERT"
        cur.execute("SET ttl_expired_rows_visible_in_delete = ON")
        cur.execute("DELETE FROM sz WHERE col_c = 103")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 1 and matched_rows == 1, "ASSERT"
        cur.execute("COMMIT")

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
        assert len(results) == 5, "ASSERT"
        time.sleep(11)
        cur.execute("BEGIN")
        cur.execute("DELETE FROM sz WHERE col_a = 4")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 0 and matched_rows == 0, "ASSERT"
        cur.execute("SET ttl_expired_rows_visible_in_delete = ON")
        cur.execute("DELETE FROM sz WHERE col_a = 4")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 1 and matched_rows == 1, "ASSERT"
        cur.execute("COMMIT")

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
        assert len(results) == 5, "ASSERT"
        time.sleep(11)
        cur.execute("BEGIN")
        cur.execute("DELETE FROM sz WHERE col_c <= 105 LIMIT 2")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 0 and matched_rows == 0, "ASSERT"
        cur.execute("SET ttl_expired_rows_visible_in_delete = ON")
        cur.execute("DELETE FROM sz WHERE col_c <= 105 LIMIT 2")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 2 and matched_rows == 2, "ASSERT"
        cur.execute("DELETE FROM sz WHERE col_c <= 105 LIMIT 2")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 2 and matched_rows == 2, "ASSERT"
        cur.execute("DELETE FROM sz WHERE col_c <= 105 LIMIT 2")
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        assert changed_rows == 1 and matched_rows == 1, "ASSERT"
        cur.execute("COMMIT")

    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_27_thdB(conn):
    global B_succ
    try:
        cur = conn.cursor()
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_28_pre(A_conn, B_conn):
    try:
        cur = A_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS sz1")
        cur.execute("CREATE TABLE test.sz1 ("
                    "col_a INT, "
                    "col_b TIMESTAMP, "
                    "col_c INT, "
                    "PRIMARY KEY(col_a), "
                    "KEY(col_c))"
                    "ENGINE = NDB, "
                    "COMMENT=\"NDB_TABLE=FULLY_REPLICATED=1,TTL=10@col_b\"")
    except Exception as e:
        print(f"PRE Create DB/TABLE failed: {e}")
        A_conn.close()
        B_conn.close()
        exit (-1)

def case_28_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz1 VALUES(1, sysdate(), 1), (2, sysdate(), 2), (3, sysdate(), 3), (4, sysdate(), 4), (5, sysdate(), 5), (6, sysdate(), 6), (7, sysdate(), 7), (8, sysdate(), 8), (9, sysdate(), 9), (10, sysdate(), 10)")
        cur.execute("COMMIT")
        time.sleep(2)
        cur.execute("BEGIN")
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 10, "ASSERT 1"
        cur.execute("SELECT * FROM sz1 where col_a = 6")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT 2"
        cur.execute("SELECT * FROM sz1 where col_a >= 6")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT 3"
        cur.execute("SELECT * FROM sz1 where col_c = 8")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT 4"
        cur.execute("SELECT * FROM sz1 where col_c <= 8")
        results = cur.fetchall()
        assert len(results) == 8, "ASSERT 5"
        time.sleep(9)
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT 6"
        cur.execute("COMMIT")

        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz1 VALUES(1, sysdate(), 1), (2, sysdate(), 2), (3, sysdate(), 3), (4, sysdate(), 4), (5, sysdate(), 5), (6, sysdate(), 6), (7, sysdate(), 7), (8, sysdate(), 8), (9, sysdate(), 9), (10, sysdate(), 10)")
        cur.execute("COMMIT")
        time.sleep(2)

    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_28_thdB(conn):
    global B_succ
    try:
        time.sleep(15)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("SELECT * FROM sz1 WHERE col_a <= 5 FOR SHARE")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT 1"
        time.sleep(6)
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT 2"
        cur.execute("SELECT * FROM sz1 WHERE col_a = 3")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT 3"
        cur.execute("SELECT * FROM sz1 WHERE col_a = 8")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT 4"
        cur.execute("SELECT * FROM sz1 WHERE col_a >= 3")
        results = cur.fetchall()
        assert len(results) == 3, "ASSERT 5"
        cur.execute("SELECT * FROM sz1 WHERE col_a <= 9")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT 6"

        cur.execute("SELECT * FROM sz1 WHERE col_c = 3")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT 7"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 8")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT 8"
        cur.execute("SELECT * FROM sz1 WHERE col_c >= 3")
        results = cur.fetchall()
        assert len(results) == 3, "ASSERT 9"
        cur.execute("SELECT * FROM sz1 WHERE col_c <= 9")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT 10"
        cur.execute("COMMIT")

        cur.execute("BEGIN")
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT 11"
        cur.execute("SELECT * FROM sz1 WHERE col_a = 3")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT 12"
        cur.execute("SELECT * FROM sz1 WHERE col_a = 8")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT 13"
        cur.execute("SELECT * FROM sz1 WHERE col_a >= 3")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT 14"
        cur.execute("SELECT * FROM sz1 WHERE col_a <= 9")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT 15"

        cur.execute("SELECT * FROM sz1 WHERE col_c = 3")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT 16"
        cur.execute("SELECT * FROM sz1 WHERE col_c = 8")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT 17"
        cur.execute("SELECT * FROM sz1 WHERE col_c >= 3")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT 18"
        cur.execute("SELECT * FROM sz1 WHERE col_c <= 9")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT 19"
        cur.execute("COMMIT")

    except Exception as e:
        print(f"Thd B failed: {e}")
        time.sleep(2)
        cur.close()
        return

    cur.close()
    B_succ = True

def case_28_post(A_conn, B_conn):
    try:
        cur = A_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS sz1")
    except Exception as e:
        print(f"POST Drop DB/TABLE failed: {e}")
        A_conn.close()
        B_conn.close()
        exit (-1)


def case_48_thdA(conn):
    # Notice: this case has unique index on col_c
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, sysdate(), 1), (2, sysdate(), 2), (3, sysdate(), 3), (4, sysdate(), 4), (5, sysdate(), 5), (6, sysdate(), 6), (7, sysdate(), 7), (8, sysdate(), 8), (9, sysdate(), 9), (10, sysdate(), 10)")
        cur.execute("COMMIT")
        time.sleep(2)
        cur.execute("BEGIN")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 10, "ASSERT 1"
        cur.execute("SELECT * FROM sz where col_a = 6")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT 2"
        cur.execute("SELECT * FROM sz where col_a >= 6")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT 3"
        # = condition here will use the unique index on col_c
        # which acquires lock on the row implicitly.
        cur.execute("SELECT * FROM sz where col_c = 8")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT 4"
        # <= condition here won't use the unique index on col_c...
        # so no lock acquires
        cur.execute("SELECT * FROM sz where col_c <= 8")
        results = cur.fetchall()
        assert len(results) == 8, "ASSERT 5"
        time.sleep(9)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        # Here is different with the case_28
        assert len(results) == 1, "ASSERT 6"
        cur.execute("COMMIT")

        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, sysdate(), 1), (2, sysdate(), 2), (3, sysdate(), 3), (4, sysdate(), 4), (5, sysdate(), 5), (6, sysdate(), 6), (7, sysdate(), 7), (8, sysdate(), 8), (9, sysdate(), 9), (10, sysdate(), 10)")
        cur.execute("COMMIT")
        time.sleep(2)

    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_48_thdB(conn):
    global B_succ
    try:
        time.sleep(15)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("SELECT * FROM sz WHERE col_a <= 5 FOR SHARE")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT 1"
        time.sleep(6)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT 2"
        cur.execute("SELECT * FROM sz WHERE col_a = 3")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT 3"
        cur.execute("SELECT * FROM sz WHERE col_a = 8")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT 4"
        cur.execute("SELECT * FROM sz WHERE col_a >= 3")
        results = cur.fetchall()
        assert len(results) == 3, "ASSERT 5"
        cur.execute("SELECT * FROM sz WHERE col_a <= 9")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT 6"

        cur.execute("SELECT * FROM sz WHERE col_c = 3")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT 7"
        cur.execute("SELECT * FROM sz WHERE col_c = 8")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT 8"
        cur.execute("SELECT * FROM sz WHERE col_c >= 3")
        results = cur.fetchall()
        assert len(results) == 3, "ASSERT 9"
        cur.execute("SELECT * FROM sz WHERE col_c <= 9")
        results = cur.fetchall()
        assert len(results) == 5, "ASSERT 10"
        cur.execute("COMMIT")

        cur.execute("BEGIN")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT 11"
        cur.execute("SELECT * FROM sz WHERE col_a = 3")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT 12"
        cur.execute("SELECT * FROM sz WHERE col_a = 8")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT 13"
        cur.execute("SELECT * FROM sz WHERE col_a >= 3")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT 14"
        cur.execute("SELECT * FROM sz WHERE col_a <= 9")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT 15"

        cur.execute("SELECT * FROM sz WHERE col_c = 3")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT 16"
        cur.execute("SELECT * FROM sz WHERE col_c = 8")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT 17"
        cur.execute("SELECT * FROM sz WHERE col_c >= 3")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT 18"
        cur.execute("SELECT * FROM sz WHERE col_c <= 9")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT 19"
        cur.execute("COMMIT")

    except Exception as e:
        print(f"Thd B failed: {e}")
        time.sleep(2)
        cur.close()
        return

    cur.close()
    B_succ = True


def case_29_pre(A_conn, B_conn):
    try:
        cur = A_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS sz1")
        cur.execute("CREATE TABLE test.sz1 ("
                    "col_a INT, "
                    "col_b TIMESTAMP, "
                    "col_c INT, "
                    "PRIMARY KEY(col_a), "
                    "KEY(col_c))"
                    "ENGINE = NDB, "
                    "COMMENT=\"NDB_TABLE=FULLY_REPLICATED=1,TTL=10@col_b\"")
    except Exception as e:
        print(f"PRE Create DB/TABLE failed: {e}")
        A_conn.close()
        B_conn.close()
        exit (-1)

def case_29_thdA(conn):
    global A_succ
    try:
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
        cur.execute("SELECT * FROM sz1 where col_c = 8")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM sz1 where col_c <= 8")
        results = cur.fetchall()
        assert len(results) == 8, "ASSERT"
        time.sleep(9)
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("COMMIT")

        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz1 VALUES(1, sysdate(), 1), (2, sysdate(), 2), (3, sysdate(), 3), (4, sysdate(), 4), (5, sysdate(), 5), (6, sysdate(), 6), (7, sysdate(), 7), (8, sysdate(), 8), (9, sysdate(), 9), (10, sysdate(), 10)")
        cur.execute("COMMIT")
        time.sleep(2)

    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_29_thdB(conn):
    global B_succ
    try:
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
        cur.execute("DROP TABLE sz1")

    except Exception as e:
        print(f"Thd B failed: {e}")
        time.sleep(2)
        cur.close()
        return

    cur.close()
    B_succ = True

def case_29_post(A_conn, B_conn):
    try:
        cur = A_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS sz1")
    except Exception as e:
        print(f"POST Drop DB/TABLE failed: {e}")
        A_conn.close()
        B_conn.close()
        exit (-1)


def case_49_pre(A_conn, B_conn):
    try:
        cur = A_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS sz1")
        cur.execute("CREATE TABLE test.sz1 ("
                    "col_a INT, "
                    "col_b TIMESTAMP, "
                    "col_c INT, "
                    "PRIMARY KEY(col_a), "
                    "UNIQUE KEY(col_c))"
                    "ENGINE = NDB, "
                    "COMMENT=\"NDB_TABLE=FULLY_REPLICATED=1,TTL=10@col_b\"")
    except Exception as e:
        print(f"PRE Create DB/TABLE failed: {e}")
        A_conn.close()
        B_conn.close()
        exit (-1)

def case_49_thdA(conn):
    # Notice: this case has unique index on col_c
    global A_succ
    try:
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

    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_49_thdB(conn):
    global B_succ
    try:
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

    except Exception as e:
        print(f"Thd B failed: {e}")
        time.sleep(2)
        cur.close()
        return

    cur.close()
    B_succ = True

def case_49_post(A_conn, B_conn):
    try:
        cur = A_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS sz1")

    except Exception as e:
        print(f"POST Drop DB/TABLE failed: {e}")
        A_conn.close()
        B_conn.close()
        exit (-1)


def case_45_thdA(conn):
    global A_succ
    try:
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


    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_45_thdB(conn):
    global B_succ
    try:
        time.sleep(5)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("SELECT * FROM sz FOR SHARE")
        #cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        time.sleep(5)
        cur.execute("SELECT * FROM sz FOR UPDATE")
        #cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        time.sleep(5)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"

    except Exception as e:
        print(f"Thd B failed: {e}")
        time.sleep(2)
        cur.close()
        return

    cur.close()
    B_succ = True


def case_30_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(5)
        cur.execute("COMMIT")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT"
        time.sleep(7)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_30_thdB(conn):
    global B_succ
    try:
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("REPLACE INTO sz VALUES(1, SYSDATE(), 200)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT"
        time.sleep(8)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return

    cur.close()
    B_succ = True


def case_31_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(11)
        cur.execute("COMMIT")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT"
        time.sleep(4)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_31_thdB(conn):
    global B_succ
    try:
        time.sleep(5)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("REPLACE INTO sz VALUES(1, SYSDATE(), 200)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT"
        time.sleep(5)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_32_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(13)
        cur.execute("COMMIT")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_32_thdB(conn):
    global B_succ
    try:
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("REPLACE INTO sz VALUES(1, SYSDATE(), 200)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_33_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(5)
        cur.execute("ROLLBACK")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT"
        time.sleep(7)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_33_thdB(conn):
    global B_succ
    try:
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("REPLACE INTO sz VALUES(1, SYSDATE(), 200)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT"
        time.sleep(8)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_34_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(11)
        cur.execute("ROLLBACK")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT"
        time.sleep(4)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_34_thdB(conn):
    global B_succ
    try:
        time.sleep(5)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("REPLACE INTO sz VALUES(1, SYSDATE(), 200)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            col_b = row[1]
            col_c = row[2]
            assert col_a == 1 and col_c == 200, "ASSERT"
        time.sleep(5)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_35_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        time.sleep(14)
        cur.execute("ROLLBACK")
        time.sleep(1)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_35_thdB(conn):
    global B_succ
    try:
        time.sleep(2)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("REPLACE INTO sz VALUES(1, SYSDATE(), 200)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_36_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sz VALUES(1, SYSDATE(), 100)")
        cur.execute("COMMIT")
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT 1"
        time.sleep(11)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT 2"
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_36_thdB(conn):
    global B_succ
    try:
        time.sleep(5)
        cur = conn.cursor()
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT 3"
        for row in results:
            col_a = row[0]
            assert col_a == 1, "ASSERT 4"
        time.sleep(6)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT 5"
        assert check_rep(conn) == True, "ASSERT 6"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_37_thdA(conn):
    global A_succ
    try:
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
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_37_thdB(conn):
    global B_succ
    try:
        time.sleep(5)
        cur = conn.cursor()
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        time.sleep(6)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        assert check_rep(conn) == True, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_38_thdA(conn):
    global A_succ
    try:
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
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_38_thdB(conn):
    global B_succ
    try:
        time.sleep(5)
        cur = conn.cursor()
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        time.sleep(6)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        assert check_rep(conn) == True, "ASSERT"

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
        assert check_rep(conn) == True, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_39_thdA(conn):
    global A_succ
    try:
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
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_39_thdB(conn):
    global B_succ
    try:
        time.sleep(4)
        cur = conn.cursor()
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT1"
        time.sleep(11)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT2"
        assert check_rep(conn) == True, "ASSERT3"

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
        assert check_rep(conn) == True, "ASSERT9"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_40_pre(A_conn, B_conn):
    try:
        cur = A_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS sz1")
        cur.execute("CREATE TABLE test.sz1 ("
                    "col_a INT, "
                    "col_b TIMESTAMP, "
                    "col_c INT)"
                    "ENGINE = NDB, "
                    "COMMENT=\"NDB_TABLE=FULLY_REPLICATED=1,TTL=10@col_b\"")
    except Exception as e:
        print(f"PRE Create DB/TABLE failed: {e}")
        A_conn.close()
        B_conn.close()
        exit (-1)

def case_40_thdA(conn):
    global A_succ
    try:
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
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_40_thdB(conn):
    global B_succ
    try:
        time.sleep(5)
        cur = conn.cursor()
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT1"
        time.sleep(11)
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT2"
        assert check_rep(conn) == True, "ASSERT3"

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
        assert check_rep(conn) == True, "ASSERT8"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True

def case_40_post(A_conn, B_conn):
    try:
        cur = A_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS sz1")
    except Exception as e:
        print(f"POST Drop DB/TABLE failed: {e}")
        A_conn.close()
        B_conn.close()
        exit (-1)


def case_41_thdA(conn):
    global A_succ
    try:
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
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_41_thdB(conn):
    global B_succ
    try:
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
        assert check_rep(conn) == True, "ASSERT"

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
        assert check_rep(conn) == True, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_42_pre(A_conn, B_conn):
    try:
        cur = A_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS sz1")
        cur.execute("CREATE TABLE test.sz1 ("
                    "col_a INT, "
                    "col_b TIMESTAMP, "
                    "col_c INT)"
                    "ENGINE = NDB, "
                    "COMMENT=\"NDB_TABLE=FULLY_REPLICATED=1,TTL=10@col_b\"")
    except Exception as e:
        print(f"PRE Create DB/TABLE failed: {e}")
        A_conn.close()
        B_conn.close()
        exit (-1)

def case_42_thdA(conn):
    global A_succ
    try:
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
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_42_thdB(conn):
    global B_succ
    try:
        time.sleep(8)
        cur = conn.cursor()
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 2, "ASSERT1"
        time.sleep(9)
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT2"
        assert check_rep(conn) == True, "ASSERT3"

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
        assert check_rep(conn) == True, "ASSERT7"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True

def case_42_post(A_conn, B_conn):
    try:
        cur = A_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS sz1")
    except Exception as e:
        print(f"POST Drop DB/TABLE failed: {e}")
        A_conn.close()
        B_conn.close()
        exit (-1)


def case_43_thdA(conn):
    global A_succ
    try:
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

    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_43_thdB(conn):
    global B_succ
    try:
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
        assert check_rep(conn) == True, "ASSERT"
        time.sleep(3)
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        assert check_rep(conn) == True, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True

def case_44_pre(A_conn, B_conn):
    return

def case_44_thdA(conn):
    global A_succ
    try:
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

    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_44_thdB(conn):
    global B_succ
    try:
        time.sleep(12)
        cur = conn.cursor()
        cur.execute("SELECT * FROM sz")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_c = row[2]
            assert col_c == 300, "ASSERT"
        assert check_rep(conn) == True, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True

def case_44_post(A_conn, B_conn):
    try:
        cur = A_conn.cursor()
        cur.execute("BEGIN")
        cur.execute("DELETE FROM sz")
        cur.execute("COMMIT")
    except Exception as e:
        print(f"POST Drop DB/TABLE failed: {e}")
        A_conn.close()
        B_conn.close()
        exit (-1)


def case_46_pre(A_conn, B_conn):
    try:
        B_cur = B_conn.cursor()
        B_cur.execute("STOP REPLICA")
        B_cur.execute("DROP DATABASE IF EXISTS test")

        cur = A_conn.cursor()
        cur.execute("DROP DATABASE IF EXISTS test")
        cur.execute("CREATE DATABASE test")
        cur.execute("USE test")
        cur.execute("CREATE TABLE test.sz1 ("
                    "col_a INT, "
                    "col_b TIMESTAMP, "
                    "col_c INT, "
                    "PRIMARY KEY(col_a), "
                    "UNIQUE KEY(col_c))"
                    "ENGINE = NDB, "
                    "COMMENT=\"NDB_TABLE=TTL=10@col_b\"")
        cur.execute("CREATE TABLE test.sz2 ("
                    "col_a INT, "
                    "col_b TIMESTAMP, "
                    "col_c INT, "
                    "KEY(col_c)) "
                    "ENGINE = NDB, "
                    "COMMENT=\"NDB_TABLE=TTL=10@col_b\"")
        cur.execute("CREATE TABLE test.sz3 ("
                    "col_a INT, "
                    "col_b TIMESTAMP, "
                    "col_c INT, "
                    "PRIMARY KEY(col_a), "
                    "KEY(col_c)) "
                    "ENGINE = NDB")
    except Exception as e:
        print(f"PRE Create DB/TABLE failed: {e}")
        A_conn.close()
        B_conn.close()
        exit (-1)

def case_46_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("USE test")
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
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 4, "ASSERT"
        cur.execute("SELECT * FROM sz2")
        results = cur.fetchall()
        assert len(results) == 4, "ASSERT"
        cur.execute("SELECT * FROM sz3")
        results = cur.fetchall()
        assert len(results) == 4, "ASSERT"
        time.sleep(11)

        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_c = row[2]
            assert col_c == 300, "ASSERT"
        cur.execute("SELECT * FROM sz2")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_c = row[2]
            assert col_c == 300, "ASSERT"
        cur.execute("SELECT * FROM sz3")
        results = cur.fetchall()
        assert len(results) == 4, "ASSERT"
        subprocess.run([BIN_DIR+"/ndb_mgm", "-e", "start backup"],
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_46_thdB(conn):
    global B_succ
    time.sleep(17)
    subprocess.run([BIN_DIR+"/ndb_restore",
                    "--ndb-connectstring=127.0.0.1:"+str(MGMD_PORT_R),
                    "--nodeid=2", "--backupid=1",
                    "--backup-path="+DATA_DIR_P_1+"/ndb_data/BACKUP/BACKUP-1",
                    "--restore_meta", "--no-restore-disk-objects"],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    subprocess.run([BIN_DIR+"/ndb_restore",
                    "--ndb-connectstring=127.0.0.1:"+str(MGMD_PORT_R),
                    "--nodeid=2", "--backupid=1",
                    "--backup-path="+DATA_DIR_P_1+"/ndb_data/BACKUP/BACKUP-1",
                    "--disable-indexes"],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    subprocess.run([BIN_DIR+"/ndb_restore",
                    "--ndb-connectstring=127.0.0.1:"+str(MGMD_PORT_R),
                    "--nodeid=2", "--backupid=1",
                    "--backup-path="+DATA_DIR_P_1+"/ndb_data/BACKUP/BACKUP-1",
                    "--restore-data"],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    subprocess.run([BIN_DIR+"/ndb_restore",
                    "--ndb-connectstring=127.0.0.1:"+str(MGMD_PORT_R),
                    "--nodeid=3", "--backupid=1",
                    "--backup-path="+DATA_DIR_P_2+"/ndb_data/BACKUP/BACKUP-1",
                    "--restore-data"],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    subprocess.run([BIN_DIR+"/ndb_restore",
                    "--ndb-connectstring=127.0.0.1:"+str(MGMD_PORT_R),
                    "--nodeid=2", "--backupid=1",
                    "--backup-path="+DATA_DIR_P_1+"/ndb_data/BACKUP/BACKUP-1",
                    "--rebuild-indexes"],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(10)
    while True:
        try:
            new_conn = pymysql.connect(host='127.0.0.1',
                                   port=MYSQLD_PORT_R,
                                   user='root')
            cur = new_conn.cursor()
            cur.execute("USE test")
            cur.execute("SELECT * FROM sz1")
            results = cur.fetchall()
            assert len(results) == 1, "ASSERT 1"
            for row in results:
                col_c = row[2]
                assert col_c == 300, "ASSERT 2"
            cur.execute("SELECT * FROM sz2")
            results = cur.fetchall()
            assert len(results) == 1, "ASSERT 3"
            for row in results:
                col_c = row[2]
                assert col_c == 300, "ASSERT 4"
            cur.execute("SELECT * FROM sz3")
            results = cur.fetchall()
            assert len(results) == 4, "ASSERT 5"
            break
        except pymysql.MySQLError as e:
            errno = e.args[0]
            if errno in [1412, 1049]:
                #1412 - Table definition has changed, please retry transaction
                #1049 - Unknown database 'test'
                print(f"Waiting for synchronizing restored schema...Retry {e}")
                time.sleep(10)
                cur.close()
                new_conn.close()
                continue
            else:
                print(f"Thd B failed: {e}")
                cur.close()
                new_conn.close()
                return
        except Exception as e:
            print(f"Thd B failed: {e}")
            cur.close()
            new_conn.close()
            return
    cur.close()
    new_conn.close()
    B_succ = True

def case_46_post(A_conn, B_conn):
    try:
        cur = A_conn.cursor()
        #cur.execute("DROP TABLE IF EXISTS sz1")
        #cur.execute("DROP TABLE IF EXISTS sz2")
        #cur.execute("DROP TABLE IF EXISTS sz3")
    except Exception as e:
        print(f"POST Drop DB/TABLE failed: {e}")
        A_conn.close()
        B_conn.close()
        exit (-1)


def case_47_thdA(conn):
    global A_succ
    try:
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

    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_47_thdB(conn):
    global B_succ
    try:
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
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        assert check_rep(conn) == True, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True


def case_50_pre(A_conn, B_conn):
    try:
        cur = A_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS sz1")
        cur.execute("CREATE TABLE test.sz1 ("
                    "col_a INT, "
                    "col_b TIMESTAMP, "
                    "col_c INT, "
                    "PRIMARY KEY(col_a))"
                    "ENGINE = NDB, "
                    "COMMENT=\"NDB_TABLE=TTL=10@col_b\"")
    except Exception as e:
        print(f"PRE Create DB/TABLE failed: {e}")
        A_conn.close()
        B_conn.close()
        exit (-1)

def case_50_thdA(conn):
    global A_succ
    try:
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

        cur.execute("ALTER TABLE sz1 ADD UNIQUE INDEX uk(col_c)");
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

        cur.execute("ALTER TABLE sz1 comment=\"NDB_TABLE=TTL=OFF\"");
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

    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_50_thdB(conn):
    global B_succ
    try:
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

        assert check_rep(conn) == True, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True

def case_50_post(A_conn, B_conn):
    try:
        cur = A_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS sz1")
    except Exception as e:
        print(f"POST Drop DB/TABLE failed: {e}")
        A_conn.close()
        B_conn.close()
        exit (-1)


def case_51_pre(A_conn, B_conn):
    # Precondition: ts_1 is pre created
    try:
        cur = A_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS sz1")
        cur.execute("CREATE TABLE test.sz1 ("
                    "col_a INT, "
                    "col_b TIMESTAMP, "
                    "col_c INT, "
                    "col_d INT STORAGE DISK, "
                    "PRIMARY KEY(col_a))"
                    "ENGINE = NDB, TABLESPACE ts_1, "
                    "COMMENT=\"NDB_TABLE=TTL=10@col_b\"")
    except Exception as e:
        print(f"PRE Create DB/TABLE failed: {e}")
        A_conn.close()
        B_conn.close()
        exit (-1)

def case_51_thdA(conn):
    global A_succ
    try:
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

        cur.execute("ALTER TABLE sz1 ADD UNIQUE INDEX uk(col_c)");
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

        cur.execute("ALTER TABLE sz1 comment=\"NDB_TABLE=TTL=OFF\"");
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

    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_51_thdB(conn):
    global B_succ
    try:
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

        assert check_rep(conn) == True, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True

def case_51_post(A_conn, B_conn):
    try:
        cur = A_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS sz1")
    except Exception as e:
        print(f"POST Drop DB/TABLE failed: {e}")
        A_conn.close()
        B_conn.close()
        exit (-1)


def case_52_pre(A_conn, B_conn):
    # Precondition: ts_1 is pre created
    try:
        cur = A_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS sz1")
        cur.execute("CREATE TABLE test.sz1 ("
                    "col_a INT, "
                    "col_b TIMESTAMP, "
                    "col_c INT, "
                    "col_d INT STORAGE DISK, "
                    "PRIMARY KEY(col_a))"
                    "ENGINE = NDB, TABLESPACE ts_1, "
                    "COMMENT=\"NDB_TABLE=TTL=10@col_b\"")
    except Exception as e:
        print(f"PRE Create DB/TABLE failed: {e}")
        A_conn.close()
        B_conn.close()
        exit (-1)

def case_52_thdA(conn):
    global A_succ
    try:
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

        cur.execute("ALTER TABLE sz1 CHANGE COLUMN col_b col_b INT STORAGE DISK");
    except Exception as e:
        if e.args[0] != 1478:
            print(f"Thd A failed: {e}")
            cur.close()
            return
    try:
        cur.execute("ALTER TABLE sz1 STORAGE DISK");
    except Exception as e:
        if e.args[0] != 1478:
            print(f"Thd A failed: {e}")
            cur.close()
            return

    cur.close()
    A_succ = True

def case_52_thdB(conn):
    global B_succ
    try:
        time.sleep(11)
        cur = conn.cursor()
        cur.execute("SELECT * FROM sz1")
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"

        assert check_rep(conn) == True, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True

def case_52_post(A_conn, B_conn):
    try:
        cur = A_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS sz1")
    except Exception as e:
        print(f"POST Drop DB/TABLE failed: {e}")
        A_conn.close()
        B_conn.close()
        exit (-1)


def case_53_pre(A_conn, B_conn):
    try:
        cur = A_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS sz1")
        cur.execute("CREATE TABLE test.sz1 ("
                    "col_a INT, "
                    "col_b TIMESTAMP, "
                    "col_c INT, "
                    "PRIMARY KEY(col_a))"
                    "ENGINE = NDB, "
                    "COMMENT=\"NDB_TABLE=TTL=10@col_b\"")
    except Exception as e:
        print(f"PRE Create DB/TABLE failed: {e}")
        A_conn.close()
        B_conn.close()
        exit (-1)

def case_53_thdA(conn):
    global A_succ
    try:
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

        cur.execute("ALTER TABLE sz1 ADD INDEX uk(col_c)");
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

        cur.execute("ALTER TABLE sz1 comment=\"NDB_TABLE=TTL=OFF\"");
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

    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_53_thdB(conn):
    global B_succ
    try:
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

        assert check_rep(conn) == True, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True

def case_53_post(A_conn, B_conn):
    try:
        cur = A_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS sz1")
    except Exception as e:
        print(f"POST Drop DB/TABLE failed: {e}")
        A_conn.close()
        B_conn.close()
        exit (-1)


def case_54_pre(A_conn, B_conn):
    try:
        cur = A_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS parent")
        cur.execute("DROP TABLE IF EXISTS sz1")
        cur.execute("DROP TABLE IF EXISTS child")
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
    except Exception as e:
        print(f"PRE Create DB/TABLE failed: {e}")
        A_conn.close()
        B_conn.close()
        exit (-1)

def case_54_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        exit
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

        cur.execute("SELECT * FROM parent WHERE col_c_ui_upd = 222");
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_c_ui_upd = 222");
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            assert col_a == 2, "ASSERT"

        cur.execute("SELECT * FROM sz1 WHERE col_d_ui_del = 33");
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM child WHERE col_g_ui_del = 36");
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"


        cur.execute("BEGIN")
        cur.execute("UPDATE sz1 SET col_f_ui_upd = 445 WHERE col_f_ui_upd = 45")
        cur.execute("DELETE FROM sz1 WHERE col_g_ui_del = 56")
        cur.execute("COMMIT")

        cur.execute("BEGIN")
        cur.execute("SELECT * FROM sz1 WHERE col_f_ui_upd = 445");
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("SELECT * FROM child WHERE col_f_ui_upd = 445");
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        for row in results:
            col_a = row[0]
            assert col_a == 4, "ASSERT"
        cur.execute("COMMIT")

        cur.execute("BEGIN")
        cur.execute("SELECT * FROM sz1 WHERE col_g_ui_del = 56");
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM child WHERE col_g_ui_del = 56");
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
        cur.execute("SELECT * FROM sz1 WHERE col_c_ui_upd = 112");
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"
        cur.execute("SELECT * FROM sz1 WHERE col_d_ui_del = 63");
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT"

        cur.execute("SELECT * FROM child WHERE col_g_ui_del = 66");
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT"
        cur.execute("COMMIT")

        cur.execute("BEGIN")
        # crash
        cur.execute("INSERT INTO sz1 VALUES(6, SYSDATE(), 61, 62, 63, 664, 65, 66)")
        cur.execute("COMMIT")


    except Exception as e:
        print(f"Thd A failed: {e}")
        cur.close()
        return
    cur.close()
    A_succ = True

def case_54_thdB(conn):
    global B_succ
    try:
        time.sleep(15)
        cur = conn.cursor()

        assert check_rep(conn) == True, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True

def case_54_post(A_conn, B_conn):
    try:
        cur = A_conn.cursor()
        #cur.execute("DROP TABLE IF EXISTS sz1")
    except Exception as e:
        print(f"POST Drop DB/TABLE failed: {e}")
        A_conn.close()
        B_conn.close()
        exit (-1)


def case_55_pre(A_conn, B_conn):
    try:
        cur = A_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS parent")
        cur.execute("DROP TABLE IF EXISTS sz1")
        cur.execute("DROP TABLE IF EXISTS child")
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

        # should return error
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
        assert 0
    except Exception as e:
        if e.args[0] != 1215:
            print(f"PRE Create DB/TABLE failed: {e}")
            A_conn.close()
            B_conn.close()
            exit (-1)

def case_55_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        exit
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

        cur.execute("SELECT * FROM parent WHERE col_c_ui_upd = 222");
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT 4"
        cur.execute("SELECT * FROM sz1 WHERE col_c_ui_upd = 222");
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT 5"
        for row in results:
            col_a = row[0]
            assert col_a == 2, "ASSERT"

        cur.execute("SELECT * FROM sz1 WHERE col_d_ui_del = 33");
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT 6"


        cur.execute("BEGIN")
        cur.execute("UPDATE sz1 SET col_f_ui_upd = 445 WHERE col_f_ui_upd = 45")
        cur.execute("DELETE FROM sz1 WHERE col_g_ui_del = 56")
        cur.execute("COMMIT")

        cur.execute("BEGIN")
        cur.execute("SELECT * FROM sz1 WHERE col_f_ui_upd = 445");
        results = cur.fetchall()
        assert len(results) == 1, "ASSERT 7"
        for row in results:
            col_a = row[0]
            assert col_a == 4, "ASSERT 8"
        cur.execute("COMMIT")

        cur.execute("BEGIN")
        cur.execute("SELECT * FROM sz1 WHERE col_g_ui_del = 56");
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
        cur.execute("SELECT * FROM sz1 WHERE col_c_ui_upd = 112");
        results = cur.fetchall()
        assert len(results) == 0, "ASSERT 13"

        cur.execute("BEGIN")
        cur.execute("DELETE FROM parent WHERE col_d_ui_del = 63")
        cur.execute("COMMIT")

        cur.execute("BEGIN")
        cur.execute("SET ttl_expired_rows_visible_in_delete = 1")
        cur.execute("DELETE FROM sz1");
        changed_rows = conn.affected_rows()
        matched_rows = cur.rowcount
        print(f"changed: {changed_rows}, matched: {matched_rows}")
        assert changed_rows == 4 and matched_rows == 4, "ASSERT"
        cur.execute("COMMIT")
        cur.execute("SET ttl_expired_rows_visible_in_delete = 0")

        # If a row in the TTL child table expires, the parent table cannot update or delete
        # the related row until the purging thread truly deletes it from the child table.
        #
        # The reason is that the scan flag used by the foreign key always includes
        # the "ignore TTL" flag in SUMA.
        # add some cases here

        cur.execute("BEGIN")
        cur.execute("DELETE FROM other");
        cur.execute("COMMIT")
        cur.execute("ALTER TABLE test.other "
                    "ADD FOREIGN KEY(col_e_ui) REFERENCES sz1(col_e_ui) ON UPDATE CASCADE ON DELETE CASCADE, "
                    "ADD FOREIGN KEY(col_f_ui_upd) REFERENCES sz1(col_f_ui_upd) ON UPDATE CASCADE ON DELETE CASCADE, "
                    "ADD FOREIGN KEY(col_g_ui_del) REFERENCES sz1(col_g_ui_del) ON UPDATE CASCADE ON DELETE CASCADE")
        assert 0

    except Exception as e:
        if e.args[0] != 1215:
            print(f"Thd A failed: {e}")
            cur.close()
            return
    cur.close()
    A_succ = True

def case_55_thdB(conn):
    global B_succ
    try:
        time.sleep(15)
        cur = conn.cursor()

        assert check_rep(conn) == True, "ASSERT"
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True

def case_55_post(A_conn, B_conn):
    try:
        cur = A_conn.cursor()
        #cur.execute("DROP TABLE IF EXISTS sz1")
    except Exception as e:
        print(f"POST Drop DB/TABLE failed: {e}")
        A_conn.close()
        B_conn.close()
        exit (-1)


def case_56_pre(A_conn, B_conn):
    try:
        cur = A_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS parent")
        cur.execute("DROP TABLE IF EXISTS parentttl")
        cur.execute("DROP TABLE IF EXISTS child")
        cur.execute("DROP TABLE IF EXISTS sz1")
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

        # should return error
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
        assert 0
    except Exception as e:
        if e.args[0] != 1215:
            print(f"PRE Create DB/TABLE failed: {e}")
            A_conn.close()
            B_conn.close()
            exit (-1)

def case_56_thdA(conn):
    global A_succ
    try:
        cur = conn.cursor()
        cur.execute("ALTER TABLE sz1 "
                    "ADD FOREIGN KEY(col_b_ui) REFERENCES parentttl(col_b_ui) ON UPDATE CASCADE ON DELETE CASCADE, "
                    "ADD FOREIGN KEY(col_c_ui_upd) REFERENCES parentttl(col_c_ui_upd) ON UPDATE CASCADE ON DELETE CASCADE, "
                    "ADD FOREIGN KEY(col_d_ui_del) REFERENCES parentttl(col_d_ui_del) ON UPDATE CASCADE ON DELETE CASCADE")
        assert 0
    except Exception as e:
        if e.args[0] != 1215:
            print(f"Thd A failed: {e}")
            cur.close()
            return

    try:
        cur.execute("ALTER TABLE parent COMMENT=\"NDB_TABLE=TTL=10@col_t\"")
        assert 0
    except Exception as e:
        if e.args[0] != 1846:
            print(f"Thd A failed: {e}")
            cur.close()
            return

    try:
        cur.execute("ALTER TABLE sz2 COMMENT=\"NDB_TABLE=TTL=10@col_t\"")
        assert 0
    except Exception as e:
        if e.args[0] != 1846:
            print(f"Thd A failed: {e}")
            cur.close()
            return

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
        assert 0
    except Exception as e:
        if e.args[0] != 1215:
            print(f"Thd A failed: {e}")
            cur.close()
            return

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
        assert 0
    except Exception as e:
        if e.args[0] != 1215:
            print(f"Thd A failed: {e}")
            cur.close()

    cur.close()
    A_succ = True

def case_56_thdB(conn):
    global B_succ
    try:
        cur = conn.cursor()
    except Exception as e:
        print(f"Thd B failed: {e}")
        cur.close()
        return
    cur.close()
    B_succ = True

def case_56_post(A_conn, B_conn):
    try:
        cur = A_conn.cursor()
        cur.execute("DROP TABLE IF EXISTS sz1")
        cur.execute("DROP TABLE IF EXISTS sz2")
        cur.execute("DROP TABLE IF EXISTS child")
        cur.execute("DROP TABLE IF EXISTS parent")
        cur.execute("DROP TABLE IF EXISTS parentttl")
    except Exception as e:
        print(f"POST Drop DB/TABLE failed: {e}")
        A_conn.close()
        B_conn.close()
        exit (-1)


def check_rep(conn):
    ret = False;
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


def case(num):
    global A_succ, B_succ
    global funcs_thdA, funcs_thdB
    A_succ = False
    B_succ = False

    cases_require_debug_sync = [23, 24, 25]
    cases_binlog = [36, 37, 38, 39, 40, 41, 42, 43, 44, 47, 50, 51, 52, 53, 54, 55]
    cases_backup_restore = [46]
    cases_need_extra_process = [28, 29, 40, 42, 44, 46, 49, 50, 51, 52, 53, 54, 55, 56]


    # 1. Connect
    try:
        A_conn = pymysql.connect(host='127.0.0.1',
                               port=MYSQLD_PORT_P,
                               database='test',
                               user='root')
    except Exception as e:
        print(f"Thd A connects to DB failed: {e}")
        exit(-1)
    A_succ = True
    try:
        if num in cases_binlog:
            B_conn = pymysql.connect(host='127.0.0.1',
                                   port=MYSQLD_PORT_R,
                                   database='test',
                                   user='root')
        elif num in cases_backup_restore:
            B_conn = pymysql.connect(host='127.0.0.1',
                                   port=MYSQLD_PORT_R,
                                   user='root')
        else:
            B_conn = pymysql.connect(host='127.0.0.1',
                                   port=MYSQLD_PORT_P,
                                   database='test',
                                   user='root')
    except Exception as e:
        print(f"Thd B connects to DB failed: {e}")
        A_conn.close()
        exit(-1)
    B_succ = True

    # 2. Verify DEBUG_SYNC for some cases
    if num in cases_require_debug_sync:
        try:
            cur = A_conn.cursor();
            cur.execute("SHOW VARIABLES LIKE 'DEBUG_SYNC'")
            results = cur.fetchall()
            # print(f"debug_sync: {results[0][0]}, {results[0][1]}")
            if results[0][1].find("ON") == -1:
                print(f"case {num} require MySQL DEBUG_SYNC, turn it on and retry...")
                cur.close()
                A_conn.close()
                B_conn.close()
                exit(-1)

            cur = B_conn.cursor();
            cur.execute("SHOW VARIABLES LIKE 'DEBUG_SYNC'")
            results = cur.fetchall()
            if results[0][1].find("ON") == -1:
                print(f"case {num} require MySQL DEBUG_SYNC, turn it on and retry...")
                cur.close()
                A_conn.close()
                B_conn.close()
                exit(-1)
        except Exception as e:
            print(f"TRY DEBUG_SYNC failed: {e}")
            cur.close()
            A_conn.close()
            B_conn.close()
            exit(-1)
    
    # 3. Call preprocess() if needed
    if num in cases_need_extra_process:
        pre_func_name = f"case_{num}_pre"
        globals()[pre_func_name](A_conn, B_conn)

    # 4. Test
    if (A_succ and B_succ):
        A_succ = False
        B_succ = False
        A_thd = threading.Thread(target=funcs_thdA[num - 1], args=(A_conn,))
        B_thd = threading.Thread(target=funcs_thdB[num - 1], args=(B_conn,))

        A_thd.start()
        B_thd.start()

        A_thd.join()
        B_thd.join()
        if (A_succ and B_succ):
            print(f"case_{num} passed")
        else:
            print(f"case_{num} failed")
        # 5. Call postprocess() if needed
        if num in cases_need_extra_process:
            post_func_name = f"case_{num}_post"
            globals()[post_func_name](A_conn, B_conn)

        A_conn.close()
        B_conn.close()
    else:
        print(f"case_{num} failed")

funcs_thdA = []
funcs_thdB = []
A_succ = False
B_succ = False
if __name__ == '__main__':
    if os.path.exists(config_path):
        spec = importlib.util.spec_from_file_location("my_config", config_path)
        my_config = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(my_config)
        BIN_DIR = getattr(my_config, 'BIN_DIR', BIN_DIR)
        MYSQLD_PORT_P = getattr(my_config, 'MYSQLD_PORT_P', MYSQLD_PORT_P)
        DATA_DIR_P_1 = getattr(my_config, 'DATA_DIR_P_1', DATA_DIR_P_1)
        DATA_DIR_P_2 = getattr(my_config, 'DATA_DIR_P_2', DATA_DIR_P_2)
        MYSQLD_PORT_R = getattr(my_config, 'MYSQLD_PORT_R', MYSQLD_PORT_R)
        MGMD_PORT_R = getattr(my_config, 'MGMD_PORT_R', MGMD_PORT_R)
        MYSQLD_PORT_R = getattr(my_config, 'MYSQLD_PORT_R', MYSQLD_PORT_R)

    case_num = 56
    # 1. create database and table
    try:
        conn = pymysql.connect(host='127.0.0.1',
                               port=MYSQLD_PORT_P,
                               user='root')
    except Exception as e:
        print(f"Connect to DB failed: {e}")
        exit (-1)
    try:
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
    except Exception as e:
        print(f"Create DB/TABLE failed: {e}")
        conn.close()
        exit (-1)
    conn.close()
    time.sleep(1)

    for i in range (1, case_num + 1):
        func_name = f"case_{i}_thdA"
        funcs_thdA.append(eval(func_name))
        func_name = f"case_{i}_thdB"
        funcs_thdB.append(eval(func_name))

    #INSERT
    case(1)
    case(2)
    case(3)
    case(4)
    case(5)

    #INSERT ON DUPLICATE KEY UPDATE
    case(6)
    case(7)
    case(8)
    case(9)
    case(10)
    case(11)
    case(12)

    #UPDATE
    case(13)
    case(14)
    case(15)
    case(16)
    case(17)
    case(18)

    #DELETE
    case(19)
    case(20)
    case(21)
    case(22)
    case(23)
    case(27)

    case(23)
    case(25)
    #case(26)
    case(24)

    #READ LOCKED
    case(28)
    case(48) # UNIQUE INDEX
    case(29) # FULLY_REPLICATED
    #case(49) # FULLY_REPLICATED, UNIQUE INDEX 
              # NOTICE seems unique index can not work with binlog replication,
              # which will cause the replica mysql Stuck while applying the DROP TABLE operation
    #case(45)

    #REPLACE INTO
    case(30)
    case(31)
    case(32)
    case(33)
    case(34)
    case(35)

    #BINLOG
    case(36)
    case(37)
    case(38)
    case(39)
    case(40)
    case(41)
    case(42)
    case(43)
    case(44)
    case(45)

    #Unique index and trigger
    case(47)
    case(50)
    case(53)

    #Disk column
    case(51)
    case(52)

    #Foreign key
    #case(54) #Can only run this case after supporting use a TTL table as the parent table
    #case(55) #Can only run this case after supporting use a TTL table as the child table
    case(56)

    #BACKUP RESTORE
    case(46)
