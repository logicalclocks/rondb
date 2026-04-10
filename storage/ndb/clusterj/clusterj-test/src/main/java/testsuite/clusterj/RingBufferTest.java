/*
   Copyright (c) 2026, Hopsworks and/or its affiliates.

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

package testsuite.clusterj;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.Constants;
import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.Transaction;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.PredicateOperand;

import testsuite.clusterj.model.RingBufferNotNull;
import testsuite.clusterj.model.RingBufferSensor;

/**
 * Tests for ring buffer table support in ClusterJ.
 *
 * Table: ring_buffer_sensor with MAX_ROWS_PER_PK=5
 *   PK: (sensor_id, ring_idx)
 *   ring_idx and ring_meta are system-managed
 *
 * Tests verify that makePersistent transparently handles the ring buffer
 * protocol: reading meta row, computing ring_idx, writing data rows,
 * and updating the meta row.
 */
public class RingBufferTest extends AbstractClusterJTest {

    private static final int RING_SIZE = 5;
    private static final int SESSION_CACHE_SIZE = 10;

    @Override
    protected Properties modifyProperties() {
        Properties modifiedProps = new Properties();
        modifiedProps.putAll(props);
        modifiedProps.put(Constants.PROPERTY_CLUSTER_MAX_CACHED_INSTANCES,
                Integer.toString(SESSION_CACHE_SIZE));
        modifiedProps.put(Constants.PROPERTY_CLUSTER_WARMUP_CACHED_SESSIONS,
                Integer.toString(SESSION_CACHE_SIZE));
        modifiedProps.put(Constants.PROPERTY_CLUSTER_MAX_CACHED_SESSIONS,
                Integer.toString(SESSION_CACHE_SIZE));
        return modifiedProps;
    }

    /**
     * Override loadSchema to create only the ring_buffer_sensor table,
     * avoiding the full schema.sql which may exceed MaxNoOfTables.
     */
    @Override
    protected void loadSchema() {
        initializeJDBC();
        try {
            getConnection();
            Statement stmt = connection.createStatement();
            stmt.execute("CREATE TABLE IF NOT EXISTS t_basic ("
                    + "id INT NOT NULL PRIMARY KEY,"
                    + "name VARCHAR(32),"
                    + "age INT,"
                    + "magic INT NOT NULL"
                    + ") ENGINE=ndbcluster");
            stmt.execute("DROP TABLE IF EXISTS ring_buffer_sensor");
            stmt.execute("CREATE TABLE ring_buffer_sensor ("
                    + "sensor_id INT NOT NULL,"
                    + "ring_idx INT NOT NULL DEFAULT 0,"
                    + "ring_meta VARBINARY(64),"
                    + "timestamp_val BIGINT,"
                    + "sensor_value DOUBLE,"
                    + "PRIMARY KEY (sensor_id, ring_idx)"
                    + ") ENGINE=ndbcluster"
                    + " COMMENT='NDB_TABLE=MAX_ROWS_PER_PK=5@ring_idx@ring_meta'");
            stmt.execute("DROP TABLE IF EXISTS ring_buffer_notnull");
            stmt.execute("CREATE TABLE ring_buffer_notnull ("
                    + "client_id INT NOT NULL,"
                    + "ring_idx INT NOT NULL DEFAULT 0,"
                    + "ring_meta VARBINARY(64),"
                    + "name VARCHAR(50) NOT NULL,"
                    + "score INT NOT NULL,"
                    + "PRIMARY KEY (client_id, ring_idx)"
                    + ") ENGINE=ndbcluster"
                    + " COMMENT='NDB_TABLE=MAX_ROWS_PER_PK=3@ring_idx@ring_meta'");
            stmt.close();
        } catch (Exception ex) {
            throw new RuntimeException("Failed to create ring_buffer_sensor table", ex);
        }
    }

    @Override
    public void localSetUp() {
        createSessionFactory();
        session = sessionFactory.getSession();
        tx = session.currentTransaction();
        // Ring buffer tables require OO_RING_BUFFER_OP for deletes via NDB API.
        // Use SQL DELETE which goes through the MySQL handler.
        cleanupViaSql();
    }

    public void test() {
        // Core ring buffer operations
        testSingleInsert();
        testMetaRowSqlCheck();
        testFillRing();
        testMultiplePrefixes();
        testBatchInsert();
        testCrossTransactionContinuity();

        // P0: Production-critical gaps
        testBatchExceedingRingSize();
        testMixedPrefixesSingleTransaction();
        testInterleavedPrefixesSingleTransaction();
        testTransactionRollback();
        testDeleteViaClusterJ();
        testUpdateViaClusterJ();

        // P1: Correctness verification
        testExactRingFill();
        testMultipleWrapCycles();
        testNullUserColumns();
        testNotNullColumns();
        testFindNonExistentSlot();
        testMetaRowVisibility();
        testQueryScan();

        // P2: Session/cache behavior
        testSessionReuseAcrossTransactions();
        testEmptyTransaction();
        testBatchExceedingRingSizeMultipleWraps();
        testSessionCacheInsertContinuity();
        testSessionCacheBatchInsert();
        testSessionCacheMultiplePrefixes();
        testSessionCacheWrapAround();
        testWithoutSessionCache();

        // P3: DTO cache behavior
        testDtoCacheInsert();
        testDtoCacheBatchInsert();
        testDtoCacheReadAfterInsert();
        testDtoCacheWithoutCache();

        // Concurrent tests (SamePrefix runs last — its normalized state
        // is checked by the SQL diagnostic queries in the .test file)
        testConcurrentDifferentPrefixes();
        testConcurrentWithSessionCache();
        testConcurrentSamePrefix();

        failOnError();
    }

    /** Check if the meta row for sensorId=100 exists via SQL with show_meta=1. */
    private void testMetaRowSqlCheck() {
        try {
            getConnection();
            Statement stmt = connection.createStatement();
            stmt.execute("SET ndb_ring_buffer_show_meta = 1");
            ResultSet rs = stmt.executeQuery(
                    "SELECT sensor_id, ring_idx, timestamp_val"
                    + " FROM ring_buffer_sensor WHERE sensor_id = 100"
                    + " ORDER BY ring_idx");
            int totalRows = 0;
            boolean metaRowFound = false;
            while (rs.next()) {
                int rid = rs.getInt("ring_idx");
                System.out.println("[META-CHECK] sensor_id=" + rs.getInt("sensor_id")
                        + " ring_idx=" + rid
                        + " ts=" + rs.getLong("timestamp_val"));
                totalRows++;
                if (rid == 0) metaRowFound = true;
            }
            rs.close();
            stmt.execute("SET ndb_ring_buffer_show_meta = 0");
            stmt.close();
            System.out.println("[META-CHECK] totalRows=" + totalRows
                    + " metaFound=" + metaRowFound);
            errorIfNotEqual("Meta row SQL check: total rows with meta",
                    2, totalRows);
            errorIfNotEqual("Meta row SQL check: meta row found",
                    true, metaRowFound);
        } catch (Exception ex) {
            error("Meta row SQL check failed: " + ex.getMessage());
        }
    }

    // =======================================================================
    // Existing core tests
    // =======================================================================

    /**
     * Test 1: Insert a single row into an empty ring.
     * Verify it lands at ring_idx=1.
     */
    private void testSingleInsert() {
        cleanup();
        tx.begin();
        RingBufferSensor row = session.newInstance(RingBufferSensor.class);
        row.setSensorId(100);
        row.setTimestampVal(1000L);
        row.setSensorValue(23.5);
        session.makePersistent(row);
        tx.commit();

        // Read back at ring_idx=1
        tx.begin();
        RingBufferSensor result = session.find(RingBufferSensor.class,
                new Object[]{100, 1});
        errorIfNotEqual("Single insert: sensor_id", 100, result.getSensorId());
        errorIfNotEqual("Single insert: ring_idx", 1, result.getRingIdx());
        errorIfNotEqual("Single insert: timestamp_val", 1000L, result.getTimestampVal());
        errorIfNotEqual("Single insert: sensor_value", 23.5, result.getSensorValue());
        tx.commit();
    }

    /**
     * Test 2: Insert 7 rows for ring_size=5.
     * After wrap-around, the oldest 2 rows should be overwritten.
     * Slots 1..5 should contain rows 3..7 (0-indexed: 2..6).
     */
    private void testFillRing() {
        cleanup();
        for (int i = 0; i < 7; i++) {
            tx.begin();
            RingBufferSensor row = session.newInstance(RingBufferSensor.class);
            row.setSensorId(200);
            row.setTimestampVal(2000L + i);
            row.setSensorValue(i * 1.0);
            session.makePersistent(row);
            tx.commit();
        }

        // After 7 inserts with ring_size=5:
        // Rows 0-4 fill slots 1-5. Row 5 wraps to slot 1. Row 6 wraps to slot 2.
        // Slot 1: insert 5 (ts=2005, val=5.0), Slot 2: insert 6 (ts=2006, val=6.0),
        // Slot 3: insert 2 (ts=2002, val=2.0), Slot 4: insert 3 (ts=2003, val=3.0),
        // Slot 5: insert 4 (ts=2004, val=4.0)
        tx.begin();
        RingBufferSensor fr1 = session.find(RingBufferSensor.class, new Object[]{200, 1});
        errorIfNotEqual("Fill ring: slot 1 timestamp", 2005L, fr1.getTimestampVal());
        errorIfNotEqual("Fill ring: slot 1 sensor_value", 5.0, fr1.getSensorValue());
        RingBufferSensor fr2 = session.find(RingBufferSensor.class, new Object[]{200, 2});
        errorIfNotEqual("Fill ring: slot 2 timestamp", 2006L, fr2.getTimestampVal());
        errorIfNotEqual("Fill ring: slot 2 sensor_value", 6.0, fr2.getSensorValue());
        RingBufferSensor fr3 = session.find(RingBufferSensor.class, new Object[]{200, 3});
        errorIfNotEqual("Fill ring: slot 3 timestamp", 2002L, fr3.getTimestampVal());
        errorIfNotEqual("Fill ring: slot 3 sensor_value", 2.0, fr3.getSensorValue());
        RingBufferSensor fr4 = session.find(RingBufferSensor.class, new Object[]{200, 4});
        errorIfNotEqual("Fill ring: slot 4 timestamp", 2003L, fr4.getTimestampVal());
        errorIfNotEqual("Fill ring: slot 4 sensor_value", 3.0, fr4.getSensorValue());
        RingBufferSensor fr5 = session.find(RingBufferSensor.class, new Object[]{200, 5});
        errorIfNotEqual("Fill ring: slot 5 timestamp", 2004L, fr5.getTimestampVal());
        errorIfNotEqual("Fill ring: slot 5 sensor_value", 4.0, fr5.getSensorValue());
        tx.commit();
    }

    /**
     * Test 3: Insert rows for two different sensor_ids.
     * Each should have an independent ring.
     */
    private void testMultiplePrefixes() {
        cleanup();
        // Insert 3 rows for sensor 301
        for (int i = 0; i < 3; i++) {
            tx.begin();
            RingBufferSensor row = session.newInstance(RingBufferSensor.class);
            row.setSensorId(301);
            row.setTimestampVal(3000L + i);
            row.setSensorValue(i);
            session.makePersistent(row);
            tx.commit();
        }
        // Insert 2 rows for sensor 302
        for (int i = 0; i < 2; i++) {
            tx.begin();
            RingBufferSensor row = session.newInstance(RingBufferSensor.class);
            row.setSensorId(302);
            row.setTimestampVal(4000L + i);
            row.setSensorValue(i + 10.0);
            session.makePersistent(row);
            tx.commit();
        }

        // Verify sensor 301 has 3 rows at slots 1-3
        tx.begin();
        for (int slot = 1; slot <= 3; slot++) {
            RingBufferSensor r = session.find(RingBufferSensor.class, new Object[]{301, slot});
            errorIfNotEqual("Multi-prefix 301 slot " + slot + " not null", true, r != null);
            errorIfNotEqual("Multi-prefix 301 slot " + slot + " timestamp",
                    3000L + (slot - 1), r.getTimestampVal());
            errorIfNotEqual("Multi-prefix 301 slot " + slot + " sensor_value",
                    (double)(slot - 1), r.getSensorValue());
        }
        // Verify sensor 302 has 2 rows at slots 1-2
        for (int slot = 1; slot <= 2; slot++) {
            RingBufferSensor r = session.find(RingBufferSensor.class, new Object[]{302, slot});
            errorIfNotEqual("Multi-prefix 302 slot " + slot + " not null", true, r != null);
            errorIfNotEqual("Multi-prefix 302 slot " + slot + " timestamp",
                    4000L + (slot - 1), r.getTimestampVal());
            errorIfNotEqual("Multi-prefix 302 slot " + slot + " sensor_value",
                    (slot - 1) + 10.0, r.getSensorValue());
        }
        tx.commit();
    }

    /**
     * Test 4: Insert 3 rows with same sensor_id in a single transaction.
     * The ring buffer writer should batch them.
     */
    private void testBatchInsert() {
        cleanup();
        tx.begin();
        for (int i = 0; i < 3; i++) {
            RingBufferSensor row = session.newInstance(RingBufferSensor.class);
            row.setSensorId(400);
            row.setTimestampVal(5000L + i);
            row.setSensorValue(i * 2.0);
            session.makePersistent(row);
        }
        tx.commit();

        // Verify 3 rows at slots 1-3
        tx.begin();
        for (int slot = 1; slot <= 3; slot++) {
            RingBufferSensor r = session.find(RingBufferSensor.class, new Object[]{400, slot});
            errorIfNotEqual("Batch insert slot " + slot + " not null", true, r != null);
            errorIfNotEqual("Batch insert slot " + slot + " timestamp",
                    5000L + (slot - 1), r.getTimestampVal());
            errorIfNotEqual("Batch insert slot " + slot + " sensor_value",
                    (slot - 1) * 2.0, r.getSensorValue());
        }
        tx.commit();
    }

    /**
     * Test 5: Insert 3 rows, commit, start new transaction, insert 3 more.
     * Verify ring state continues from where it left off.
     */
    private void testCrossTransactionContinuity() {
        cleanup();
        // Transaction 1: insert 3 rows
        tx.begin();
        for (int i = 0; i < 3; i++) {
            RingBufferSensor row = session.newInstance(RingBufferSensor.class);
            row.setSensorId(500);
            row.setTimestampVal(6000L + i);
            row.setSensorValue(i);
            session.makePersistent(row);
        }
        tx.commit();

        // Transaction 2: insert 3 more rows
        tx.begin();
        for (int i = 0; i < 3; i++) {
            RingBufferSensor row = session.newInstance(RingBufferSensor.class);
            row.setSensorId(500);
            row.setTimestampVal(7000L + i);
            row.setSensorValue(i + 10.0);
            session.makePersistent(row);
        }
        tx.commit();

        // After 6 inserts with ring_size=5:
        // Slots 1-5 filled by first 5 inserts. 6th wraps to slot 1.
        // Insert 0: slot 1 (ts=6000, val=0), Insert 1: slot 2 (ts=6001, val=1),
        // Insert 2: slot 3 (ts=6002, val=2)
        // Insert 3: slot 4 (ts=7000, val=10), Insert 4: slot 5 (ts=7001, val=11),
        // Insert 5: slot 1 (ts=7002, val=12)
        tx.begin();
        RingBufferSensor ct1 = session.find(RingBufferSensor.class, new Object[]{500, 1});
        errorIfNotEqual("Cross-tx: slot 1 should have last wrap", 7002L, ct1.getTimestampVal());
        errorIfNotEqual("Cross-tx: slot 1 sensor_value", 12.0, ct1.getSensorValue());
        RingBufferSensor ct2 = session.find(RingBufferSensor.class, new Object[]{500, 2});
        errorIfNotEqual("Cross-tx: slot 2 timestamp", 6001L, ct2.getTimestampVal());
        errorIfNotEqual("Cross-tx: slot 2 sensor_value", 1.0, ct2.getSensorValue());
        RingBufferSensor ct3 = session.find(RingBufferSensor.class, new Object[]{500, 3});
        errorIfNotEqual("Cross-tx: slot 3 timestamp", 6002L, ct3.getTimestampVal());
        errorIfNotEqual("Cross-tx: slot 3 sensor_value", 2.0, ct3.getSensorValue());
        RingBufferSensor ct4 = session.find(RingBufferSensor.class, new Object[]{500, 4});
        errorIfNotEqual("Cross-tx: slot 4 timestamp", 7000L, ct4.getTimestampVal());
        errorIfNotEqual("Cross-tx: slot 4 sensor_value", 10.0, ct4.getSensorValue());
        RingBufferSensor ct5 = session.find(RingBufferSensor.class, new Object[]{500, 5});
        errorIfNotEqual("Cross-tx: slot 5 timestamp", 7001L, ct5.getTimestampVal());
        errorIfNotEqual("Cross-tx: slot 5 sensor_value", 11.0, ct5.getSensorValue());
        tx.commit();
    }

    // =======================================================================
    // P0: Production-critical gaps
    // =======================================================================

    /**
     * Insert more than ring_size rows for the same PK prefix in a single
     * transaction. This exercises multiple wrap-arounds of batchMeta.advance()
     * within one batch before flushBatch() is called.
     *
     * 8 inserts, ring_size=5: slots wrap as 1,2,3,4,5,1,2,3.
     * Final state: slot 1 has insert 5 (ts=10005), slot 2 has insert 6 (ts=10006),
     * slot 3 has insert 7 (ts=10007), slot 4 has insert 3 (ts=10003),
     * slot 5 has insert 4 (ts=10004).
     */
    private void testBatchExceedingRingSize() {
        cleanup();
        tx.begin();
        for (int i = 0; i < 8; i++) {
            RingBufferSensor row = session.newInstance(RingBufferSensor.class);
            row.setSensorId(600);
            row.setTimestampVal(10000L + i);
            row.setSensorValue(i);
            session.makePersistent(row);
        }
        tx.commit();

        // After 8 inserts: slots cycle 1->2->3->4->5->1->2->3
        // Slot 1: insert 5 (ts=10005, val=5), Slot 2: insert 6 (ts=10006, val=6),
        // Slot 3: insert 7 (ts=10007, val=7), Slot 4: insert 3 (ts=10003, val=3),
        // Slot 5: insert 4 (ts=10004, val=4)
        tx.begin();
        RingBufferSensor br1 = session.find(RingBufferSensor.class, new Object[]{600, 1});
        errorIfNotEqual("Batch>ring slot 1 timestamp", 10005L, br1.getTimestampVal());
        errorIfNotEqual("Batch>ring slot 1 sensor_value", 5.0, br1.getSensorValue());
        RingBufferSensor br2 = session.find(RingBufferSensor.class, new Object[]{600, 2});
        errorIfNotEqual("Batch>ring slot 2 timestamp", 10006L, br2.getTimestampVal());
        errorIfNotEqual("Batch>ring slot 2 sensor_value", 6.0, br2.getSensorValue());
        RingBufferSensor br3 = session.find(RingBufferSensor.class, new Object[]{600, 3});
        errorIfNotEqual("Batch>ring slot 3 timestamp", 10007L, br3.getTimestampVal());
        errorIfNotEqual("Batch>ring slot 3 sensor_value", 7.0, br3.getSensorValue());
        RingBufferSensor br4 = session.find(RingBufferSensor.class, new Object[]{600, 4});
        errorIfNotEqual("Batch>ring slot 4 timestamp", 10003L, br4.getTimestampVal());
        errorIfNotEqual("Batch>ring slot 4 sensor_value", 3.0, br4.getSensorValue());
        RingBufferSensor br5 = session.find(RingBufferSensor.class, new Object[]{600, 5});
        errorIfNotEqual("Batch>ring slot 5 timestamp", 10004L, br5.getTimestampVal());
        errorIfNotEqual("Batch>ring slot 5 sensor_value", 4.0, br5.getSensorValue());
        tx.commit();
    }

    /**
     * Insert rows for different PK prefixes in a single transaction.
     * This triggers the flushBatch() -> readMetaRow() mid-transaction path
     * when the PK prefix changes.
     */
    private void testMixedPrefixesSingleTransaction() {
        cleanup();
        tx.begin();
        // Insert 3 rows for sensor 701
        for (int i = 0; i < 3; i++) {
            RingBufferSensor row = session.newInstance(RingBufferSensor.class);
            row.setSensorId(701);
            row.setTimestampVal(11000L + i);
            row.setSensorValue(i);
            session.makePersistent(row);
        }
        // Insert 2 rows for sensor 702 (triggers flush of 701 batch, new meta read)
        for (int i = 0; i < 2; i++) {
            RingBufferSensor row = session.newInstance(RingBufferSensor.class);
            row.setSensorId(702);
            row.setTimestampVal(12000L + i);
            row.setSensorValue(i + 10.0);
            session.makePersistent(row);
        }
        tx.commit();

        // Verify both prefixes independently correct
        tx.begin();
        for (int slot = 1; slot <= 3; slot++) {
            RingBufferSensor r = session.find(RingBufferSensor.class, new Object[]{701, slot});
            errorIfNotEqual("Mixed-prefix 701 slot " + slot + " not null", true, r != null);
            errorIfNotEqual("Mixed-prefix 701 slot " + slot + " timestamp",
                    11000L + (slot - 1), r.getTimestampVal());
            errorIfNotEqual("Mixed-prefix 701 slot " + slot + " sensor_value",
                    (double)(slot - 1), r.getSensorValue());
        }
        for (int slot = 1; slot <= 2; slot++) {
            RingBufferSensor r = session.find(RingBufferSensor.class, new Object[]{702, slot});
            errorIfNotEqual("Mixed-prefix 702 slot " + slot + " not null", true, r != null);
            errorIfNotEqual("Mixed-prefix 702 slot " + slot + " timestamp",
                    12000L + (slot - 1), r.getTimestampVal());
            errorIfNotEqual("Mixed-prefix 702 slot " + slot + " sensor_value",
                    (slot - 1) + 10.0, r.getSensorValue());
        }
        tx.commit();
    }

    /**
     * Interleave PK prefixes in a single transaction: A, B, A again.
     * This causes: flush batch A, start batch B, flush batch B, start new
     * batch A. The second batch for A must re-read the meta row (updated by
     * the first flush) and continue correctly.
     */
    private void testInterleavedPrefixesSingleTransaction() {
        cleanup();
        tx.begin();
        // 2 rows for sensor 801
        for (int i = 0; i < 2; i++) {
            RingBufferSensor row = session.newInstance(RingBufferSensor.class);
            row.setSensorId(801);
            row.setTimestampVal(13000L + i);
            row.setSensorValue(i);
            session.makePersistent(row);
        }
        // 2 rows for sensor 802 (flush 801, start 802)
        for (int i = 0; i < 2; i++) {
            RingBufferSensor row = session.newInstance(RingBufferSensor.class);
            row.setSensorId(802);
            row.setTimestampVal(14000L + i);
            row.setSensorValue(i);
            session.makePersistent(row);
        }
        // 2 more rows for sensor 801 (flush 802, re-read 801 meta, continue)
        for (int i = 0; i < 2; i++) {
            RingBufferSensor row = session.newInstance(RingBufferSensor.class);
            row.setSensorId(801);
            row.setTimestampVal(15000L + i);
            row.setSensorValue(i + 20.0);
            session.makePersistent(row);
        }
        tx.commit();

        // Sensor 801: 4 total inserts -> slots 1-4
        // First batch: slot 1 (ts=13000, val=0), slot 2 (ts=13001, val=1)
        // Second batch: slot 3 (ts=15000, val=20), slot 4 (ts=15001, val=21)
        tx.begin();
        RingBufferSensor il1 = session.find(RingBufferSensor.class, new Object[]{801, 1});
        errorIfNotEqual("Interleaved 801 slot 1 timestamp", 13000L, il1.getTimestampVal());
        errorIfNotEqual("Interleaved 801 slot 1 sensor_value", 0.0, il1.getSensorValue());
        RingBufferSensor il2 = session.find(RingBufferSensor.class, new Object[]{801, 2});
        errorIfNotEqual("Interleaved 801 slot 2 timestamp", 13001L, il2.getTimestampVal());
        errorIfNotEqual("Interleaved 801 slot 2 sensor_value", 1.0, il2.getSensorValue());
        RingBufferSensor il3 = session.find(RingBufferSensor.class, new Object[]{801, 3});
        errorIfNotEqual("Interleaved 801 slot 3 timestamp", 15000L, il3.getTimestampVal());
        errorIfNotEqual("Interleaved 801 slot 3 sensor_value", 20.0, il3.getSensorValue());
        RingBufferSensor il4 = session.find(RingBufferSensor.class, new Object[]{801, 4});
        errorIfNotEqual("Interleaved 801 slot 4 timestamp", 15001L, il4.getTimestampVal());
        errorIfNotEqual("Interleaved 801 slot 4 sensor_value", 21.0, il4.getSensorValue());

        // Sensor 802: 2 inserts -> slots 1-2
        RingBufferSensor il5 = session.find(RingBufferSensor.class, new Object[]{802, 1});
        errorIfNotEqual("Interleaved 802 slot 1 timestamp", 14000L, il5.getTimestampVal());
        errorIfNotEqual("Interleaved 802 slot 1 sensor_value", 0.0, il5.getSensorValue());
        RingBufferSensor il6 = session.find(RingBufferSensor.class, new Object[]{802, 2});
        errorIfNotEqual("Interleaved 802 slot 2 timestamp", 14001L, il6.getTimestampVal());
        errorIfNotEqual("Interleaved 802 slot 2 sensor_value", 1.0, il6.getSensorValue());
        tx.commit();
    }

    /**
     * Rollback a transaction that contains ring buffer inserts.
     * Since RingBufferWriter does executeNoCommitDirect internally, the
     * NoCommit operations should be rolled back by the transaction rollback.
     * After rollback, the ring should remain empty and a subsequent
     * transaction should start fresh.
     */
    private void testTransactionRollback() {
        cleanup();

        // Insert and commit 2 rows first to establish ring state
        tx.begin();
        for (int i = 0; i < 2; i++) {
            RingBufferSensor row = session.newInstance(RingBufferSensor.class);
            row.setSensorId(900);
            row.setTimestampVal(16000L + i);
            row.setSensorValue(i);
            session.makePersistent(row);
        }
        tx.commit();

        // Now insert 3 more but rollback
        tx.begin();
        for (int i = 0; i < 3; i++) {
            RingBufferSensor row = session.newInstance(RingBufferSensor.class);
            row.setSensorId(900);
            row.setTimestampVal(17000L + i);
            row.setSensorValue(i + 10.0);
            session.makePersistent(row);
        }
        tx.rollback();

        // After rollback, we need a fresh session since the transaction state
        // may be invalid. Close and reopen.
        session.close();
        session = sessionFactory.getSession();
        tx = session.currentTransaction();

        // Verify the committed data is still there and rollback didn't corrupt it
        tx.begin();
        RingBufferSensor rb1 = session.find(RingBufferSensor.class, new Object[]{900, 1});
        errorIfNotEqual("Rollback: slot 1 preserved ts", 16000L, rb1.getTimestampVal());
        errorIfNotEqual("Rollback: slot 1 preserved val", 0.0, rb1.getSensorValue());
        RingBufferSensor rb2 = session.find(RingBufferSensor.class, new Object[]{900, 2});
        errorIfNotEqual("Rollback: slot 2 preserved ts", 16001L, rb2.getTimestampVal());
        errorIfNotEqual("Rollback: slot 2 preserved val", 1.0, rb2.getSensorValue());
        // Slot 3 should NOT exist (rollback undid the inserts)
        RingBufferSensor r3 = session.find(RingBufferSensor.class, new Object[]{900, 3});
        errorIfNotEqual("Rollback: slot 3 should not exist", null, r3);
        tx.commit();

        // Verify the ring continues correctly from the committed state
        tx.begin();
        RingBufferSensor row = session.newInstance(RingBufferSensor.class);
        row.setSensorId(900);
        row.setTimestampVal(18000L);
        row.setSensorValue(99.0);
        session.makePersistent(row);
        tx.commit();

        // Should land at slot 3 (continuing from the 2 committed rows)
        tx.begin();
        RingBufferSensor r = session.find(RingBufferSensor.class, new Object[]{900, 3});
        errorIfNotEqual("Rollback recovery: slot 3 not null", true, r != null);
        errorIfNotEqual("Rollback recovery: slot 3 timestamp", 18000L, r.getTimestampVal());
        errorIfNotEqual("Rollback recovery: slot 3 sensor_value", 99.0, r.getSensorValue());
        tx.commit();
    }

    /**
     * Attempt to delete a ring buffer row via ClusterJ session.deletePersistent().
     * This should either throw a clean error or succeed via the handler.
     * The key concern is that it does NOT silently corrupt ring state.
     */
    private void testDeleteViaClusterJ() {
        cleanup();
        // Insert 3 rows
        tx.begin();
        for (int i = 0; i < 3; i++) {
            RingBufferSensor row = session.newInstance(RingBufferSensor.class);
            row.setSensorId(1000);
            row.setTimestampVal(19000L + i);
            row.setSensorValue(i);
            session.makePersistent(row);
        }
        tx.commit();

        // Try to delete a specific row via ClusterJ
        boolean deleteThrew = false;
        try {
            tx.begin();
            RingBufferSensor toDelete = session.find(RingBufferSensor.class,
                    new Object[]{1000, 2});
            if (toDelete != null) {
                session.deletePersistent(toDelete);
                tx.commit();
            } else {
                tx.commit();
                error("Delete test: could not find row to delete");
            }
        } catch (ClusterJException ex) {
            deleteThrew = true;
            // Expected: ring buffer delete without OO_RING_BUFFER_OP should fail
            if (tx.isActive()) {
                tx.rollback();
            }
        }

        // Reopen session if needed
        if (deleteThrew) {
            session.close();
            session = sessionFactory.getSession();
            tx = session.currentTransaction();
        }

        // Verify data integrity after the delete attempt
        tx.begin();
        RingBufferSensor r1 = session.find(RingBufferSensor.class, new Object[]{1000, 1});
        errorIfNotEqual("Delete test: slot 1 intact", 19000L, r1.getTimestampVal());
        RingBufferSensor r3 = session.find(RingBufferSensor.class, new Object[]{1000, 3});
        errorIfNotEqual("Delete test: slot 3 intact", 19002L, r3.getTimestampVal());

        RingBufferSensor r2 = session.find(RingBufferSensor.class, new Object[]{1000, 2});
        if (deleteThrew) {
            // Delete failed: all 3 slots should still exist
            errorIfNotEqual("Delete test: slot 2 intact after exception", 19001L,
                    r2.getTimestampVal());
        } else {
            // Delete succeeded: slot 2 should be gone
            errorIfNotEqual("Delete test: slot 2 should be null after delete", null, r2);
        }

        // Verify ring can still accept new inserts after the delete attempt
        RingBufferSensor newRow = session.newInstance(RingBufferSensor.class);
        newRow.setSensorId(1000);
        newRow.setTimestampVal(19999L);
        newRow.setSensorValue(99.0);
        session.makePersistent(newRow);
        tx.commit();

        tx.begin();
        // New insert should land at slot 4 (continuing from 3 committed rows)
        RingBufferSensor r4 = session.find(RingBufferSensor.class, new Object[]{1000, 4});
        errorIfNotEqual("Delete test: new insert after delete attempt", 19999L,
                r4.getTimestampVal());
        tx.commit();
    }

    /**
     * Attempt to update a ring buffer row via ClusterJ session.updatePersistent().
     * The update path is NOT intercepted by the ring buffer code (only insert is).
     * This should either throw or succeed without corrupting the ring state.
     */
    private void testUpdateViaClusterJ() {
        cleanup();
        // Insert 3 rows
        tx.begin();
        for (int i = 0; i < 3; i++) {
            RingBufferSensor row = session.newInstance(RingBufferSensor.class);
            row.setSensorId(1100);
            row.setTimestampVal(20000L + i);
            row.setSensorValue(i);
            session.makePersistent(row);
        }
        tx.commit();

        // Try to update an existing row
        boolean updateThrew = false;
        try {
            tx.begin();
            RingBufferSensor toUpdate = session.find(RingBufferSensor.class,
                    new Object[]{1100, 2});
            if (toUpdate != null) {
                toUpdate.setSensorValue(999.0);
                toUpdate.setTimestampVal(29999L);
                session.updatePersistent(toUpdate);
                tx.commit();
            } else {
                tx.commit();
                error("Update test: could not find row to update");
            }
        } catch (ClusterJException ex) {
            updateThrew = true;
            if (tx.isActive()) {
                tx.rollback();
            }
        }

        // Reopen session if needed
        if (updateThrew) {
            session.close();
            session = sessionFactory.getSession();
            tx = session.currentTransaction();
        }

        // Verify data integrity
        tx.begin();
        RingBufferSensor r1 = session.find(RingBufferSensor.class, new Object[]{1100, 1});
        errorIfNotEqual("Update test: slot 1 intact", 20000L, r1.getTimestampVal());
        RingBufferSensor r3 = session.find(RingBufferSensor.class, new Object[]{1100, 3});
        errorIfNotEqual("Update test: slot 3 intact", 20002L, r3.getTimestampVal());

        RingBufferSensor r2 = session.find(RingBufferSensor.class, new Object[]{1100, 2});
        if (updateThrew) {
            // Update failed: original value preserved
            errorIfNotEqual("Update test: slot 2 original after exception", 20001L,
                    r2.getTimestampVal());
        } else {
            // Update succeeded: verify updated values
            errorIfNotEqual("Update test: slot 2 updated timestamp", 29999L,
                    r2.getTimestampVal());
            errorIfNotEqual("Update test: slot 2 updated value", 999.0,
                    r2.getSensorValue());
        }

        // Verify ring can still accept new inserts
        RingBufferSensor newRow = session.newInstance(RingBufferSensor.class);
        newRow.setSensorId(1100);
        newRow.setTimestampVal(20999L);
        newRow.setSensorValue(88.0);
        session.makePersistent(newRow);
        tx.commit();

        tx.begin();
        RingBufferSensor r4 = session.find(RingBufferSensor.class, new Object[]{1100, 4});
        errorIfNotEqual("Update test: new insert after update attempt", 20999L,
                r4.getTimestampVal());
        tx.commit();
    }

    // =======================================================================
    // P1: Correctness verification
    // =======================================================================

    /**
     * Insert exactly ring_size (5) rows. This is the boundary condition where
     * nextPos is about to wrap but hasn't yet. After 5 inserts, all slots
     * 1-5 should be filled, and the next insert would wrap.
     */
    private void testExactRingFill() {
        cleanup();
        for (int i = 0; i < RING_SIZE; i++) {
            tx.begin();
            RingBufferSensor row = session.newInstance(RingBufferSensor.class);
            row.setSensorId(1200);
            row.setTimestampVal(21000L + i);
            row.setSensorValue(i);
            session.makePersistent(row);
            tx.commit();
        }

        // Verify all 5 slots are filled correctly
        tx.begin();
        for (int slot = 1; slot <= RING_SIZE; slot++) {
            RingBufferSensor r = session.find(RingBufferSensor.class,
                    new Object[]{1200, slot});
            errorIfNotEqual("Exact fill slot " + slot + " not null", true, r != null);
            errorIfNotEqual("Exact fill slot " + slot + " timestamp",
                    21000L + (slot - 1), r.getTimestampVal());
            errorIfNotEqual("Exact fill slot " + slot + " sensor_value",
                    (double)(slot - 1), r.getSensorValue());
        }
        tx.commit();

        // Now insert one more to verify the wrap happens correctly
        tx.begin();
        RingBufferSensor row = session.newInstance(RingBufferSensor.class);
        row.setSensorId(1200);
        row.setTimestampVal(21999L);
        row.setSensorValue(99.0);
        session.makePersistent(row);
        tx.commit();

        tx.begin();
        // Slot 1 should be overwritten with the new insert
        errorIfNotEqual("Exact fill + 1: slot 1 wrapped", 21999L,
                session.find(RingBufferSensor.class, new Object[]{1200, 1}).getTimestampVal());
        // Slot 2 should still have the original second insert
        errorIfNotEqual("Exact fill + 1: slot 2 unchanged", 21001L,
                session.find(RingBufferSensor.class, new Object[]{1200, 2}).getTimestampVal());
        tx.commit();
    }

    /**
     * Insert 25 rows (5 full wrap cycles) to verify sustained correctness
     * of count, totalInserts, and nextPos across many wraps.
     */
    private void testMultipleWrapCycles() {
        cleanup();
        int totalInserts = 25;
        for (int i = 0; i < totalInserts; i++) {
            tx.begin();
            RingBufferSensor row = session.newInstance(RingBufferSensor.class);
            row.setSensorId(1300);
            row.setTimestampVal(22000L + i);
            row.setSensorValue(i);
            session.makePersistent(row);
            tx.commit();
        }

        // After 25 inserts with ring_size=5:
        // Last 5 inserts are 20-24 (ts=22020..22024, val=20..24).
        // 25 % 5 = 0, so nextPos wraps back to 1 after the 25th insert.
        // Insert 20 (idx 20): slot (20%5)+1=1, Insert 21: slot 2, ..., Insert 24: slot 5
        tx.begin();
        for (int slot = 1; slot <= RING_SIZE; slot++) {
            int insertIdx = (totalInserts - RING_SIZE) + (slot - 1);
            long expectedTs = 22000L + insertIdx;
            double expectedVal = (double) insertIdx;
            RingBufferSensor r = session.find(RingBufferSensor.class,
                    new Object[]{1300, slot});
            errorIfNotEqual("Multi-wrap slot " + slot + " not null", true, r != null);
            errorIfNotEqual("Multi-wrap slot " + slot + " timestamp",
                    expectedTs, r.getTimestampVal());
            errorIfNotEqual("Multi-wrap slot " + slot + " sensor_value",
                    expectedVal, r.getSensorValue());
        }
        tx.commit();
    }

    /**
     * Insert rows with NULL values for nullable user columns
     * (timestamp_val, sensor_value).
     */
    private void testNullUserColumns() {
        cleanup();
        tx.begin();
        // Insert with both user columns null
        RingBufferSensor row1 = session.newInstance(RingBufferSensor.class);
        row1.setSensorId(1400);
        // Don't set timestamp_val or sensor_value -- they should be null/default
        session.makePersistent(row1);
        tx.commit();

        tx.begin();
        RingBufferSensor r1 = session.find(RingBufferSensor.class, new Object[]{1400, 1});
        errorIfNotEqual("Null columns: row exists", true, r1 != null);
        errorIfNotEqual("Null columns: sensor_id", 1400, r1.getSensorId());
        errorIfNotEqual("Null columns: ring_idx", 1, r1.getRingIdx());
        // Unset primitive long/double default to 0 in ClusterJ proxy
        errorIfNotEqual("Null columns: timestamp_val should be 0", 0L, r1.getTimestampVal());
        errorIfNotEqual("Null columns: sensor_value should be 0.0", 0.0, r1.getSensorValue());
        tx.commit();

        // Insert a second row with only timestamp set
        tx.begin();
        RingBufferSensor row2 = session.newInstance(RingBufferSensor.class);
        row2.setSensorId(1400);
        row2.setTimestampVal(23000L);
        // Don't set sensor_value
        session.makePersistent(row2);
        tx.commit();

        tx.begin();
        RingBufferSensor r2 = session.find(RingBufferSensor.class, new Object[]{1400, 2});
        errorIfNotEqual("Null columns: row 2 timestamp", 23000L, r2.getTimestampVal());
        errorIfNotEqual("Null columns: row 2 sensor_value should be 0.0", 0.0,
                r2.getSensorValue());
        tx.commit();
    }

    /**
     * Test ring buffer table with NOT NULL user columns (VARCHAR NOT NULL, INT NOT NULL).
     * Exercises the zeroColumn() path in RingBufferWriter which must correctly
     * zero fixed-length columns (e.g. INT = 4 bytes) in the meta row.
     * Uses ring_buffer_notnull table with MAX_ROWS_PER_PK=3.
     */
    private void testNotNullColumns() {
        // Clean up the notnull table via SQL
        try {
            getConnection();
            Statement stmt = connection.createStatement();
            stmt.execute("DELETE FROM ring_buffer_notnull");
            stmt.close();
        } catch (Throwable t) {
            // ignore - table might be empty
        }

        // Insert a single row
        tx.begin();
        RingBufferNotNull row1 = session.newInstance(RingBufferNotNull.class);
        row1.setClientId(1);
        row1.setName("alice");
        row1.setScore(99);
        session.makePersistent(row1);
        tx.commit();

        // Verify via SQL that data row has correct values
        try {
            getConnection();
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(
                    "SELECT name, score FROM ring_buffer_notnull"
                    + " WHERE client_id = 1 AND ring_idx > 0");
            boolean found = false;
            while (rs.next()) {
                found = true;
                errorIfNotEqual("NOT NULL: name", "alice", rs.getString("name"));
                errorIfNotEqual("NOT NULL: score", 99, rs.getInt("score"));
            }
            rs.close();
            errorIfNotEqual("NOT NULL: data row found", true, found);

            // Verify meta row has zeroed NOT NULL columns (score=0, name='')
            stmt.execute("SET ndb_ring_buffer_show_meta = 1");
            rs = stmt.executeQuery(
                    "SELECT ring_idx, name, score FROM ring_buffer_notnull"
                    + " WHERE client_id = 1 AND ring_idx = 0");
            if (rs.next()) {
                errorIfNotEqual("NOT NULL meta: score should be 0", 0, rs.getInt("score"));
                errorIfNotEqual("NOT NULL meta: name should be empty", "", rs.getString("name"));
            } else {
                error("NOT NULL: meta row not found");
            }
            rs.close();
            stmt.execute("SET ndb_ring_buffer_show_meta = 0");
            stmt.close();
        } catch (Exception ex) {
            error("NOT NULL SQL check failed: " + ex.getMessage());
        }

        // Fill the ring (3 slots) and verify wrap-around
        tx.begin();
        RingBufferNotNull row2 = session.newInstance(RingBufferNotNull.class);
        row2.setClientId(1);
        row2.setName("bob");
        row2.setScore(88);
        session.makePersistent(row2);
        RingBufferNotNull row3 = session.newInstance(RingBufferNotNull.class);
        row3.setClientId(1);
        row3.setName("charlie");
        row3.setScore(77);
        session.makePersistent(row3);
        tx.commit();

        // Insert one more to trigger wrap-around (overwrites slot 1)
        tx.begin();
        RingBufferNotNull row4 = session.newInstance(RingBufferNotNull.class);
        row4.setClientId(1);
        row4.setName("diana");
        row4.setScore(66);
        session.makePersistent(row4);
        tx.commit();

        // Verify: ring should now contain bob(88), charlie(77), diana(66)
        // slot 1=diana(66), slot 2=bob(88), slot 3=charlie(77)
        tx.begin();
        RingBufferNotNull s1 = session.find(RingBufferNotNull.class, new Object[]{1, 1});
        RingBufferNotNull s2 = session.find(RingBufferNotNull.class, new Object[]{1, 2});
        RingBufferNotNull s3 = session.find(RingBufferNotNull.class, new Object[]{1, 3});
        errorIfNotEqual("NOT NULL wrap: slot 1 name", "diana", s1.getName());
        errorIfNotEqual("NOT NULL wrap: slot 1 score", 66, s1.getScore());
        errorIfNotEqual("NOT NULL wrap: slot 2 name", "bob", s2.getName());
        errorIfNotEqual("NOT NULL wrap: slot 2 score", 88, s2.getScore());
        errorIfNotEqual("NOT NULL wrap: slot 3 name", "charlie", s3.getName());
        errorIfNotEqual("NOT NULL wrap: slot 3 score", 77, s3.getScore());
        tx.commit();
    }

    /**
     * Find a ring slot that hasn't been written yet.
     * Should return null.
     */
    private void testFindNonExistentSlot() {
        cleanup();
        // Insert 2 rows for sensor 1500
        tx.begin();
        for (int i = 0; i < 2; i++) {
            RingBufferSensor row = session.newInstance(RingBufferSensor.class);
            row.setSensorId(1500);
            row.setTimestampVal(24000L + i);
            row.setSensorValue(i);
            session.makePersistent(row);
        }
        tx.commit();

        tx.begin();
        // Slot 3 has never been written
        RingBufferSensor r3 = session.find(RingBufferSensor.class, new Object[]{1500, 3});
        errorIfNotEqual("Non-existent slot 3 should be null", null, r3);

        // Slot 5 has never been written
        RingBufferSensor r5 = session.find(RingBufferSensor.class, new Object[]{1500, 5});
        errorIfNotEqual("Non-existent slot 5 should be null", null, r5);

        // Completely unknown sensor_id
        RingBufferSensor rUnknown = session.find(RingBufferSensor.class, new Object[]{9999, 1});
        errorIfNotEqual("Unknown sensor slot should be null", null, rUnknown);
        tx.commit();
    }

    /**
     * Check whether the meta row (ring_idx=0) is visible via session.find().
     * The meta row is an internal implementation detail and ideally should
     * be hidden, but at minimum we need to know the behavior.
     */
    private void testMetaRowVisibility() {
        cleanup();
        // Insert 3 rows to establish a ring with meta row
        tx.begin();
        for (int i = 0; i < 3; i++) {
            RingBufferSensor row = session.newInstance(RingBufferSensor.class);
            row.setSensorId(1600);
            row.setTimestampVal(25000L + i);
            row.setSensorValue(i);
            session.makePersistent(row);
        }
        tx.commit();

        // Try to find the meta row at ring_idx=0.
        // ClusterJ find() does NOT set OO_RING_BUFFER_OP, so the kernel
        // filters meta rows. The meta row should NOT be visible.
        tx.begin();
        RingBufferSensor metaRow = session.find(RingBufferSensor.class,
                new Object[]{1600, 0});
        errorIfNotEqual("Meta row: should NOT be visible via ClusterJ find()",
                null, metaRow);
        // Data rows must be unaffected regardless of meta row visibility
        for (int slot = 1; slot <= 3; slot++) {
            RingBufferSensor r = session.find(RingBufferSensor.class,
                    new Object[]{1600, slot});
            errorIfNotEqual("Meta visibility: data slot " + slot + " should exist",
                    true, r != null);
            errorIfNotEqual("Meta visibility: data slot " + slot + " sensor_id",
                    1600, r.getSensorId());
            errorIfNotEqual("Meta visibility: data slot " + slot + " ring_idx",
                    slot, r.getRingIdx());
            errorIfNotEqual("Meta visibility: data slot " + slot + " timestamp",
                    25000L + (slot - 1), r.getTimestampVal());
            errorIfNotEqual("Meta visibility: data slot " + slot + " sensor_value",
                    (double)(slot - 1), r.getSensorValue());
        }
        tx.commit();
    }

    /**
     * Query ring buffer table using ClusterJ QueryBuilder (table scan).
     * Production read patterns often use scans rather than PK lookups.
     */
    private void testQueryScan() {
        cleanup();
        // Insert rows for 2 sensors
        tx.begin();
        for (int i = 0; i < 3; i++) {
            RingBufferSensor row = session.newInstance(RingBufferSensor.class);
            row.setSensorId(1700);
            row.setTimestampVal(26000L + i);
            row.setSensorValue(i * 1.5);
            session.makePersistent(row);
        }
        for (int i = 0; i < 2; i++) {
            RingBufferSensor row = session.newInstance(RingBufferSensor.class);
            row.setSensorId(1701);
            row.setTimestampVal(27000L + i);
            row.setSensorValue(i * 2.5);
            session.makePersistent(row);
        }
        tx.commit();

        // Query: find all rows for sensor_id=1700
        tx.begin();
        QueryBuilder builder = session.getQueryBuilder();
        QueryDomainType<RingBufferSensor> dobj =
                builder.createQueryDefinition(RingBufferSensor.class);
        PredicateOperand param = dobj.param("sensorId");
        PredicateOperand column = dobj.get("sensorId");
        Predicate pred = column.equal(param);
        dobj.where(pred);
        Query<RingBufferSensor> query = session.createQuery(dobj);
        query.setParameter("sensorId", 1700);
        List<RingBufferSensor> results = query.getResultList();

        // Should return exactly 3 data rows (kernel filters meta rows)
        errorIfNotEqual("Query scan: total result count for sensor 1700 (no meta)",
                3, results.size());
        boolean[] slotsFound = new boolean[RING_SIZE + 1]; // index 1..5
        for (RingBufferSensor r : results) {
            errorIfNotEqual("Query scan 1700: ring_idx > 0 (no meta row)",
                    true, r.getRingIdx() > 0);
            errorIfNotEqual("Query scan 1700: sensor_id", 1700, r.getSensorId());
            int slot = r.getRingIdx();
            errorIfNotEqual("Query scan 1700 slot " + slot + " timestamp",
                    26000L + (slot - 1), r.getTimestampVal());
            errorIfNotEqual("Query scan 1700 slot " + slot + " sensor_value",
                    (slot - 1) * 1.5, r.getSensorValue());
            slotsFound[slot] = true;
        }
        for (int s = 1; s <= 3; s++) {
            errorIfNotEqual("Query scan: slot " + s + " found", true, slotsFound[s]);
        }
        tx.commit();

        // Query: find all rows where timestamp_val >= 27000
        tx.begin();
        builder = session.getQueryBuilder();
        dobj = builder.createQueryDefinition(RingBufferSensor.class);
        param = dobj.param("minTs");
        column = dobj.get("timestampVal");
        pred = column.greaterEqual(param);
        dobj.where(pred);
        query = session.createQuery(dobj);
        query.setParameter("minTs", 27000L);
        results = query.getResultList();

        // Should return exactly 2 rows (both from sensor 1701, no meta rows)
        errorIfNotEqual("Query scan ts>=27000: total result count (no meta)",
                2, results.size());
        for (RingBufferSensor r : results) {
            errorIfNotEqual("Query scan ts>=27000: ring_idx > 0 (no meta row)",
                    true, r.getRingIdx() > 0);
            // All matching rows should be from sensor 1701
            errorIfNotEqual("Query scan ts>=27000: sensor_id", 1701, r.getSensorId());
            int slot = r.getRingIdx();
            errorIfNotEqual("Query scan ts>=27000 slot " + slot + " timestamp",
                    27000L + (slot - 1), r.getTimestampVal());
            errorIfNotEqual("Query scan ts>=27000 slot " + slot + " sensor_value",
                    (slot - 1) * 2.5, r.getSensorValue());
        }
        tx.commit();
    }

    // =======================================================================
    // P2: Edge cases and hardening
    // =======================================================================

    /**
     * Reuse the same session across multiple independent transactions
     * inserting into the same ring buffer table. Verify that the
     * RingBufferWriter is properly cleaned up between transactions.
     */
    private void testSessionReuseAcrossTransactions() {
        cleanup();

        // Transaction 1: sensor 1800, 2 rows
        tx.begin();
        for (int i = 0; i < 2; i++) {
            RingBufferSensor row = session.newInstance(RingBufferSensor.class);
            row.setSensorId(1800);
            row.setTimestampVal(28000L + i);
            row.setSensorValue(i);
            session.makePersistent(row);
        }
        tx.commit();

        // Transaction 2: different sensor 1801, 3 rows (same session)
        tx.begin();
        for (int i = 0; i < 3; i++) {
            RingBufferSensor row = session.newInstance(RingBufferSensor.class);
            row.setSensorId(1801);
            row.setTimestampVal(29000L + i);
            row.setSensorValue(i);
            session.makePersistent(row);
        }
        tx.commit();

        // Transaction 3: back to sensor 1800, add 3 more (same session)
        tx.begin();
        for (int i = 0; i < 3; i++) {
            RingBufferSensor row = session.newInstance(RingBufferSensor.class);
            row.setSensorId(1800);
            row.setTimestampVal(30000L + i);
            row.setSensorValue(i + 10.0);
            session.makePersistent(row);
        }
        tx.commit();

        // Verify sensor 1800: 5 total inserts, all in slots 1-5
        // Inserts 0-1 (ts=28000,28001) from tx1 at slots 1-2
        // Inserts 2-4 (ts=30000,30001,30002) from tx3 at slots 3-5
        tx.begin();
        errorIfNotEqual("Session reuse 1800 slot 1", 28000L,
                session.find(RingBufferSensor.class, new Object[]{1800, 1}).getTimestampVal());
        errorIfNotEqual("Session reuse 1800 slot 2", 28001L,
                session.find(RingBufferSensor.class, new Object[]{1800, 2}).getTimestampVal());
        errorIfNotEqual("Session reuse 1800 slot 3", 30000L,
                session.find(RingBufferSensor.class, new Object[]{1800, 3}).getTimestampVal());
        errorIfNotEqual("Session reuse 1800 slot 4", 30001L,
                session.find(RingBufferSensor.class, new Object[]{1800, 4}).getTimestampVal());
        errorIfNotEqual("Session reuse 1800 slot 5", 30002L,
                session.find(RingBufferSensor.class, new Object[]{1800, 5}).getTimestampVal());

        // Verify sensor 1801: 3 inserts at slots 1-3
        errorIfNotEqual("Session reuse 1801 slot 1", 29000L,
                session.find(RingBufferSensor.class, new Object[]{1801, 1}).getTimestampVal());
        errorIfNotEqual("Session reuse 1801 slot 2", 29001L,
                session.find(RingBufferSensor.class, new Object[]{1801, 2}).getTimestampVal());
        errorIfNotEqual("Session reuse 1801 slot 3", 29002L,
                session.find(RingBufferSensor.class, new Object[]{1801, 3}).getTimestampVal());
        tx.commit();
    }

    /**
     * Begin and commit a transaction with NO ring buffer inserts.
     * This should be a no-op for the ring buffer writer (flushRingBufferWriters
     * should handle null/empty writer map gracefully).
     */
    private void testEmptyTransaction() {
        cleanup();

        // Insert some data first
        tx.begin();
        RingBufferSensor row = session.newInstance(RingBufferSensor.class);
        row.setSensorId(1900);
        row.setTimestampVal(31000L);
        row.setSensorValue(1.0);
        session.makePersistent(row);
        tx.commit();

        // Empty transaction -- just begin and commit
        tx.begin();
        tx.commit();

        // Another empty transaction with only a read
        tx.begin();
        RingBufferSensor r = session.find(RingBufferSensor.class, new Object[]{1900, 1});
        errorIfNotEqual("Empty tx: data still accessible", 31000L, r.getTimestampVal());
        tx.commit();

        // Now insert again to verify ring state wasn't corrupted by empty tx
        tx.begin();
        RingBufferSensor row2 = session.newInstance(RingBufferSensor.class);
        row2.setSensorId(1900);
        row2.setTimestampVal(31001L);
        row2.setSensorValue(2.0);
        session.makePersistent(row2);
        tx.commit();

        tx.begin();
        errorIfNotEqual("Empty tx: slot 2 after empty tx", 31001L,
                session.find(RingBufferSensor.class, new Object[]{1900, 2}).getTimestampVal());
        tx.commit();
    }

    /**
     * Batch insert that wraps more than once in a single transaction.
     * 13 inserts with ring_size=5 means 2 full wraps + 3 extra.
     * Verify the final state after all wraps complete within one tx.
     */
    private void testBatchExceedingRingSizeMultipleWraps() {
        cleanup();
        int totalInserts = 13;
        tx.begin();
        for (int i = 0; i < totalInserts; i++) {
            RingBufferSensor row = session.newInstance(RingBufferSensor.class);
            row.setSensorId(2000);
            row.setTimestampVal(32000L + i);
            row.setSensorValue(i);
            session.makePersistent(row);
        }
        tx.commit();

        // After 13 inserts with ring_size=5, slots cycle through:
        // i=0→s1, i=1→s2, i=2→s3, i=3→s4, i=4→s5,
        // i=5→s1, i=6→s2, i=7→s3, i=8→s4, i=9→s5,
        // i=10→s1, i=11→s2, i=12→s3
        // Final: Slot 1=i10(ts=32010,val=10), Slot 2=i11(ts=32011,val=11),
        //        Slot 3=i12(ts=32012,val=12), Slot 4=i8(ts=32008,val=8),
        //        Slot 5=i9(ts=32009,val=9)
        tx.begin();
        RingBufferSensor mw1 = session.find(RingBufferSensor.class, new Object[]{2000, 1});
        errorIfNotEqual("Multi-wrap batch slot 1 timestamp", 32010L, mw1.getTimestampVal());
        errorIfNotEqual("Multi-wrap batch slot 1 sensor_value", 10.0, mw1.getSensorValue());
        RingBufferSensor mw2 = session.find(RingBufferSensor.class, new Object[]{2000, 2});
        errorIfNotEqual("Multi-wrap batch slot 2 timestamp", 32011L, mw2.getTimestampVal());
        errorIfNotEqual("Multi-wrap batch slot 2 sensor_value", 11.0, mw2.getSensorValue());
        RingBufferSensor mw3 = session.find(RingBufferSensor.class, new Object[]{2000, 3});
        errorIfNotEqual("Multi-wrap batch slot 3 timestamp", 32012L, mw3.getTimestampVal());
        errorIfNotEqual("Multi-wrap batch slot 3 sensor_value", 12.0, mw3.getSensorValue());
        RingBufferSensor mw4 = session.find(RingBufferSensor.class, new Object[]{2000, 4});
        errorIfNotEqual("Multi-wrap batch slot 4 timestamp", 32008L, mw4.getTimestampVal());
        errorIfNotEqual("Multi-wrap batch slot 4 sensor_value", 8.0, mw4.getSensorValue());
        RingBufferSensor mw5 = session.find(RingBufferSensor.class, new Object[]{2000, 5});
        errorIfNotEqual("Multi-wrap batch slot 5 timestamp", 32009L, mw5.getTimestampVal());
        errorIfNotEqual("Multi-wrap batch slot 5 sensor_value", 9.0, mw5.getSensorValue());
        tx.commit();
    }

    // =======================================================================
    // P3: Session cache and DTO cache
    // =======================================================================

    /**
     * Insert rows, return session to cache, get session from cache,
     * insert more rows. Verify ring continues correctly from the
     * committed state (the RingBufferWriter is per-transaction, so
     * a cached session must re-read the meta row on the new transaction).
     */
    private void testSessionCacheInsertContinuity() {
        cleanup();

        // Get a fresh session, insert 2 rows, return to cache
        Session s1 = sessionFactory.getSession();
        Transaction t1 = s1.currentTransaction();
        t1.begin();
        for (int i = 0; i < 2; i++) {
            RingBufferSensor row = s1.newInstance(RingBufferSensor.class);
            row.setSensorId(2100);
            row.setTimestampVal(40000L + i);
            row.setSensorValue(i);
            s1.makePersistent(row);
        }
        t1.commit();
        s1.closeCache();  // return to session cache pool

        // Get session from cache, insert 3 more rows
        Session s2 = sessionFactory.getSession();
        Transaction t2 = s2.currentTransaction();
        t2.begin();
        for (int i = 0; i < 3; i++) {
            RingBufferSensor row = s2.newInstance(RingBufferSensor.class);
            row.setSensorId(2100);
            row.setTimestampVal(41000L + i);
            row.setSensorValue(i + 10.0);
            s2.makePersistent(row);
        }
        t2.commit();

        // Verify: 5 total inserts, slots 1-5
        t2.begin();
        errorIfNotEqual("SessionCache continuity slot 1", 40000L,
                s2.find(RingBufferSensor.class, new Object[]{2100, 1}).getTimestampVal());
        errorIfNotEqual("SessionCache continuity slot 2", 40001L,
                s2.find(RingBufferSensor.class, new Object[]{2100, 2}).getTimestampVal());
        errorIfNotEqual("SessionCache continuity slot 3", 41000L,
                s2.find(RingBufferSensor.class, new Object[]{2100, 3}).getTimestampVal());
        errorIfNotEqual("SessionCache continuity slot 4", 41001L,
                s2.find(RingBufferSensor.class, new Object[]{2100, 4}).getTimestampVal());
        errorIfNotEqual("SessionCache continuity slot 5", 41002L,
                s2.find(RingBufferSensor.class, new Object[]{2100, 5}).getTimestampVal());
        t2.commit();
        s2.closeCache();
    }

    /**
     * Batch insert within a cached session: insert multiple rows in
     * one transaction, return session to cache, get it back, batch
     * insert again.
     */
    private void testSessionCacheBatchInsert() {
        cleanup();

        // First cached session: batch insert 3 rows
        Session s1 = sessionFactory.getSession();
        Transaction t1 = s1.currentTransaction();
        t1.begin();
        for (int i = 0; i < 3; i++) {
            RingBufferSensor row = s1.newInstance(RingBufferSensor.class);
            row.setSensorId(2200);
            row.setTimestampVal(42000L + i);
            row.setSensorValue(i);
            s1.makePersistent(row);
        }
        t1.commit();
        s1.closeCache();

        // Second cached session: batch insert 4 more (causes wrap)
        Session s2 = sessionFactory.getSession();
        Transaction t2 = s2.currentTransaction();
        t2.begin();
        for (int i = 0; i < 4; i++) {
            RingBufferSensor row = s2.newInstance(RingBufferSensor.class);
            row.setSensorId(2200);
            row.setTimestampVal(43000L + i);
            row.setSensorValue(i + 10.0);
            s2.makePersistent(row);
        }
        t2.commit();

        // After 7 inserts: slots wrap at 6th and 7th
        // Slot 1: insert 5 (ts=43002), Slot 2: insert 6 (ts=43003),
        // Slot 3: insert 2 (ts=42002), Slot 4: insert 3 (ts=43000),
        // Slot 5: insert 4 (ts=43001)
        t2.begin();
        errorIfNotEqual("SessionCache batch slot 1", 43002L,
                s2.find(RingBufferSensor.class, new Object[]{2200, 1}).getTimestampVal());
        errorIfNotEqual("SessionCache batch slot 2", 43003L,
                s2.find(RingBufferSensor.class, new Object[]{2200, 2}).getTimestampVal());
        errorIfNotEqual("SessionCache batch slot 3", 42002L,
                s2.find(RingBufferSensor.class, new Object[]{2200, 3}).getTimestampVal());
        errorIfNotEqual("SessionCache batch slot 4", 43000L,
                s2.find(RingBufferSensor.class, new Object[]{2200, 4}).getTimestampVal());
        errorIfNotEqual("SessionCache batch slot 5", 43001L,
                s2.find(RingBufferSensor.class, new Object[]{2200, 5}).getTimestampVal());
        t2.commit();
        s2.closeCache();
    }

    /**
     * Use cached sessions to insert into multiple PK prefixes across
     * cache cycles. Each prefix should maintain its own independent ring.
     */
    private void testSessionCacheMultiplePrefixes() {
        cleanup();

        // Session 1: insert for sensor 2301 and 2302
        Session s1 = sessionFactory.getSession();
        Transaction t1 = s1.currentTransaction();
        t1.begin();
        for (int i = 0; i < 3; i++) {
            RingBufferSensor row = s1.newInstance(RingBufferSensor.class);
            row.setSensorId(2301);
            row.setTimestampVal(44000L + i);
            row.setSensorValue(i);
            s1.makePersistent(row);
        }
        for (int i = 0; i < 2; i++) {
            RingBufferSensor row = s1.newInstance(RingBufferSensor.class);
            row.setSensorId(2302);
            row.setTimestampVal(45000L + i);
            row.setSensorValue(i);
            s1.makePersistent(row);
        }
        t1.commit();
        s1.closeCache();

        // Session 2: insert more for both sensors
        Session s2 = sessionFactory.getSession();
        Transaction t2 = s2.currentTransaction();
        t2.begin();
        for (int i = 0; i < 2; i++) {
            RingBufferSensor row = s2.newInstance(RingBufferSensor.class);
            row.setSensorId(2301);
            row.setTimestampVal(46000L + i);
            row.setSensorValue(i + 10.0);
            s2.makePersistent(row);
        }
        for (int i = 0; i < 3; i++) {
            RingBufferSensor row = s2.newInstance(RingBufferSensor.class);
            row.setSensorId(2302);
            row.setTimestampVal(47000L + i);
            row.setSensorValue(i + 10.0);
            s2.makePersistent(row);
        }
        t2.commit();

        // Sensor 2301: 5 inserts -> slots 1-5
        t2.begin();
        errorIfNotEqual("SessionCache prefix 2301 slot 1", 44000L,
                s2.find(RingBufferSensor.class, new Object[]{2301, 1}).getTimestampVal());
        errorIfNotEqual("SessionCache prefix 2301 slot 2", 44001L,
                s2.find(RingBufferSensor.class, new Object[]{2301, 2}).getTimestampVal());
        errorIfNotEqual("SessionCache prefix 2301 slot 3", 44002L,
                s2.find(RingBufferSensor.class, new Object[]{2301, 3}).getTimestampVal());
        errorIfNotEqual("SessionCache prefix 2301 slot 4", 46000L,
                s2.find(RingBufferSensor.class, new Object[]{2301, 4}).getTimestampVal());
        errorIfNotEqual("SessionCache prefix 2301 slot 5", 46001L,
                s2.find(RingBufferSensor.class, new Object[]{2301, 5}).getTimestampVal());

        // Sensor 2302: 5 inserts -> slots 1-5
        errorIfNotEqual("SessionCache prefix 2302 slot 1", 45000L,
                s2.find(RingBufferSensor.class, new Object[]{2302, 1}).getTimestampVal());
        errorIfNotEqual("SessionCache prefix 2302 slot 2", 45001L,
                s2.find(RingBufferSensor.class, new Object[]{2302, 2}).getTimestampVal());
        errorIfNotEqual("SessionCache prefix 2302 slot 3", 47000L,
                s2.find(RingBufferSensor.class, new Object[]{2302, 3}).getTimestampVal());
        errorIfNotEqual("SessionCache prefix 2302 slot 4", 47001L,
                s2.find(RingBufferSensor.class, new Object[]{2302, 4}).getTimestampVal());
        errorIfNotEqual("SessionCache prefix 2302 slot 5", 47002L,
                s2.find(RingBufferSensor.class, new Object[]{2302, 5}).getTimestampVal());
        t2.commit();
        s2.closeCache();
    }

    /**
     * Cycle a session through cache multiple times with wrap-around.
     * Insert more than ring_size across 3 cache cycles to verify the
     * meta row is always re-read correctly from a cached session.
     */
    private void testSessionCacheWrapAround() {
        cleanup();

        // Cycle 1: insert 3 rows
        Session s = sessionFactory.getSession();
        Transaction t = s.currentTransaction();
        t.begin();
        for (int i = 0; i < 3; i++) {
            RingBufferSensor row = s.newInstance(RingBufferSensor.class);
            row.setSensorId(2400);
            row.setTimestampVal(48000L + i);
            row.setSensorValue(i);
            s.makePersistent(row);
        }
        t.commit();
        s.closeCache();

        // Cycle 2: insert 3 more (fills ring at 5, wraps 6th to slot 1)
        s = sessionFactory.getSession();
        t = s.currentTransaction();
        t.begin();
        for (int i = 0; i < 3; i++) {
            RingBufferSensor row = s.newInstance(RingBufferSensor.class);
            row.setSensorId(2400);
            row.setTimestampVal(49000L + i);
            row.setSensorValue(i + 10.0);
            s.makePersistent(row);
        }
        t.commit();
        s.closeCache();

        // Cycle 3: insert 3 more (wraps further)
        s = sessionFactory.getSession();
        t = s.currentTransaction();
        t.begin();
        for (int i = 0; i < 3; i++) {
            RingBufferSensor row = s.newInstance(RingBufferSensor.class);
            row.setSensorId(2400);
            row.setTimestampVal(50000L + i);
            row.setSensorValue(i + 20.0);
            s.makePersistent(row);
        }
        t.commit();

        // 9 total inserts with ring_size=5:
        // Inserts 0-2: slots 1,2,3 (ts=48000,48001,48002)
        // Inserts 3-5: slots 4,5,1 (ts=49000,49001,49002)
        // Inserts 6-8: slots 2,3,4 (ts=50000,50001,50002)
        // Final: slot 1=49002, slot 2=50000, slot 3=50001, slot 4=50002, slot 5=49001
        t.begin();
        errorIfNotEqual("SessionCache wrap slot 1", 49002L,
                s.find(RingBufferSensor.class, new Object[]{2400, 1}).getTimestampVal());
        errorIfNotEqual("SessionCache wrap slot 2", 50000L,
                s.find(RingBufferSensor.class, new Object[]{2400, 2}).getTimestampVal());
        errorIfNotEqual("SessionCache wrap slot 3", 50001L,
                s.find(RingBufferSensor.class, new Object[]{2400, 3}).getTimestampVal());
        errorIfNotEqual("SessionCache wrap slot 4", 50002L,
                s.find(RingBufferSensor.class, new Object[]{2400, 4}).getTimestampVal());
        errorIfNotEqual("SessionCache wrap slot 5", 49001L,
                s.find(RingBufferSensor.class, new Object[]{2400, 5}).getTimestampVal());
        t.commit();
        s.closeCache();
    }

    /**
     * Run the same insert-verify cycle using session.close() instead of
     * closeCache(). This confirms ring buffer works the same without
     * session caching, serving as a control test.
     */
    private void testWithoutSessionCache() {
        cleanup();

        // Session 1: insert 3 rows, close without caching
        Session s1 = sessionFactory.getSession();
        Transaction t1 = s1.currentTransaction();
        t1.begin();
        for (int i = 0; i < 3; i++) {
            RingBufferSensor row = s1.newInstance(RingBufferSensor.class);
            row.setSensorId(2500);
            row.setTimestampVal(51000L + i);
            row.setSensorValue(i);
            s1.makePersistent(row);
        }
        t1.commit();
        s1.close();  // no caching

        // Session 2: insert 3 more, close without caching
        Session s2 = sessionFactory.getSession();
        Transaction t2 = s2.currentTransaction();
        t2.begin();
        for (int i = 0; i < 3; i++) {
            RingBufferSensor row = s2.newInstance(RingBufferSensor.class);
            row.setSensorId(2500);
            row.setTimestampVal(52000L + i);
            row.setSensorValue(i + 10.0);
            s2.makePersistent(row);
        }
        t2.commit();

        // 6 inserts: slot 1 wrapped
        t2.begin();
        errorIfNotEqual("NoCache slot 1", 52002L,
                s2.find(RingBufferSensor.class, new Object[]{2500, 1}).getTimestampVal());
        errorIfNotEqual("NoCache slot 2", 51001L,
                s2.find(RingBufferSensor.class, new Object[]{2500, 2}).getTimestampVal());
        errorIfNotEqual("NoCache slot 3", 51002L,
                s2.find(RingBufferSensor.class, new Object[]{2500, 3}).getTimestampVal());
        errorIfNotEqual("NoCache slot 4", 52000L,
                s2.find(RingBufferSensor.class, new Object[]{2500, 4}).getTimestampVal());
        errorIfNotEqual("NoCache slot 5", 52001L,
                s2.find(RingBufferSensor.class, new Object[]{2500, 5}).getTimestampVal());
        t2.commit();
        s2.close();
    }

    // =======================================================================
    // P4: DTO cache
    // =======================================================================

    /**
     * Insert rows using releaseCache to return DTO instances to the pool.
     * Get new instances (which may be recycled from cache) and insert more.
     * Verify that recycled DTOs don't carry stale ring_idx or other state.
     */
    private void testDtoCacheInsert() {
        cleanup();
        tx.begin();
        for (int i = 0; i < 3; i++) {
            RingBufferSensor row = session.newInstance(RingBufferSensor.class);
            row.setSensorId(2600);
            row.setTimestampVal(53000L + i);
            row.setSensorValue(i);
            session.makePersistent(row);
            session.releaseCache(row, RingBufferSensor.class);
        }
        tx.commit();

        // Insert 3 more with recycled DTOs
        tx.begin();
        for (int i = 0; i < 3; i++) {
            RingBufferSensor row = session.newInstance(RingBufferSensor.class);
            row.setSensorId(2600);
            row.setTimestampVal(54000L + i);
            row.setSensorValue(i + 10.0);
            session.makePersistent(row);
            session.releaseCache(row, RingBufferSensor.class);
        }
        tx.commit();

        // 6 inserts: slot 1 wrapped
        tx.begin();
        errorIfNotEqual("DTO cache insert slot 1", 54002L,
                session.find(RingBufferSensor.class, new Object[]{2600, 1}).getTimestampVal());
        errorIfNotEqual("DTO cache insert slot 2", 53001L,
                session.find(RingBufferSensor.class, new Object[]{2600, 2}).getTimestampVal());
        errorIfNotEqual("DTO cache insert slot 3", 53002L,
                session.find(RingBufferSensor.class, new Object[]{2600, 3}).getTimestampVal());
        errorIfNotEqual("DTO cache insert slot 4", 54000L,
                session.find(RingBufferSensor.class, new Object[]{2600, 4}).getTimestampVal());
        errorIfNotEqual("DTO cache insert slot 5", 54001L,
                session.find(RingBufferSensor.class, new Object[]{2600, 5}).getTimestampVal());
        tx.commit();
    }

    /**
     * Batch insert multiple rows in one transaction, releasing each DTO
     * to cache immediately after makePersistent. The ring buffer writer
     * copies buffer data on addRow(), so releasing the DTO should be safe.
     */
    private void testDtoCacheBatchInsert() {
        cleanup();
        tx.begin();
        for (int i = 0; i < 8; i++) {
            RingBufferSensor row = session.newInstance(RingBufferSensor.class);
            row.setSensorId(2700);
            row.setTimestampVal(55000L + i);
            row.setSensorValue(i);
            session.makePersistent(row);
            session.releaseCache(row, RingBufferSensor.class);
        }
        tx.commit();

        // 8 inserts with ring_size=5: slots cycle 1,2,3,4,5,1,2,3
        // Final: slot 1=55005, slot 2=55006, slot 3=55007,
        //        slot 4=55003, slot 5=55004
        tx.begin();
        errorIfNotEqual("DTO cache batch slot 1", 55005L,
                session.find(RingBufferSensor.class, new Object[]{2700, 1}).getTimestampVal());
        errorIfNotEqual("DTO cache batch slot 2", 55006L,
                session.find(RingBufferSensor.class, new Object[]{2700, 2}).getTimestampVal());
        errorIfNotEqual("DTO cache batch slot 3", 55007L,
                session.find(RingBufferSensor.class, new Object[]{2700, 3}).getTimestampVal());
        errorIfNotEqual("DTO cache batch slot 4", 55003L,
                session.find(RingBufferSensor.class, new Object[]{2700, 4}).getTimestampVal());
        errorIfNotEqual("DTO cache batch slot 5", 55004L,
                session.find(RingBufferSensor.class, new Object[]{2700, 5}).getTimestampVal());
        tx.commit();
    }

    /**
     * Find (read) rows and release them to DTO cache, then insert new rows.
     * Verifies that a recycled DTO from a find() doesn't carry stale PK
     * or ring_idx values into a subsequent insert.
     */
    private void testDtoCacheReadAfterInsert() {
        cleanup();
        // Insert 3 rows
        tx.begin();
        for (int i = 0; i < 3; i++) {
            RingBufferSensor row = session.newInstance(RingBufferSensor.class);
            row.setSensorId(2800);
            row.setTimestampVal(56000L + i);
            row.setSensorValue(i);
            session.makePersistent(row);
        }
        tx.commit();

        // Read all 3 rows and release each to DTO cache
        tx.begin();
        for (int slot = 1; slot <= 3; slot++) {
            RingBufferSensor r = session.find(RingBufferSensor.class,
                    new Object[]{2800, slot});
            errorIfNotEqual("DTO cache read slot " + slot, 56000L + (slot - 1),
                    r.getTimestampVal());
            session.releaseCache(r, RingBufferSensor.class);
        }
        tx.commit();

        // Now insert 3 more rows using potentially recycled DTOs
        tx.begin();
        for (int i = 0; i < 3; i++) {
            RingBufferSensor row = session.newInstance(RingBufferSensor.class);
            row.setSensorId(2800);
            row.setTimestampVal(57000L + i);
            row.setSensorValue(i + 10.0);
            session.makePersistent(row);
            session.releaseCache(row, RingBufferSensor.class);
        }
        tx.commit();

        // 6 inserts: slot 1 wrapped
        tx.begin();
        errorIfNotEqual("DTO cache read+insert slot 1", 57002L,
                session.find(RingBufferSensor.class, new Object[]{2800, 1}).getTimestampVal());
        errorIfNotEqual("DTO cache read+insert slot 2", 56001L,
                session.find(RingBufferSensor.class, new Object[]{2800, 2}).getTimestampVal());
        errorIfNotEqual("DTO cache read+insert slot 3", 56002L,
                session.find(RingBufferSensor.class, new Object[]{2800, 3}).getTimestampVal());
        errorIfNotEqual("DTO cache read+insert slot 4", 57000L,
                session.find(RingBufferSensor.class, new Object[]{2800, 4}).getTimestampVal());
        errorIfNotEqual("DTO cache read+insert slot 5", 57001L,
                session.find(RingBufferSensor.class, new Object[]{2800, 5}).getTimestampVal());
        tx.commit();
    }

    /**
     * Same as testDtoCacheInsert but using release() instead of releaseCache().
     * Control test to confirm behavior is identical without DTO caching.
     */
    private void testDtoCacheWithoutCache() {
        cleanup();
        tx.begin();
        for (int i = 0; i < 3; i++) {
            RingBufferSensor row = session.newInstance(RingBufferSensor.class);
            row.setSensorId(2900);
            row.setTimestampVal(58000L + i);
            row.setSensorValue(i);
            session.makePersistent(row);
            session.release(row);
        }
        tx.commit();

        // Insert 3 more with non-cached release
        tx.begin();
        for (int i = 0; i < 3; i++) {
            RingBufferSensor row = session.newInstance(RingBufferSensor.class);
            row.setSensorId(2900);
            row.setTimestampVal(59000L + i);
            row.setSensorValue(i + 10.0);
            session.makePersistent(row);
            session.release(row);
        }
        tx.commit();

        // 6 inserts: slot 1 wrapped
        tx.begin();
        errorIfNotEqual("DTO no-cache slot 1", 59002L,
                session.find(RingBufferSensor.class, new Object[]{2900, 1}).getTimestampVal());
        errorIfNotEqual("DTO no-cache slot 2", 58001L,
                session.find(RingBufferSensor.class, new Object[]{2900, 2}).getTimestampVal());
        errorIfNotEqual("DTO no-cache slot 3", 58002L,
                session.find(RingBufferSensor.class, new Object[]{2900, 3}).getTimestampVal());
        errorIfNotEqual("DTO no-cache slot 4", 59000L,
                session.find(RingBufferSensor.class, new Object[]{2900, 4}).getTimestampVal());
        errorIfNotEqual("DTO no-cache slot 5", 59001L,
                session.find(RingBufferSensor.class, new Object[]{2900, 5}).getTimestampVal());
        tx.commit();
    }

    // =======================================================================
    // P5: Concurrency
    // =======================================================================

    private static final int NUM_THREADS = 4;
    private static final int INSERTS_PER_THREAD = 10;

    /**
     * Multiple threads insert into different PK prefixes concurrently.
     * Each thread owns its own sensor_id so there is no meta row contention.
     * Each thread's ring should be independently correct.
     */
    private void testConcurrentDifferentPrefixes() {
        cleanup();
        final int baseSensorId = 3000;
        final CountDownLatch startLatch = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<Thread>();

        for (int t = 0; t < NUM_THREADS; t++) {
            final int threadIdx = t;
            Thread thread = new Thread(new Runnable() {
                public void run() {
                    try {
                        startLatch.await();
                    } catch (InterruptedException e) {
                        return;
                    }
                    int sensorId = baseSensorId + threadIdx;
                    Session s = sessionFactory.getSession();
                    try {
                        for (int i = 0; i < INSERTS_PER_THREAD; i++) {
                            Transaction t = s.currentTransaction();
                            t.begin();
                            RingBufferSensor row = s.newInstance(RingBufferSensor.class);
                            row.setSensorId(sensorId);
                            row.setTimestampVal(60000L + threadIdx * 1000 + i);
                            row.setSensorValue(i);
                            s.makePersistent(row);
                            t.commit();
                        }
                    } catch (Throwable ex) {
                        error("Concurrent different prefixes thread " + threadIdx
                                + ": " + ex.getMessage());
                    } finally {
                        s.close();
                    }
                }
            });
            threads.add(thread);
            thread.start();
        }

        startLatch.countDown();  // release all threads
        joinThreads(threads);

        // Verify each thread's ring independently
        // 10 inserts with ring_size=5: last 5 inserts survive
        tx.begin();
        for (int t = 0; t < NUM_THREADS; t++) {
            int sensorId = baseSensorId + t;
            for (int slot = 1; slot <= RING_SIZE; slot++) {
                RingBufferSensor r = session.find(RingBufferSensor.class,
                        new Object[]{sensorId, slot});
                errorIfNotEqual("Concurrent diff prefix sensor " + sensorId
                        + " slot " + slot + " not null", true, r != null);
            }
            // Last 5 inserts: indices 5..9 map to slots that wrap
            // After 10 inserts: slot layout is insert 5→s1, 6→s2, 7→s3, 8→s4, 9→s5
            for (int slot = 1; slot <= RING_SIZE; slot++) {
                long expectedTs = 60000L + t * 1000
                        + (INSERTS_PER_THREAD - RING_SIZE) + (slot - 1);
                RingBufferSensor r = session.find(RingBufferSensor.class,
                        new Object[]{sensorId, slot});
                errorIfNotEqual("Concurrent diff prefix sensor " + sensorId
                        + " slot " + slot + " timestamp", expectedTs,
                        r.getTimestampVal());
            }
        }
        tx.commit();
    }

    /**
     * Multiple threads insert into the SAME PK prefix concurrently.
     * The meta row exclusive lock serializes them. After all threads
     * finish, verify: exactly ring_size data rows exist and the total
     * insert count equals NUM_THREADS * INSERTS_PER_THREAD.
     */
    private void testConcurrentSamePrefix() {
        cleanup();
        final int sensorId = 3100;
        final CountDownLatch startLatch = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<Thread>();

        for (int t = 0; t < NUM_THREADS; t++) {
            final int threadIdx = t;
            Thread thread = new Thread(new Runnable() {
                public void run() {
                    try {
                        startLatch.await();
                    } catch (InterruptedException e) {
                        return;
                    }
                    try {
                        for (int i = 0; i < INSERTS_PER_THREAD; i++) {
                            // Retry loop: concurrent first-inserts for the same
                            // PK prefix can fail with NDB error 630 (duplicate
                            // meta row).  This is expected — NDB cannot lock a
                            // non-existent row, so two transactions may both see
                            // 626 and race on insertTuple.  Retry with a fresh
                            // session like a real application should.
                            boolean inserted = false;
                            for (int attempt = 0; attempt < 3 && !inserted; attempt++) {
                                Session s = sessionFactory.getSession();
                                try {
                                    Transaction t = s.currentTransaction();
                                    t.begin();
                                    RingBufferSensor row = s.newInstance(RingBufferSensor.class);
                                    row.setSensorId(sensorId);
                                    row.setTimestampVal(70000L + threadIdx * 1000 + i);
                                    row.setSensorValue(threadIdx * 100 + i);
                                    s.makePersistent(row);
                                    t.commit();
                                    inserted = true;
                                } catch (ClusterJException ex) {
                                    if (attempt == 2) {
                                        throw ex;
                                    }
                                    // retry with a fresh session
                                } finally {
                                    s.close();
                                }
                            }
                        }
                    } catch (Throwable ex) {
                        error("Concurrent same prefix thread " + threadIdx
                                + ": " + ex.getMessage());
                    }
                }
            });
            threads.add(thread);
            thread.start();
        }

        startLatch.countDown();
        joinThreads(threads);

        // Verify: all 5 slots should have data with valid timestamps.
        // 4 threads write ts in ranges [70000..70009], [71000..71009],
        // [72000..72009], [73000..73009]. Thread ordering is non-deterministic,
        // but every slot must have a timestamp from one of those ranges.
        tx.begin();
        int dataRowCount = 0;
        for (int slot = 1; slot <= RING_SIZE; slot++) {
            RingBufferSensor r = session.find(RingBufferSensor.class,
                    new Object[]{sensorId, slot});
            errorIfNotEqual("Concurrent same prefix slot " + slot + " not null",
                    true, r != null);
            if (r != null) {
                dataRowCount++;
                errorIfNotEqual("Concurrent same prefix slot " + slot
                        + " sensor_id", sensorId, r.getSensorId());
                long ts = r.getTimestampVal();
                // Timestamp must fall in one of the 4 thread ranges
                int threadOfTs = (int)((ts - 70000L) / 1000);
                int insertOfTs = (int)((ts - 70000L) % 1000);
                errorIfNotEqual("Concurrent same prefix slot " + slot
                        + " ts in valid thread range (0-3)",
                        true, threadOfTs >= 0 && threadOfTs < NUM_THREADS);
                errorIfNotEqual("Concurrent same prefix slot " + slot
                        + " ts in valid insert range (0-9)",
                        true, insertOfTs >= 0 && insertOfTs < INSERTS_PER_THREAD);
                // sensor_value = threadIdx * 100 + i, verify consistency with ts
                double expectedVal = threadOfTs * 100.0 + insertOfTs;
                errorIfNotEqual("Concurrent same prefix slot " + slot
                        + " sensor_value matches timestamp",
                        expectedVal, r.getSensorValue());
            }
        }
        errorIfNotEqual("Concurrent same prefix: all slots filled",
                RING_SIZE, dataRowCount);
        tx.commit();

        // Direct ClusterJ find for meta row (ring_idx=0)
        // Note: ClusterJ find doesn't set ring_buffer_op, so kernel
        // filters meta rows. NOT FOUND here is expected.
        tx.begin();
        RingBufferSensor metaDirect = session.find(RingBufferSensor.class,
                new Object[]{sensorId, 0});
        System.out.println("[CONCURRENT-META] ClusterJ find(ring_idx=0) = "
                + (metaDirect != null ? "FOUND" : "NOT FOUND (expected)"));
        errorIfNotEqual("Concurrent same prefix: meta row NOT visible via find()",
                null, metaDirect);
        tx.commit();

        // Use RingBufferWriter's readMetaRow (has OO_RING_BUFFER_OP + LM_Exclusive)
        // to check if meta row exists at NDB level
        {
            Session freshSession = sessionFactory.getSession();
            Transaction freshTx = freshSession.currentTransaction();
            freshTx.begin();
            RingBufferSensor probe = freshSession.newInstance(RingBufferSensor.class);
            probe.setSensorId(sensorId);
            probe.setTimestampVal(99999L);
            probe.setSensorValue(0.0);
            try {
                freshSession.makePersistent(probe);
                // If meta row exists, this INSERT into ring buffer succeeds
                // (readMetaRow finds it, writeDataRow queues, flushBatch updates meta)
                freshTx.commit();
                System.out.println("[CONCURRENT-META] RingBufferWriter probe: meta row EXISTS"
                        + " (insert at next slot succeeded)");
            } catch (ClusterJException ex) {
                System.out.println("[CONCURRENT-META] RingBufferWriter probe: FAILED - "
                        + ex.getMessage());
                if (freshTx.isActive()) freshTx.rollback();
            }
            freshSession.close();
        }

        // Normalize ring to known state for deterministic JDBC diagnostic
        // output. Concurrent correctness was already verified above.
        // Delete all and re-insert RING_SIZE rows with known values.
        cleanupViaSql();
        for (int i = 0; i < RING_SIZE; i++) {
            Session ns = sessionFactory.getSession();
            Transaction nt = ns.currentTransaction();
            nt.begin();
            RingBufferSensor row = ns.newInstance(RingBufferSensor.class);
            row.setSensorId(sensorId);
            row.setTimestampVal(90000L + i);
            row.setSensorValue(i * 1.0);
            ns.makePersistent(row);
            nt.commit();
            ns.close();
        }

        // Verify meta row visible via SQL with show_meta=1.
        // Use a scan-based query (SELECT with WHERE), NOT COUNT(*),
        // because COUNT(*) uses HA_COUNT_ROWS_INSTANT which reads NDB
        // stats directly and bypasses the scan-level meta row filter.
        try {
            getConnection();
            Statement stmt = connection.createStatement();

            // DIAGNOSTIC 1: FLUSH TABLES to force table descriptor refresh
            // If the meta row becomes visible after this, the bug is
            // stale NDB table descriptor cache in mysqld.
            System.out.println("[DIAG] Flushing tables before show_meta check...");
            stmt.execute("FLUSH TABLES");

            stmt.execute("SET ndb_ring_buffer_show_meta = 1");
            // Verify the SET took effect
            ResultSet chk = stmt.executeQuery(
                    "SELECT @@ndb_ring_buffer_show_meta AS val");
            if (chk.next()) {
                System.out.println("[DEBUG] ndb_ring_buffer_show_meta = "
                        + chk.getInt("val"));
            }
            chk.close();
            // Check the execution plan
            ResultSet expl = stmt.executeQuery(
                    "EXPLAIN SELECT sensor_id, ring_idx, timestamp_val,"
                    + " sensor_value FROM ring_buffer_sensor"
                    + " WHERE sensor_id = " + sensorId);
            while (expl.next()) {
                System.out.println("[DEBUG] EXPLAIN: type="
                        + expl.getString("type")
                        + " key=" + expl.getString("key")
                        + " rows=" + expl.getString("rows")
                        + " Extra=" + expl.getString("Extra"));
            }
            expl.close();
            ResultSet rs = stmt.executeQuery(
                    "SELECT sensor_id, ring_idx, timestamp_val, sensor_value"
                    + " FROM ring_buffer_sensor WHERE sensor_id = "
                    + sensorId + " ORDER BY ring_idx");
            int totalRows = 0;
            boolean metaRowFound = false;
            while (rs.next()) {
                int rid = rs.getInt("ring_idx");
                System.out.println("[DEBUG] row: sensor_id="
                        + rs.getInt("sensor_id")
                        + " ring_idx=" + rid
                        + " ts=" + rs.getLong("timestamp_val")
                        + " val=" + rs.getDouble("sensor_value"));
                totalRows++;
                if (rid == 0) {
                    metaRowFound = true;
                }
            }
            rs.close();
            System.out.println("[DEBUG] totalRows=" + totalRows
                    + " metaRowFound=" + metaRowFound);
            // Direct PK lookup for meta row (ring_idx=0)
            ResultSet pkrs = stmt.executeQuery(
                    "SELECT sensor_id, ring_idx, timestamp_val"
                    + " FROM ring_buffer_sensor"
                    + " WHERE sensor_id = " + sensorId
                    + " AND ring_idx = 0");
            if (pkrs.next()) {
                System.out.println("[DEBUG] PK lookup meta row FOUND: "
                        + "sensor_id=" + pkrs.getInt("sensor_id")
                        + " ring_idx=" + pkrs.getInt("ring_idx")
                        + " ts=" + pkrs.getLong("timestamp_val"));
            } else {
                System.out.println("[DEBUG] PK lookup meta row NOT FOUND");
            }
            pkrs.close();
            // Check meta row for a simpler test's sensorId (1000 = testSingleInsert)
            ResultSet pkrs2 = stmt.executeQuery(
                    "SELECT sensor_id, ring_idx"
                    + " FROM ring_buffer_sensor"
                    + " WHERE sensor_id = 1000"
                    + " AND ring_idx = 0");
            System.out.println("[DEBUG] sensorId=1000 meta row: "
                    + (pkrs2.next() ? "FOUND" : "NOT FOUND"));
            pkrs2.close();
            stmt.execute("SET ndb_ring_buffer_show_meta = 0");
            stmt.close();
            errorIfNotEqual(
                    "Concurrent same prefix: total rows with meta (show_meta=1)",
                    RING_SIZE + 1, totalRows);
            errorIfNotEqual(
                    "Concurrent same prefix: meta row found (show_meta=1)",
                    true, metaRowFound);
        } catch (Exception ex) {
            error("Concurrent same prefix SQL meta check: " + ex.getMessage());
        }

        // DIAGNOSTIC 2: Check meta row from a DIFFERENT mysqld.
        // If meta row is visible here but not above, the bug is
        // per-mysqld state contamination from ClusterJ's NDB connections.
        try {
            // Derive second mysqld URL by replacing the port in jdbcURL.
            // jdbcURL = "jdbc:mysql://localhost:<port>/test"
            // We need to connect to mysqld.2.1 instead of mysqld.1.1.
            String url2 = jdbcURL.replaceFirst(
                    ":\\d+/", ":" + System.getProperty("mysqld2.port", "13001") + "/");
            System.out.println("[DIAG] Checking meta row from second mysqld: " + url2);
            Connection conn2 = DriverManager.getConnection(url2, "root", "");
            Statement stmt2 = conn2.createStatement();
            stmt2.execute("SET ndb_ring_buffer_show_meta = 1");
            ResultSet rs2 = stmt2.executeQuery(
                    "SELECT sensor_id, ring_idx, timestamp_val, sensor_value"
                    + " FROM ring_buffer_sensor WHERE sensor_id = "
                    + sensorId + " ORDER BY ring_idx");
            int totalRows2 = 0;
            boolean metaRowFound2 = false;
            while (rs2.next()) {
                int rid = rs2.getInt("ring_idx");
                System.out.println("[DIAG-MYSQLD2] row: sensor_id="
                        + rs2.getInt("sensor_id")
                        + " ring_idx=" + rid
                        + " ts=" + rs2.getLong("timestamp_val")
                        + " val=" + rs2.getDouble("sensor_value"));
                totalRows2++;
                if (rid == 0) {
                    metaRowFound2 = true;
                }
            }
            rs2.close();
            stmt2.execute("SET ndb_ring_buffer_show_meta = 0");
            stmt2.close();
            conn2.close();
            System.out.println("[DIAG-MYSQLD2] totalRows=" + totalRows2
                    + " metaRowFound=" + metaRowFound2);
            errorIfNotEqual(
                    "MYSQLD2: total rows with meta (show_meta=1)",
                    RING_SIZE + 1, totalRows2);
            errorIfNotEqual(
                    "MYSQLD2: meta row found (show_meta=1)",
                    true, metaRowFound2);
        } catch (Exception ex) {
            System.out.println("[DIAG-MYSQLD2] FAILED to connect: " + ex.getMessage());
        }

    }

    /**
     * Multiple threads use session cache (closeCache/getSession) while
     * inserting into different PK prefixes. Combines concurrency with
     * session recycling.
     */
    private void testConcurrentWithSessionCache() {
        cleanup();
        final int baseSensorId = 3200;
        final CountDownLatch startLatch = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<Thread>();

        for (int t = 0; t < NUM_THREADS; t++) {
            final int threadIdx = t;
            Thread thread = new Thread(new Runnable() {
                public void run() {
                    try {
                        startLatch.await();
                    } catch (InterruptedException e) {
                        return;
                    }
                    int sensorId = baseSensorId + threadIdx;
                    try {
                        // Cycle through cache: get session, insert, return to cache
                        for (int i = 0; i < INSERTS_PER_THREAD; i++) {
                            Session s = sessionFactory.getSession();
                            Transaction t = s.currentTransaction();
                            t.begin();
                            RingBufferSensor row = s.newInstance(RingBufferSensor.class);
                            row.setSensorId(sensorId);
                            row.setTimestampVal(80000L + threadIdx * 1000 + i);
                            row.setSensorValue(i);
                            s.makePersistent(row);
                            t.commit();
                            s.closeCache();  // return to pool
                        }
                    } catch (Throwable ex) {
                        error("Concurrent session cache thread " + threadIdx
                                + ": " + ex.getMessage());
                    }
                }
            });
            threads.add(thread);
            thread.start();
        }

        startLatch.countDown();
        joinThreads(threads);

        // Verify each thread's ring
        tx.begin();
        for (int t = 0; t < NUM_THREADS; t++) {
            int sensorId = baseSensorId + t;
            for (int slot = 1; slot <= RING_SIZE; slot++) {
                long expectedTs = 80000L + t * 1000
                        + (INSERTS_PER_THREAD - RING_SIZE) + (slot - 1);
                RingBufferSensor r = session.find(RingBufferSensor.class,
                        new Object[]{sensorId, slot});
                errorIfNotEqual("Concurrent cache sensor " + sensorId
                        + " slot " + slot + " not null", true, r != null);
                errorIfNotEqual("Concurrent cache sensor " + sensorId
                        + " slot " + slot + " timestamp", expectedTs,
                        r.getTimestampVal());
            }
        }
        tx.commit();
    }

    // =======================================================================
    // Helpers
    // =======================================================================

    private void joinThreads(List<Thread> threads) {
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                error("Interrupted while joining thread: " + e.getMessage());
            }
        }
    }

    /** Clean up ring buffer table via SQL (NDB API delete needs OO_RING_BUFFER_OP). */
    private void cleanup() {
        cleanupViaSql();
    }

    private void cleanupViaSql() {
        try {
            getConnection();
            Statement stmt = connection.createStatement();
            stmt.execute("DELETE FROM ring_buffer_sensor");
            stmt.close();
        } catch (Throwable t) {
            // ignore - table might be empty
        }
    }
}
