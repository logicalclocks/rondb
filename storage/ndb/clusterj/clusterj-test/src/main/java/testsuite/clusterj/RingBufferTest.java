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
import java.util.List;

import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.Query;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import com.mysql.clusterj.query.Predicate;
import com.mysql.clusterj.query.PredicateOperand;

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
        // P0: existing core tests
        testSingleInsert();
        testFillRing();
        testMultiplePrefixes();
        testBatchInsert();
        testCrossTransactionContinuity();

        // P0: production-critical gaps
        testBatchExceedingRingSize();
        testMixedPrefixesSingleTransaction();
        testInterleavedPrefixesSingleTransaction();
        testTransactionRollback();
        testDeleteViaClusterJ();
        testUpdateViaClusterJ();

        // P1: correctness verification
        testExactRingFill();
        testMultipleWrapCycles();
        testNullUserColumns();
        testFindNonExistentSlot();
        testMetaRowVisibility();
        testQueryScan();

        // P2: edge cases and hardening
        testSessionReuseAcrossTransactions();
        testEmptyTransaction();
        testBatchExceedingRingSizeMultipleWraps();

        failOnError();
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
        // Slot 1: insert 5 (ts=2005), Slot 2: insert 6 (ts=2006),
        // Slot 3: insert 2 (ts=2002), Slot 4: insert 3 (ts=2003),
        // Slot 5: insert 4 (ts=2004)
        tx.begin();
        errorIfNotEqual("Fill ring: slot 1 timestamp", 2005L,
                session.find(RingBufferSensor.class, new Object[]{200, 1}).getTimestampVal());
        errorIfNotEqual("Fill ring: slot 2 timestamp", 2006L,
                session.find(RingBufferSensor.class, new Object[]{200, 2}).getTimestampVal());
        errorIfNotEqual("Fill ring: slot 3 timestamp", 2002L,
                session.find(RingBufferSensor.class, new Object[]{200, 3}).getTimestampVal());
        errorIfNotEqual("Fill ring: slot 4 timestamp", 2003L,
                session.find(RingBufferSensor.class, new Object[]{200, 4}).getTimestampVal());
        errorIfNotEqual("Fill ring: slot 5 timestamp", 2004L,
                session.find(RingBufferSensor.class, new Object[]{200, 5}).getTimestampVal());
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
            errorIfNotEqual("Multi-prefix 301 slot " + slot + " timestamp",
                    3000L + (slot - 1), r.getTimestampVal());
        }
        // Verify sensor 302 has 2 rows at slots 1-2
        for (int slot = 1; slot <= 2; slot++) {
            RingBufferSensor r = session.find(RingBufferSensor.class, new Object[]{302, slot});
            errorIfNotEqual("Multi-prefix 302 slot " + slot + " timestamp",
                    4000L + (slot - 1), r.getTimestampVal());
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
            errorIfNotEqual("Batch insert slot " + slot + " timestamp",
                    5000L + (slot - 1), r.getTimestampVal());
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
        // Insert 0: slot 1 (ts=6000), Insert 1: slot 2 (ts=6001), Insert 2: slot 3 (ts=6002)
        // Insert 3: slot 4 (ts=7000), Insert 4: slot 5 (ts=7001), Insert 5: slot 1 (ts=7002)
        tx.begin();
        errorIfNotEqual("Cross-tx: slot 1 should have last wrap", 7002L,
                session.find(RingBufferSensor.class, new Object[]{500, 1}).getTimestampVal());
        errorIfNotEqual("Cross-tx: slot 2 timestamp", 6001L,
                session.find(RingBufferSensor.class, new Object[]{500, 2}).getTimestampVal());
        errorIfNotEqual("Cross-tx: slot 3 timestamp", 6002L,
                session.find(RingBufferSensor.class, new Object[]{500, 3}).getTimestampVal());
        errorIfNotEqual("Cross-tx: slot 4 timestamp", 7000L,
                session.find(RingBufferSensor.class, new Object[]{500, 4}).getTimestampVal());
        errorIfNotEqual("Cross-tx: slot 5 timestamp", 7001L,
                session.find(RingBufferSensor.class, new Object[]{500, 5}).getTimestampVal());
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
        // Slot 1: insert 5 (ts=10005), Slot 2: insert 6 (ts=10006),
        // Slot 3: insert 7 (ts=10007), Slot 4: insert 3 (ts=10003),
        // Slot 5: insert 4 (ts=10004)
        tx.begin();
        errorIfNotEqual("Batch>ring slot 1", 10005L,
                session.find(RingBufferSensor.class, new Object[]{600, 1}).getTimestampVal());
        errorIfNotEqual("Batch>ring slot 2", 10006L,
                session.find(RingBufferSensor.class, new Object[]{600, 2}).getTimestampVal());
        errorIfNotEqual("Batch>ring slot 3", 10007L,
                session.find(RingBufferSensor.class, new Object[]{600, 3}).getTimestampVal());
        errorIfNotEqual("Batch>ring slot 4", 10003L,
                session.find(RingBufferSensor.class, new Object[]{600, 4}).getTimestampVal());
        errorIfNotEqual("Batch>ring slot 5", 10004L,
                session.find(RingBufferSensor.class, new Object[]{600, 5}).getTimestampVal());
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
            errorIfNotEqual("Mixed-prefix 701 slot " + slot,
                    11000L + (slot - 1), r.getTimestampVal());
        }
        for (int slot = 1; slot <= 2; slot++) {
            RingBufferSensor r = session.find(RingBufferSensor.class, new Object[]{702, slot});
            errorIfNotEqual("Mixed-prefix 702 slot " + slot,
                    12000L + (slot - 1), r.getTimestampVal());
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
        // First batch: slot 1 (ts=13000), slot 2 (ts=13001)
        // Second batch: slot 3 (ts=15000), slot 4 (ts=15001)
        tx.begin();
        errorIfNotEqual("Interleaved 801 slot 1", 13000L,
                session.find(RingBufferSensor.class, new Object[]{801, 1}).getTimestampVal());
        errorIfNotEqual("Interleaved 801 slot 2", 13001L,
                session.find(RingBufferSensor.class, new Object[]{801, 2}).getTimestampVal());
        errorIfNotEqual("Interleaved 801 slot 3", 15000L,
                session.find(RingBufferSensor.class, new Object[]{801, 3}).getTimestampVal());
        errorIfNotEqual("Interleaved 801 slot 4", 15001L,
                session.find(RingBufferSensor.class, new Object[]{801, 4}).getTimestampVal());

        // Sensor 802: 2 inserts -> slots 1-2
        errorIfNotEqual("Interleaved 802 slot 1", 14000L,
                session.find(RingBufferSensor.class, new Object[]{802, 1}).getTimestampVal());
        errorIfNotEqual("Interleaved 802 slot 2", 14001L,
                session.find(RingBufferSensor.class, new Object[]{802, 2}).getTimestampVal());
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
        errorIfNotEqual("Rollback: slot 1 preserved", 16000L,
                session.find(RingBufferSensor.class, new Object[]{900, 1}).getTimestampVal());
        errorIfNotEqual("Rollback: slot 2 preserved", 16001L,
                session.find(RingBufferSensor.class, new Object[]{900, 2}).getTimestampVal());
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
        errorIfNotEqual("Rollback recovery: slot 3 timestamp", 18000L, r.getTimestampVal());
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
        // Last 5 inserts are 20-24 (ts=22020..22024).
        // 25 % 5 = 0, so nextPos wraps back to 1 after the 25th insert.
        // Insert 20 (idx 20): slot (20%5)+1=1, Insert 21: slot 2, ..., Insert 24: slot 5
        tx.begin();
        for (int slot = 1; slot <= RING_SIZE; slot++) {
            long expectedTs = 22000L + (totalInserts - RING_SIZE) + (slot - 1);
            RingBufferSensor r = session.find(RingBufferSensor.class,
                    new Object[]{1300, slot});
            errorIfNotEqual("Multi-wrap slot " + slot + " timestamp",
                    expectedTs, r.getTimestampVal());
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

        // Try to find the meta row at ring_idx=0
        tx.begin();
        RingBufferSensor metaRow = session.find(RingBufferSensor.class,
                new Object[]{1600, 0});
        if (metaRow != null) {
            // Meta row is visible -- verify its structure
            errorIfNotEqual("Meta row: ring_idx should be 0", 0, metaRow.getRingIdx());
            errorIfNotEqual("Meta row: sensor_id should match", 1600, metaRow.getSensorId());
            // User columns in meta row should be zeroed (NOT NULL columns)
            // or null/default (nullable columns)
            errorIfNotEqual("Meta row: timestamp_val should be 0", 0L,
                    metaRow.getTimestampVal());
            errorIfNotEqual("Meta row: sensor_value should be 0.0", 0.0,
                    metaRow.getSensorValue());
        }
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

        // Should return 3 data rows (meta row may or may not be included)
        int dataRowCount = 0;
        boolean[] slotsFound = new boolean[RING_SIZE + 1]; // index 1..5
        for (RingBufferSensor r : results) {
            if (r.getRingIdx() > 0) {
                dataRowCount++;
                errorIfNotEqual("Query scan 1700: sensor_id", 1700, r.getSensorId());
                int slot = r.getRingIdx();
                errorIfNotEqual("Query scan 1700 slot " + slot + " timestamp",
                        26000L + (slot - 1), r.getTimestampVal());
                slotsFound[slot] = true;
            }
        }
        errorIfNotEqual("Query scan: data row count for sensor 1700", 3, dataRowCount);
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

        dataRowCount = 0;
        for (RingBufferSensor r : results) {
            if (r.getRingIdx() > 0) {
                dataRowCount++;
                // All matching rows should be from sensor 1701
                errorIfNotEqual("Query scan ts>=27000: sensor_id", 1701, r.getSensorId());
                int slot = r.getRingIdx();
                errorIfNotEqual("Query scan ts>=27000 slot " + slot + " timestamp",
                        27000L + (slot - 1), r.getTimestampVal());
            }
        }
        errorIfNotEqual("Query scan: rows with ts >= 27000", 2, dataRowCount);
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
        // Final: Slot 1=i10(32010), Slot 2=i11(32011), Slot 3=i12(32012),
        //        Slot 4=i8(32008), Slot 5=i9(32009)
        tx.begin();
        errorIfNotEqual("Multi-wrap batch slot 1", 32010L,
                session.find(RingBufferSensor.class, new Object[]{2000, 1}).getTimestampVal());
        errorIfNotEqual("Multi-wrap batch slot 2", 32011L,
                session.find(RingBufferSensor.class, new Object[]{2000, 2}).getTimestampVal());
        errorIfNotEqual("Multi-wrap batch slot 3", 32012L,
                session.find(RingBufferSensor.class, new Object[]{2000, 3}).getTimestampVal());
        errorIfNotEqual("Multi-wrap batch slot 4", 32008L,
                session.find(RingBufferSensor.class, new Object[]{2000, 4}).getTimestampVal());
        errorIfNotEqual("Multi-wrap batch slot 5", 32009L,
                session.find(RingBufferSensor.class, new Object[]{2000, 5}).getTimestampVal());
        tx.commit();
    }

    // =======================================================================
    // Helpers
    // =======================================================================

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
