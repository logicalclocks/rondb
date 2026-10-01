/*
 *  Copyright (c) 2026, Hopsworks and/or its affiliates.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, version 2.0,
 *  as published by the Free Software Foundation.
 *
 *  This program is designed to work with certain software (including
 *  but not limited to OpenSSL) that is licensed under separate terms,
 *  as designated in a particular file or component or in included license
 *  documentation.  The authors of MySQL hereby grant you an additional
 *  permission to link the program and your derivative works with the
 *  separately licensed software that they have either included with
 *  the program or referenced in the documentation.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License, version 2.0, for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package com.mysql.clusterj.tie;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import com.mysql.clusterj.ClusterJDatastoreException;
import com.mysql.clusterj.ClusterJUserException;
import com.mysql.clusterj.ColumnType;
import com.mysql.clusterj.core.store.Column;
import com.mysql.clusterj.core.store.Table;

import com.mysql.clusterj.core.util.I18NHelper;
import com.mysql.clusterj.core.util.Logger;
import com.mysql.clusterj.core.util.LoggerFactoryService;

import com.mysql.ndbjtie.ndbapi.NdbOperation;
import com.mysql.ndbjtie.ndbapi.NdbOperationConst;
import com.mysql.ndbjtie.ndbapi.NdbOperationConst.AbortOption;

/**
 * Java port of the C++ NdbRingBufferWriter.
 *
 * Manages the ring buffer INSERT protocol for ring buffer tables:
 * reads the meta row (ring_idx=0) with exclusive lock, computes the
 * next slot, writes data rows with the correct ring_idx, and
 * updates the meta row on flush.
 *
 * Usage from SessionImpl: call addRow() for each row, then flushBatch()
 * before commit.  The writer batches rows with the same PK prefix.
 */
class RingBufferWriter {

    static final I18NHelper local = I18NHelper.getInstance(RingBufferWriter.class);
    static final Logger logger = LoggerFactoryService.getFactory().getInstance(RingBufferWriter.class);

    private static final int ROW_NOT_FOUND = 626;

    // ---------------------------------------------------------------
    // Ring meta row format (32 bytes, stored in ring_meta VARBINARY)
    // ---------------------------------------------------------------
    static class RingMeta {
        static final short VERSION = 1;
        static final int SIZE = 32;

        short version;
        int nextPos;
        int count;
        long totalInserts;

        void initFirstInsert() {
            version = VERSION;
            nextPos = 1;
            count = 0;
            totalInserts = 0;
        }

        // Online ring-buffer resize is disabled (rejected authoritatively
        // in DBDICT, with ha_ndbcluster's parse_comment validator as a
        // front-end check), so post-grow stale-meta states cannot arise
        // here and the formula needs no grow-adjustment.
        void advance(int ringSize) {
            nextPos = (nextPos % ringSize) + 1;
            if (count < ringSize) count++;
            totalInserts++;
        }

        byte[] pack() {
            byte[] buf = new byte[SIZE];
            ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
            bb.putShort(0, version);
            bb.putShort(2, (short) 0);  // reserved
            bb.putInt(4, nextPos);
            bb.putInt(8, count);
            bb.putInt(12, 0);           // reserved
            bb.putLong(16, totalInserts);
            bb.putLong(24, 0L);         // reserved
            return buf;
        }

        /** Unpack an existing meta value. Returns false if the value is
         *  corrupt (NULL, too short, unknown version, or next_pos/count out
         *  of range for the ring) - the caller must fail rather than
         *  silently re-initialize, which would reset count/totalInserts and
         *  orphan every existing data row, or write at a slot outside the
         *  ring (next_pos 0 is the meta row itself). Mirrors
         *  NdbRingBufferWriter and the SQL handler (NDB error 4357). */
        boolean unpack(byte[] data, int ringSize) {
            if (data == null || data.length < SIZE) {
                return false;
            }
            ByteBuffer bb = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
            version = bb.getShort(0);
            nextPos = bb.getInt(4);
            count = bb.getInt(8);
            totalInserts = bb.getLong(16);
            return version == VERSION && nextPos >= 1 && nextPos <= ringSize
                    && count >= 0 && count <= ringSize;
        }
    }

    // ---------------------------------------------------------------
    // Fields
    // ---------------------------------------------------------------

    private final ClusterTransactionImpl trans;
    private final Table storeTable;
    private final NdbRecordImpl ndbRecordImpl;

    private final int ringBufferSize;
    private final Column ringIdxColumn;
    private final Column ringMetaColumn;
    private final int ringIdxColumnId;
    private final int ringMetaColumnId;

    /** PK columns excluding ring_idx */
    private final Column[] pkPrefixColumns;

    /** NOT NULL, non-blob, non-PK, non-ring columns (need zero-fill in meta row) */
    private final Column[] notNullColumns;

    /** TTL column of a TTL ring buffer table; null when the table has no TTL.
     *  The meta row carries the type maximum in it (see setTTLColumnMax). */
    private final Column ttlColumn;

    /** TTL ring buffer table: every column except ring_idx/ring_meta must be
     *  set in each row's mask (a slot write onto an occupied slot is an
     *  update; an unset column would keep the overwritten row's value, and a
     *  replica whose purge removed the slot could not rebuild the row).
     *  BLOB/TEXT columns are exempt: ClusterJ cannot write them on a ring
     *  buffer table at all (see NdbRecordOperationImpl), so on a wrap they
     *  keep the overwritten row's value; use SQL for such tables.
     *  Empty on a table without TTL. */
    private final Column[] requiredColumns;

    // Internal buffers (all from NdbRecordImpl buffer pool)
    private final ByteBuffer metaRowBuffer;
    private final ByteBuffer keyRowBuffer;
    private ByteBuffer pkPrefixBuffer;  // cached PK prefix for batch hit detection

    // Masks
    private final byte[] metaMask;    // columns to include in meta row read/write
    private final int maskByteSize;

    // Reusable OperationOptions (native allocations)
    private final NdbOperation.OperationOptions readOpts;
    private final NdbOperation.OperationOptions writeOpts;

    // Batch state
    private boolean batchActive = false;
    private boolean batchMetaExisted = false;
    private final RingMeta batchMeta = new RingMeta();
    private boolean closed = false;

    // ---------------------------------------------------------------
    // Constructor
    // ---------------------------------------------------------------

    RingBufferWriter(ClusterTransactionImpl trans, Table storeTable) {
        this.trans = trans;
        this.storeTable = storeTable;
        this.ndbRecordImpl = trans.getCachedNdbRecordImpl(trans.db, storeTable);

        if (!storeTable.isRingBuffer()) {
            throw new ClusterJUserException(
                    "Table " + storeTable.getName() + " is not a ring buffer table");
        }

        this.ringBufferSize = storeTable.getRingBufferSize();
        this.ringIdxColumn = storeTable.getRingIdxColumn();
        this.ringMetaColumn = storeTable.getRingMetaColumn();
        this.ringIdxColumnId = ringIdxColumn.getColumnId();
        this.ringMetaColumnId = ringMetaColumn.getColumnId();

        // Identify PK prefix columns (all PK except ring_idx)
        String[] pkNames = storeTable.getPrimaryKeyColumnNames();
        List<Column> pkPrefixList = new ArrayList<Column>();
        for (String name : pkNames) {
            Column col = storeTable.getColumn(name);
            if (col.getColumnId() != ringIdxColumnId) {
                pkPrefixList.add(col);
            }
        }
        this.pkPrefixColumns = pkPrefixList.toArray(new Column[0]);

        // Identify NOT NULL non-blob non-PK non-ring columns for meta row zero-fill
        String[] allColumnNames = storeTable.getColumnNames();
        List<Column> notNullList = new ArrayList<Column>();
        for (String name : allColumnNames) {
            Column col = storeTable.getColumn(name);
            if (col.isPrimaryKey()) continue;
            if (col.getColumnId() == ringMetaColumnId) continue;
            if (col.getNullable()) continue;
            if (col.isLob()) continue;
            notNullList.add(col);
        }
        this.notNullColumns = notNullList.toArray(new Column[0]);

        // TTL column (TTL ring buffer table): filled with the type maximum
        // in the meta row, nullable or not
        this.ttlColumn = storeTable.isTTLEnabled() ? storeTable.getTTLColumn() : null;
        List<Column> requiredList = new ArrayList<Column>();
        if (ttlColumn != null) {
            for (String name : allColumnNames) {
                Column col = storeTable.getColumn(name);
                int id = col.getColumnId();
                if (id == ringIdxColumnId || id == ringMetaColumnId) continue;
                if (col.isLob()) continue;
                requiredList.add(col);
            }
        }
        this.requiredColumns = requiredList.toArray(new Column[0]);

        // Allocate internal buffers
        this.metaRowBuffer = ndbRecordImpl.newBuffer();
        this.keyRowBuffer = ndbRecordImpl.newBuffer();
        this.pkPrefixBuffer = null; // allocated on first addRow

        // Build meta mask: all PK + ring_meta + NOT NULL non-blob columns
        this.maskByteSize = 1 + (ndbRecordImpl.getNumberOfColumns() / 8);
        this.metaMask = new byte[maskByteSize];
        // PK columns (prefix + ring_idx)
        for (String name : pkNames) {
            Column col = storeTable.getColumn(name);
            columnSet(metaMask, col.getColumnId());
        }
        // ring_meta
        columnSet(metaMask, ringMetaColumnId);
        // NOT NULL non-blob non-PK columns
        for (Column col : notNullColumns) {
            columnSet(metaMask, col.getColumnId());
        }
        // TTL column
        if (ttlColumn != null) {
            columnSet(metaMask, ttlColumn.getColumnId());
        }

        // Create reusable OperationOptions
        this.readOpts = NdbOperation.OperationOptions.create();
        readOpts.optionsPresent(
                NdbOperation.OperationOptionsConst.Flags.OO_RING_BUFFER_OP
                | NdbOperation.OperationOptionsConst.Flags.OO_ABORTOPTION);
        readOpts.abortOption(AbortOption.AO_IgnoreError);

        this.writeOpts = NdbOperation.OperationOptions.create();
        writeOpts.optionsPresent(NdbOperation.OperationOptionsConst.Flags.OO_RING_BUFFER_OP);
    }

    // ---------------------------------------------------------------
    // Public API
    // ---------------------------------------------------------------

    /**
     * Queue a row for insertion into the ring buffer.
     * The userRowBuffer must be an NdbRecord-layout buffer with user columns
     * (and PK prefix) filled in.  ring_idx and ring_meta are set by the writer.
     *
     * @param userRowBuffer NdbRecord buffer with user column values
     * @param userMask      column mask (user columns only, no ring_idx/ring_meta bits;
     *                      must include every other column on a TTL table)
     */
    void addRow(ByteBuffer userRowBuffer, byte[] userMask) {
        if (closed) {
            throw new ClusterJUserException("RingBufferWriter is closed");
        }
        // A slot write onto an occupied slot is an update: a column absent
        // from the mask keeps the overwritten row's value. On a TTL ring
        // buffer table every column is therefore mandatory: an inherited
        // expired TTL value would hide the new row, and a replica whose
        // purge removed the slot could not rebuild the row.
        for (Column col : requiredColumns) {
            if (!columnIsSet(userMask, col.getColumnId())) {
                throw new ClusterJUserException("Ring buffer insert on TTL table "
                        + storeTable.getName() + " must set every column; column "
                        + col.getName() + " is not set");
            }
        }

        // Check if PK prefix matches current batch
        if (batchActive && pkPrefixMatches(userRowBuffer)) {
            // Same PK prefix -- batch hit: just advance meta and write data
            writeDataRow(userRowBuffer, userMask);
            batchMeta.advance(ringBufferSize);
            return;
        }

        // Different PK prefix or first row -- flush old batch, start new
        flushBatch();
        readMetaRow(userRowBuffer);
        writeDataRow(userRowBuffer, userMask);
        batchMeta.advance(ringBufferSize);

        // Mark batch active AFTER writeDataRow succeeds (matches C++ ordering).
        // If writeDataRow throws, batchActive stays false so flush() is a no-op.
        batchActive = true;
    }

    /**
     * Flush any pending batch: writes the meta row and executes NoCommit.
     * Must be called after the last addRow() and before commit.
     */
    void flushBatch() {
        if (!batchActive) return;
        try {
            flushBatchInternal();
        } finally {
            /* A failed flush must not leave a stale batch behind: the
               commit-time flush would re-execute it on the aborted
               transaction, masking the original error. The C++ writer
               resets unconditionally too. */
            batchActive = false;
        }
    }

    private void flushBatchInternal() {
        // Build meta row in metaRowBuffer
        ndbRecordImpl.initializeBuffer(metaRowBuffer);

        // Copy PK prefix from pkPrefixBuffer
        copyPkPrefix(pkPrefixBuffer, metaRowBuffer);

        // Set ring_idx = 0 (meta row sentinel)
        ndbRecordImpl.setInt(metaRowBuffer, ringIdxColumn, 0);

        // Set ring_meta = packed meta data
        byte[] packedMeta = batchMeta.pack();
        ndbRecordImpl.setBytes(metaRowBuffer, ringMetaColumn, packedMeta);

        // Zero NOT NULL non-blob columns
        for (Column col : notNullColumns) {
            zeroColumn(metaRowBuffer, col);
        }

        // TTL ring buffer table: meta row TTL column = type maximum
        if (ttlColumn != null) {
            setTTLColumnMax(metaRowBuffer, ttlColumn);
        }

        metaRowBuffer.position(0);
        metaRowBuffer.limit(ndbRecordImpl.getBufferSize());

        // Insert or update meta row
        NdbOperationConst metaOp;
        if (batchMetaExisted) {
            metaOp = trans.updateTuple(
                    ndbRecordImpl.getNdbRecord(), metaRowBuffer, metaMask, writeOpts);
        } else {
            metaOp = trans.insertTuple(
                    ndbRecordImpl.getNdbRecord(), metaRowBuffer, metaMask, writeOpts);
        }

        if (metaOp == null) {
            throw new ClusterJDatastoreException(
                    "Failed to write meta row for ring buffer table " + storeTable.getName());
        }

        // Execute NoCommit to persist the meta + data rows
        int rc = trans.executeNoCommitDirect(AbortOption.AbortOnError);
        if (rc != 0) {
            int errCode = trans.getNdbTransaction().getNdbError().code();
            throw new ClusterJDatastoreException(
                    "Ring buffer flush failed for table " + storeTable.getName()
                    + ", NDB error " + errCode);
        }
    }

    /**
     * Release resources (OperationOptions, buffers).
     * Must be called when the writer is no longer needed.
     */
    void close() {
        if (closed) return;
        closed = true;

        NdbOperation.OperationOptions.delete(readOpts);
        NdbOperation.OperationOptions.delete(writeOpts);
        ndbRecordImpl.returnBuffer(metaRowBuffer);
        ndbRecordImpl.returnBuffer(keyRowBuffer);
        if (pkPrefixBuffer != null) {
            ndbRecordImpl.returnBuffer(pkPrefixBuffer);
        }
    }

    // ---------------------------------------------------------------
    // Private: meta row read
    // ---------------------------------------------------------------

    private void readMetaRow(ByteBuffer userRowBuffer) {
        // Build key buffer: PK prefix from user row + ring_idx=0
        ndbRecordImpl.initializeBuffer(keyRowBuffer);
        copyPkPrefix(userRowBuffer, keyRowBuffer);
        ndbRecordImpl.setInt(keyRowBuffer, ringIdxColumn, 0);
        keyRowBuffer.position(0);
        keyRowBuffer.limit(ndbRecordImpl.getBufferSize());

        // Prepare meta result buffer
        ndbRecordImpl.initializeBuffer(metaRowBuffer);
        metaRowBuffer.position(0);
        metaRowBuffer.limit(ndbRecordImpl.getBufferSize());

        // Read meta row with exclusive lock
        NdbOperationConst readOp = trans.readTupleExplicitLock(
                ndbRecordImpl.getNdbRecord(), keyRowBuffer,
                ndbRecordImpl.getNdbRecord(), metaRowBuffer,
                metaMask,
                NdbOperationConst.LockMode.LM_Exclusive,
                readOpts);

        if (readOp == null) {
            throw new ClusterJDatastoreException(
                    "Failed to read meta row for ring buffer table " + storeTable.getName());
        }

        // Execute the pending round. The meta read op itself carries
        // AO_IgnoreError (readOpts), so a missing meta row (626) does not
        // abort the transaction. Use DefaultAbortOption so every other
        // queued operation keeps its own abort option - an execute-level
        // AO_IgnoreError would silently swallow their real errors.
        int rc = trans.executeNoCommitDirect(AbortOption.DefaultAbortOption);
        if (rc != 0) {
            // Another operation in this round failed (the meta read's own
            // 626 is ignored per-op and cannot fail the execute).
            throw new ClusterJDatastoreException(
                    "Ring buffer meta read round failed for table "
                    + storeTable.getName() + ", NDB error "
                    + trans.getNdbTransaction().getNdbError().code());
        }

        // Check operation-level error
        int errorCode = readOp.getNdbError().code();

        if (errorCode == 0) {
            // Meta row exists -- unpack; a corrupt value must fail, not
            // silently re-initialize (NDB error 4357, see RingMeta.unpack)
            byte[] metaBytes = ndbRecordImpl.getBytes(metaRowBuffer, ringMetaColumnId);
            if (!batchMeta.unpack(metaBytes, ringBufferSize)) {
                throw new ClusterJDatastoreException(
                        "Corrupt ring_meta value in ring buffer meta row for table "
                        + storeTable.getName() + ", NDB error 4357");
            }
            batchMetaExisted = true;
        } else if (errorCode == ROW_NOT_FOUND) {
            // Meta row doesn't exist yet -- fresh ring.
            // The ignored 626 was recorded as the transaction error state;
            // an empty execute (nothing pending) clears it so later
            // executes start clean.
            trans.executeNoCommitDirect(AbortOption.DefaultAbortOption);
            batchMeta.initFirstInsert();
            batchMetaExisted = false;
        } else {
            throw new ClusterJDatastoreException(
                    "Ring buffer meta read failed for table " + storeTable.getName()
                    + ", NDB error " + errorCode);
        }

        // Cache PK prefix for batch hit detection
        if (pkPrefixBuffer == null) {
            pkPrefixBuffer = ndbRecordImpl.newBuffer();
        }
        ndbRecordImpl.initializeBuffer(pkPrefixBuffer);
        copyPkPrefix(userRowBuffer, pkPrefixBuffer);
    }

    // ---------------------------------------------------------------
    // Private: data row write
    // ---------------------------------------------------------------

    private void writeDataRow(ByteBuffer userRowBuffer, byte[] userMask) {
        // Set ring_idx to the computed slot in the user's buffer
        ndbRecordImpl.setInt(userRowBuffer, ringIdxColumn, batchMeta.nextPos);

        // Set ring_meta to NULL in data rows
        ndbRecordImpl.setNull(userRowBuffer, ringMetaColumn);

        // Build data mask: user mask + ring_idx + ring_meta bits
        byte[] dataMask = buildDataMask(userMask);

        userRowBuffer.position(0);
        userRowBuffer.limit(ndbRecordImpl.getBufferSize());

        // Write data row with OO_RING_BUFFER_OP
        NdbOperationConst dataOp = trans.writeTuple(
                ndbRecordImpl.getNdbRecord(), userRowBuffer, dataMask, writeOpts);

        if (dataOp == null) {
            throw new ClusterJDatastoreException(
                    "Failed to write data row for ring buffer table " + storeTable.getName());
        }
    }

    // ---------------------------------------------------------------
    // Private: PK prefix comparison
    // ---------------------------------------------------------------

    private boolean pkPrefixMatches(ByteBuffer rowBuffer) {
        if (pkPrefixBuffer == null) return false;

        for (Column col : pkPrefixColumns) {
            int columnId = col.getColumnId();
            int offset = ndbRecordImpl.offsets[columnId];
            int prefixLength = col.getPrefixLength();
            int totalLength = prefixLength + col.getSize();

            for (int i = 0; i < totalLength; i++) {
                if (rowBuffer.get(offset + i) != pkPrefixBuffer.get(offset + i)) {
                    return false;
                }
            }
        }
        return true;
    }

    // ---------------------------------------------------------------
    // Private: utilities
    // ---------------------------------------------------------------

    private void copyPkPrefix(ByteBuffer src, ByteBuffer dst) {
        for (Column col : pkPrefixColumns) {
            int columnId = col.getColumnId();
            int offset = ndbRecordImpl.offsets[columnId];
            int prefixLength = col.getPrefixLength();
            // Use getSize() for the byte size of the column data (e.g. 4 for INT).
            // ndbRecordImpl.lengths[] stores Column.getLength() which is the
            // element count (1 for scalar INT), NOT the byte count.
            int totalLength = prefixLength + col.getSize();

            for (int i = 0; i < totalLength; i++) {
                dst.put(offset + i, src.get(offset + i));
            }

            // Copy null bit state if nullable
            if (col.getNullable()) {
                int nullByteOffset = ndbRecordImpl.nullbitByteOffset[columnId];
                int nullBitInByte = ndbRecordImpl.nullbitBitInByte[columnId];
                byte srcByte = src.get(nullByteOffset);
                byte dstByte = dst.get(nullByteOffset);
                byte mask = NdbRecordImpl.BIT_IN_BYTE_MASK[nullBitInByte];
                // Copy just this bit
                dstByte = (byte) ((dstByte & ~mask) | (srcByte & mask));
                dst.put(nullByteOffset, dstByte);
            }
        }
    }

    private byte[] buildDataMask(byte[] userMask) {
        byte[] dataMask = new byte[maskByteSize];
        // Copy user mask
        int copyLen = Math.min(userMask.length, maskByteSize);
        System.arraycopy(userMask, 0, dataMask, 0, copyLen);
        // Add ring_idx and ring_meta bits
        columnSet(dataMask, ringIdxColumnId);
        columnSet(dataMask, ringMetaColumnId);
        // Ensure ring_idx bit is set (it's a PK column the user may not have set)
        return dataMask;
    }

    private void zeroColumn(ByteBuffer buffer, Column col) {
        int columnId = col.getColumnId();
        int offset = ndbRecordImpl.offsets[columnId];
        int prefixLength = col.getPrefixLength();
        // Use col.getSize() for the byte size (e.g. 4 for INT, 8 for BIGINT).
        // ndbRecordImpl.lengths[] stores Column.getLength() which is the
        // element count (1 for scalar INT), NOT the byte count.
        int byteSize = col.getSize();
        // Zero out the column's storage area
        for (int i = 0; i < prefixLength + byteSize; i++) {
            buffer.put(offset + i, (byte) 0);
        }
        // For NOT NULL columns, null bit doesn't exist, so no need to clear
    }

    /**
     * Write the type maximum into the meta row's TTL column so that the meta
     * row's entry in an ordered index on that column sorts after every purge
     * candidate: TIMESTAMP '2038-01-19 03:14:07' UTC, DATETIME
     * '9999-12-31 23:59:59'. Not needed for correctness (DBTUP never expires a
     * ring meta row); it only keeps the TTL purge from visiting one wasted
     * candidate per prefix per round.
     *
     * Encoding follows NdbSqlUtil::pack_timestamp2 / pack_datetime2: big-endian
     * seconds (4 bytes) or packed date-time (5 bytes, sign bit set), followed
     * by (precision + 1) / 2 big-endian fraction bytes, here zero.
     */
    private void setTTLColumnMax(ByteBuffer buffer, Column col) {
        int columnId = col.getColumnId();
        int offset = ndbRecordImpl.offsets[columnId];
        int fracBytes = (col.getPrecision() + 1) / 2;
        byte[] packed;
        if (col.getType() == ColumnType.Timestamp2) {
            // 0x7FFFFFFF seconds since the epoch
            packed = new byte[] {(byte) 0x7F, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
        } else {
            // 9999-12-31 23:59:59: ((9999 * 13 + 12) << 5 | 31) << 17 | (23 << 12 | 59 << 6 | 59),
            // plus the sign bit (bit 39) for a positive value
            packed = new byte[] {(byte) 0xFE, (byte) 0xF3, (byte) 0xFF, (byte) 0x7E, (byte) 0xFB};
        }
        ndbRecordImpl.resetNull(buffer, col);
        for (int i = 0; i < packed.length; i++) {
            buffer.put(offset + i, packed[i]);
        }
        for (int i = 0; i < fracBytes; i++) {
            buffer.put(offset + packed.length + i, (byte) 0);
        }
    }

    private static boolean columnIsSet(byte[] mask, int columnId) {
        int byteOffset = columnId / 8;
        if (byteOffset >= mask.length) return false;
        int bitInByte = columnId - (byteOffset * 8);
        return (mask[byteOffset] & (1 << bitInByte)) != 0;
    }

    private static void columnSet(byte[] mask, int columnId) {
        int byteOffset = columnId / 8;
        int bitInByte = columnId - (byteOffset * 8);
        mask[byteOffset] |= NdbRecordImpl.BIT_IN_BYTE_MASK[bitInByte];
    }
}
