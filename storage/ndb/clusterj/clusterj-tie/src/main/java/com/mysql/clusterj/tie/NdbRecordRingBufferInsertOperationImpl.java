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

import com.mysql.clusterj.core.store.Table;

/**
 * Insert operation for ring buffer tables.  Instead of submitting a normal
 * insertTuple, it delegates to the RingBufferWriter which manages the
 * ring_idx/ring_meta columns and the meta row protocol.
 *
 * From the caller's perspective this behaves like a normal insert operation:
 * set column values, then endDefinition().  The ring buffer protocol is
 * handled transparently.
 */
public class NdbRecordRingBufferInsertOperationImpl extends NdbRecordOperationImpl {

    public NdbRecordRingBufferInsertOperationImpl(
            ClusterTransactionImpl clusterTransaction, Table storeTable) {
        super(clusterTransaction, storeTable);
        this.valueBuffer = ndbRecordValues.newBuffer();
        this.ndbRecordKeys = ndbRecordValues;
        this.keyBuffer = valueBuffer;
        resetMask();
    }

    @Override
    public void endDefinition() {
        // Get the ring buffer writer (cached per table per transaction)
        RingBufferWriter writer = clusterTransaction.getRingBufferWriter(storeTable);

        // Prepare the buffer for the writer
        valueBuffer.position(0);
        valueBuffer.limit(valueBufferSize);

        // Delegate to the ring buffer writer
        writer.addRow(valueBuffer, mask);

        // Return the buffer to the pool since the writer copies/consumes data
        ndbRecordValues.returnBuffer(valueBuffer);
        valueBuffer = null;
    }

    @Override
    public String toString() {
        return " ring_buffer_insert " + tableName;
    }
}
