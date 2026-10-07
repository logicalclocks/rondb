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

package testsuite.clusterj.model;

import java.sql.Timestamp;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;

/** Schema
 *
drop table if exists ring_buffer_ttl;
create table ring_buffer_ttl (
    client_id int not null,
    ring_idx int not null default 0,
    ring_meta varbinary(64),
    ts timestamp null,
    val varchar(50),
    PRIMARY KEY (client_id, ring_idx)
) ENGINE=ndbcluster COMMENT='NDB_TABLE=TTL=3600@ts,MAX_ROWS_PER_PK=3@ring_idx@ring_meta';

 * A ring buffer table with TTL: rows expire one hour after ts; the ring
 * meta row carries the TIMESTAMP maximum in ts and never expires.
 */
@PersistenceCapable(table="ring_buffer_ttl")
public interface RingBufferTtl {

    @PrimaryKey
    @Column(name="client_id")
    int getClientId();
    void setClientId(int id);

    @PrimaryKey
    @Column(name="ring_idx")
    int getRingIdx();
    void setRingIdx(int idx);

    @Column(name="ts")
    Timestamp getTs();
    void setTs(Timestamp ts);

    @Column(name="val")
    String getVal();
    void setVal(String val);
}
