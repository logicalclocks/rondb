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

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;

/** Schema
 *
drop table if exists ring_buffer_notnull;
create table ring_buffer_notnull (
    client_id int not null,
    ring_idx int not null default 0,
    ring_meta varbinary(64),
    name varchar(50) not null,
    score int not null,
    PRIMARY KEY (client_id, ring_idx)
) ENGINE=ndbcluster COMMENT='NDB_TABLE=MAX_ROWS_PER_PK=3@ring_idx@ring_meta';
 */
@PersistenceCapable(table="ring_buffer_notnull")
public interface RingBufferNotNull {

    @PrimaryKey
    @Column(name="client_id")
    int getClientId();
    void setClientId(int id);

    @PrimaryKey
    @Column(name="ring_idx")
    int getRingIdx();
    void setRingIdx(int idx);

    @Column(name="name")
    String getName();
    void setName(String name);

    @Column(name="score")
    int getScore();
    void setScore(int score);
}
