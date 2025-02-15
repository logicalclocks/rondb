/*
   Copyright (c) 2024, 2024, Hopsworks and/or its affiliates.

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

CREATE TABLE string_values(
    rondb_key BIGINT UNSIGNED NOT NULL,
    ordinal INT UNSIGNED NOT NULL,
    expiry_date TIMESTAMP,
    value VARBINARY(29500) NOT NULL,
    KEY ttl_index(expiry_date),
    PRIMARY KEY (rondb_key, ordinal)
) ENGINE NDB,
COMMENT = "NDB_TABLE=PARTITION_BALANCE=FOR_RP_BY_LDM_X_8,TTL=0@expiry_date";
