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

CREATE TABLE string_keys(
    -- Redis actually supports a max key size of 512MiB,
    -- but we choose not to support that here
    redis_key_id BIGINT UNSIGNED NOT NULL,
    redis_key VARBINARY(3000) NOT NULL,
    -- This is to save space when referencing the key in the value table
    rondb_key BIGINT UNSIGNED AUTO_INCREMENT NULL,
    -- TODO: Replace with Enum below
    value_data_type INT UNSIGNED NOT NULL,
    -- value_data_type ENUM('string', 'number', 'binary_string'),
    -- Max 512MiB --> 512 * 1,048,576 bytes = 536,870,912 characters
    -- --> To describe the length, one needs at least UINT (4,294,967,295)
    tot_value_len INT UNSIGNED NOT NULL,
    -- Technically implicit
    num_rows INT UNSIGNED NOT NULL,
    value_start VARBINARY(4096) NOT NULL,
    -- Redis supports get/set of seconds/milliseconds
    expiry_date TIMESTAMP,
    -- Easier to sort and delete keys this way
    KEY ttl_index(expiry_date),
    PRIMARY KEY (redis_key_id, redis_key) USING HASH,
    UNIQUE KEY (rondb_key) USING HASH
) ENGINE NDB -- Each CHAR will use 1 byte
CHARSET = latin1 COMMENT = "NDB_TABLE=PARTITION_BALANCE=FOR_RP_BY_LDM_X_8,TTL=0@expiry_date";
