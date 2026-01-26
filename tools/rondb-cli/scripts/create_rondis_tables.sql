/*
   Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.

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

CREATE TABLE IF NOT EXISTS string_keys(
  redis_key_id BIGINT UNSIGNED NOT NULL,
  redis_key VARBINARY(3000) NOT NULL,
  rondb_key BIGINT UNSIGNED AUTO_INCREMENT NULL,
  value_data_type INT UNSIGNED NOT NULL,
  tot_value_len INT UNSIGNED NOT NULL,
  num_rows INT UNSIGNED NOT NULL,
  value_start VARBINARY(4096) NOT NULL,
  expiry_date TIMESTAMP,
  KEY ttl_index(expiry_date),
  PRIMARY KEY (redis_key_id, redis_key) USING HASH,
  UNIQUE KEY (rondb_key) USING HASH
) ENGINE NDB CHARSET = latin1;
CREATE TABLE IF NOT EXISTS hset_keys(
  redis_key VARBINARY(3000) NOT NULL,
  redis_key_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (redis_key) USING HASH,
  UNIQUE KEY (redis_key_id) USING HASH
) ENGINE NDB CHARSET latin1;
CREATE TABLE IF NOT EXISTS string_values(
  rondb_key BIGINT UNSIGNED NOT NULL,
  ordinal INT UNSIGNED NOT NULL,
  expiry_date TIMESTAMP,
  value VARBINARY(29500) NOT NULL,
  KEY ttl_index(expiry_date),
  PRIMARY KEY (rondb_key, ordinal)
) ENGINE NDB CHARSET latin1;

