-- Copyright (C) 2023 Hopsworks AB
--
-- This program is free software; you can redistribute it and/or
-- modify it under the terms of the GNU General Public License
-- as published by the Free Software Foundation; either version 2
-- of the License, or (at your option) any later version.
--
-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.
--
-- You should have received a copy of the GNU General Public License
-- along with this program; if not, write to the Free Software
-- Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301,
-- USA.


-- additional api keys for testing

-- Explicit column list: this template runs AFTER the hopsworks-ddl migration
-- patches, which add columns to api_key (e.g. V73 adds `expiry`), so a
-- positional VALUES list would no longer match the table definition.
INSERT INTO
    `api_key` (`id`, `prefix`, `secret`, `salt`, `created`, `modified`, `name`, `user_id`, `reserved`)
VALUES
    (
         KEY_ID,
        'KEY_PREFIX',
        '709faa77accc3f30394cfb53b67253ba64881528cb3056eea110703ca430cce4',
        '1/1TxiaiIB01rIcY2E36iuwKP6fm2GzBaNaQqOVGMhH0AvcIlIzaUIw0fMDjKNLa0OWxAOrfTSPqAolpI/n+ug==',
        '2022-06-14 10:27:03',
        '2022-06-14 10:27:03',
        'KEY_NAME',
        10000,
        0
    );

