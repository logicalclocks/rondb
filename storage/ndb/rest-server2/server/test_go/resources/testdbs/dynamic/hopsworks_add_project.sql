--
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

-- Sed keywords PROJECT_NAME, PROJECT_NUMBER
INSERT INTO
    `project`
VALUES
    (
        PROJECT_NUMBER, 'PROJECT_NAME', 'macho@hopsworks.ai', '2022-05-30 14:17:22', 'Some desc', 'NOLIMIT', '2022-05-30 14:17:38', 100, 'SomeDockerImage', 1, 0
    );

-- Register the database as a feature store and share it entirely with the
-- api key user's home project 999/demo0. macho is only a member of 999;
-- access to every other database comes via shared_feature_store, mirroring
-- how real Hopsworks grants cross-project access.
INSERT INTO
    `feature_store`
SET
    `id` = PROJECT_NUMBER,
    `name` = 'PROJECT_NAME',
    `project_id` = PROJECT_NUMBER,
    `created` = '2022-05-30 14:17:22';

INSERT INTO
    `shared_feature_store`
SET
    `id` = PROJECT_NUMBER,
    `feature_store` = PROJECT_NUMBER,
    `shared_by` = 10000,
    `shared_on` = '2022-06-01 13:28:05',
    `shared_with_project` = 999,
    `shared_entirely` = 1;
