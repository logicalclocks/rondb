--
-- This file is part of the RonDB REST API Server
-- Copyright (c) 2023 Hopsworks AB
--
-- This program is free software: you can redistribute it and/or modify
-- it under the terms of the GNU General Public License as published by
-- the Free Software Foundation, version 3.
--
-- This program is distributed in the hope that it will be useful, but
-- WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
-- General Public License for more details.
--
-- You should have received a copy of the GNU General Public License
-- along with this program. If not, see <http://www.gnu.org/licenses/>.

-- Sed keywords PROJECT_NAME, PROJECT_NUMBER
INSERT INTO
    `project`
VALUES
    (
        PROJECT_NUMBER, 'PROJECT_NAME', 'macho@hopsworks.ai', '2022-05-30 14:17:22', 'Some desc', 'NOLIMIT', '2022-05-30 14:17:38', 100, 'SomeDockerImage', 1, 0
    );

-- Also make macho a member of the project. In username mode
-- (RateLimitIdentity) only member projects have a project-user rate limit
-- identity ("<project>_<username>", matching the online-FS MySQL account
-- Hopsworks creates per membership), so the rate limit tests need their
-- target databases to be member projects - as they are in production,
-- where a feature view is served from a project the caller belongs to.
INSERT INTO
    `project_team`
SET
    `project_id` = PROJECT_NUMBER,
    `team_member` = 'macho@hopsworks.ai',
    `team_role` = 'Data scientist',
    `added` = '2022-06-01 13:28:05';

-- Register the database as a feature store and share it entirely with the
-- api key user's home project 999/demo0, mirroring how real Hopsworks
-- grants cross-project access (the membership above and this share grant
-- the same data access; sharing-specific behaviour is covered by the
-- sharing test package with its own users).
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
