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
        PROJECT_NUMBER, 322, 'PROJECT_NAME', 322, 'PROJECT_NAME', 'macho@hopsworks.ai', '2022-05-30 14:17:22', 'Some desc', 'NOLIMIT', '2022-05-30 14:17:38', 100, 'SomeDockerImage', 1, 0
    );

INSERT INTO
    `project_team`
VALUES
    (
        PROJECT_NUMBER,
        'macho@hopsworks.ai',
        'Data scientist',
        '2022-06-01 13:28:05'
    );
