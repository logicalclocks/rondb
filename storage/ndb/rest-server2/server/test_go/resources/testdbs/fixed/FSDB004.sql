-- This file is part of the RonDB REST API Server
-- Copyright (c) 2025 Hopsworks AB
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

-- Signed and unsigned number data types
DROP DATABASE IF EXISTS fsdb004;

CREATE DATABASE fsdb004;

USE fsdb004;

DROP TABLE IF EXISTS `fg1_1`;

CREATE TABLE `fg1_1` (
  `id` bigint NOT NULL,
  `ts` date DEFAULT NULL,
  `col2` bigint DEFAULT NULL,
  PRIMARY KEY (`id`) USING HASH
) ENGINE=ndbcluster DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='NDB_TABLE=READ_BACKUP=1';

INSERT INTO `fg1_1` VALUES (3,'2022-01-14',0);
INSERT INTO `fg1_1` VALUES (1,'2022-01-14',0);
INSERT INTO `fg1_1` VALUES (2,'2022-01-14',1);


DROP TABLE IF EXISTS `fg2_1`;


CREATE TABLE `fg2_1` (
  `id` bigint NOT NULL,
  `ts` date DEFAULT NULL,
  `col2` double DEFAULT NULL,
  PRIMARY KEY (`id`) USING HASH
) ENGINE=ndbcluster DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='NDB_TABLE=READ_BACKUP=1';


INSERT INTO `fg2_1` VALUES (3,'2022-01-14',0.3);
INSERT INTO `fg2_1` VALUES (1,'2022-01-14',0.1);
INSERT INTO `fg2_1` VALUES (2,'2022-01-14',0.2);


DROP TABLE IF EXISTS `iris_modal_1`;


CREATE TABLE `iris_modal_1` (
  `index` bigint NOT NULL,
  `sepal_length` double DEFAULT NULL,
  `sepal_width` double DEFAULT NULL,
  `petal_length` double DEFAULT NULL,
  `petal_width` double DEFAULT NULL,
  `variety` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`index`) USING HASH
) ENGINE=ndbcluster DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='NDB_TABLE=READ_BACKUP=1';


INSERT INTO `iris_modal_1` VALUES (59,5.2,2.7,3.9,1.4,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (84,5.4,3,4.5,1.5,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (70,5.9,3.2,4.8,1.8,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (11,4.8,3.4,1.6,0.2,'Setosa');
INSERT INTO `iris_modal_1` VALUES (72,6.3,2.5,4.9,1.5,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (86,6.7,3.1,4.7,1.5,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (92,5.8,2.6,4,1.2,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (98,5.1,2.5,3,1.1,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (20,5.4,3.4,1.7,0.2,'Setosa');
INSERT INTO `iris_modal_1` VALUES (80,5.5,2.4,3.8,1.1,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (123,6.3,2.7,4.9,1.8,'Virginica');
INSERT INTO `iris_modal_1` VALUES (106,4.9,2.5,4.5,1.7,'Virginica');
INSERT INTO `iris_modal_1` VALUES (124,6.7,3.3,5.7,2.1,'Virginica');
INSERT INTO `iris_modal_1` VALUES (116,6.5,3,5.5,1.8,'Virginica');
INSERT INTO `iris_modal_1` VALUES (137,6.4,3.1,5.5,1.8,'Virginica');
INSERT INTO `iris_modal_1` VALUES (143,6.8,3.2,5.9,2.3,'Virginica');
INSERT INTO `iris_modal_1` VALUES (102,7.1,3,5.9,2.1,'Virginica');
INSERT INTO `iris_modal_1` VALUES (54,6.5,2.8,4.6,1.5,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (96,5.7,2.9,4.2,1.3,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (148,6.2,3.4,5.4,2.3,'Virginica');
INSERT INTO `iris_modal_1` VALUES (26,5,3.4,1.6,0.4,'Setosa');
INSERT INTO `iris_modal_1` VALUES (132,6.4,2.8,5.6,2.2,'Virginica');
INSERT INTO `iris_modal_1` VALUES (145,6.7,3,5.2,2.3,'Virginica');
INSERT INTO `iris_modal_1` VALUES (0,5.1,3.5,1.4,0.2,'Setosa');
INSERT INTO `iris_modal_1` VALUES (47,4.6,3.2,1.4,0.2,'Setosa');
INSERT INTO `iris_modal_1` VALUES (50,7,3.2,4.7,1.4,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (113,5.7,2.5,5,2,'Virginica');
INSERT INTO `iris_modal_1` VALUES (10,5.4,3.7,1.5,0.2,'Setosa');
INSERT INTO `iris_modal_1` VALUES (19,5.1,3.8,1.5,0.3,'Setosa');
INSERT INTO `iris_modal_1` VALUES (39,5.1,3.4,1.5,0.2,'Setosa');
INSERT INTO `iris_modal_1` VALUES (52,6.9,3.1,4.9,1.5,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (23,5.1,3.3,1.7,0.5,'Setosa');
INSERT INTO `iris_modal_1` VALUES (89,5.5,2.5,4,1.3,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (135,7.7,3,6.1,2.3,'Virginica');
INSERT INTO `iris_modal_1` VALUES (14,5.8,4,1.2,0.2,'Setosa');
INSERT INTO `iris_modal_1` VALUES (134,6.1,2.6,5.6,1.4,'Virginica');
INSERT INTO `iris_modal_1` VALUES (12,4.8,3,1.4,0.1,'Setosa');
INSERT INTO `iris_modal_1` VALUES (44,5.1,3.8,1.9,0.4,'Setosa');
INSERT INTO `iris_modal_1` VALUES (53,5.5,2.3,4,1.3,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (66,5.6,3,4.5,1.5,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (112,6.8,3,5.5,2.1,'Virginica');
INSERT INTO `iris_modal_1` VALUES (119,6,2.2,5,1.5,'Virginica');
INSERT INTO `iris_modal_1` VALUES (40,5,3.5,1.3,0.3,'Setosa');
INSERT INTO `iris_modal_1` VALUES (88,5.6,3,4.1,1.3,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (94,5.6,2.7,4.2,1.3,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (15,5.7,4.4,1.5,0.4,'Setosa');
INSERT INTO `iris_modal_1` VALUES (35,5,3.2,1.2,0.2,'Setosa');
INSERT INTO `iris_modal_1` VALUES (57,4.9,2.4,3.3,1,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (97,6.2,2.9,4.3,1.3,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (142,5.8,2.7,5.1,1.9,'Virginica');
INSERT INTO `iris_modal_1` VALUES (16,5.4,3.9,1.3,0.4,'Setosa');
INSERT INTO `iris_modal_1` VALUES (69,5.6,2.5,3.9,1.1,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (1,4.9,3,1.4,0.2,'Setosa');
INSERT INTO `iris_modal_1` VALUES (33,5.5,4.2,1.4,0.2,'Setosa');
INSERT INTO `iris_modal_1` VALUES (91,6.1,3,4.6,1.4,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (110,6.5,3.2,5.1,2,'Virginica');
INSERT INTO `iris_modal_1` VALUES (3,4.6,3.1,1.5,0.2,'Setosa');
INSERT INTO `iris_modal_1` VALUES (8,4.4,2.9,1.4,0.2,'Setosa');
INSERT INTO `iris_modal_1` VALUES (87,6.3,2.3,4.4,1.3,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (138,6,3,4.8,1.8,'Virginica');
INSERT INTO `iris_modal_1` VALUES (146,6.3,2.5,5,1.9,'Virginica');
INSERT INTO `iris_modal_1` VALUES (41,4.5,2.3,1.3,0.3,'Setosa');
INSERT INTO `iris_modal_1` VALUES (49,5,3.3,1.4,0.2,'Setosa');
INSERT INTO `iris_modal_1` VALUES (136,6.3,3.4,5.6,2.4,'Virginica');
INSERT INTO `iris_modal_1` VALUES (36,5.5,3.5,1.3,0.2,'Setosa');
INSERT INTO `iris_modal_1` VALUES (61,5.9,3,4.2,1.5,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (51,6.4,3.2,4.5,1.5,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (85,6,3.4,4.5,1.6,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (29,4.7,3.2,1.6,0.2,'Setosa');
INSERT INTO `iris_modal_1` VALUES (55,5.7,2.8,4.5,1.3,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (60,5,2,3.5,1,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (100,6.3,3.3,6,2.5,'Virginica');
INSERT INTO `iris_modal_1` VALUES (104,6.5,3,5.8,2.2,'Virginica');
INSERT INTO `iris_modal_1` VALUES (2,4.7,3.2,1.3,0.2,'Setosa');
INSERT INTO `iris_modal_1` VALUES (131,7.9,3.8,6.4,2,'Virginica');
INSERT INTO `iris_modal_1` VALUES (9,4.9,3.1,1.5,0.1,'Setosa');
INSERT INTO `iris_modal_1` VALUES (25,5,3,1.6,0.2,'Setosa');
INSERT INTO `iris_modal_1` VALUES (30,4.8,3.1,1.6,0.2,'Setosa');
INSERT INTO `iris_modal_1` VALUES (79,5.7,2.6,3.5,1,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (27,5.2,3.5,1.5,0.2,'Setosa');
INSERT INTO `iris_modal_1` VALUES (118,7.7,2.6,6.9,2.3,'Virginica');
INSERT INTO `iris_modal_1` VALUES (126,6.2,2.8,4.8,1.8,'Virginica');
INSERT INTO `iris_modal_1` VALUES (147,6.5,3,5.2,2,'Virginica');
INSERT INTO `iris_modal_1` VALUES (109,7.2,3.6,6.1,2.5,'Virginica');
INSERT INTO `iris_modal_1` VALUES (120,6.9,3.2,5.7,2.3,'Virginica');
INSERT INTO `iris_modal_1` VALUES (141,6.9,3.1,5.1,2.3,'Virginica');
INSERT INTO `iris_modal_1` VALUES (17,5.1,3.5,1.4,0.3,'Setosa');
INSERT INTO `iris_modal_1` VALUES (21,5.1,3.7,1.5,0.4,'Setosa');
INSERT INTO `iris_modal_1` VALUES (34,4.9,3.1,1.5,0.2,'Setosa');
INSERT INTO `iris_modal_1` VALUES (90,5.5,2.6,4.4,1.2,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (37,4.9,3.6,1.4,0.1,'Setosa');
INSERT INTO `iris_modal_1` VALUES (122,7.7,2.8,6.7,2,'Virginica');
INSERT INTO `iris_modal_1` VALUES (28,5.2,3.4,1.4,0.2,'Setosa');
INSERT INTO `iris_modal_1` VALUES (121,5.6,2.8,4.9,2,'Virginica');
INSERT INTO `iris_modal_1` VALUES (117,7.7,3.8,6.7,2.2,'Virginica');
INSERT INTO `iris_modal_1` VALUES (18,5.7,3.8,1.7,0.3,'Setosa');
INSERT INTO `iris_modal_1` VALUES (67,5.8,2.7,4.1,1,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (93,5,2.3,3.3,1,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (107,7.3,2.9,6.3,1.8,'Virginica');
INSERT INTO `iris_modal_1` VALUES (46,5.1,3.8,1.6,0.2,'Setosa');
INSERT INTO `iris_modal_1` VALUES (130,7.4,2.8,6.1,1.9,'Virginica');
INSERT INTO `iris_modal_1` VALUES (133,6.3,2.8,5.1,1.5,'Virginica');
INSERT INTO `iris_modal_1` VALUES (125,7.2,3.2,6,1.8,'Virginica');
INSERT INTO `iris_modal_1` VALUES (45,4.8,3,1.4,0.3,'Setosa');
INSERT INTO `iris_modal_1` VALUES (58,6.6,2.9,4.6,1.3,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (111,6.4,2.7,5.3,1.9,'Virginica');
INSERT INTO `iris_modal_1` VALUES (5,5.4,3.9,1.7,0.4,'Setosa');
INSERT INTO `iris_modal_1` VALUES (7,5,3.4,1.5,0.2,'Setosa');
INSERT INTO `iris_modal_1` VALUES (76,6.8,2.8,4.8,1.4,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (101,5.8,2.7,5.1,1.9,'Virginica');
INSERT INTO `iris_modal_1` VALUES (63,6.1,2.9,4.7,1.4,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (31,5.4,3.4,1.5,0.4,'Setosa');
INSERT INTO `iris_modal_1` VALUES (127,6.1,3,4.9,1.8,'Virginica');
INSERT INTO `iris_modal_1` VALUES (144,6.7,3.3,5.7,2.5,'Virginica');
INSERT INTO `iris_modal_1` VALUES (48,5.3,3.7,1.5,0.2,'Setosa');
INSERT INTO `iris_modal_1` VALUES (82,5.8,2.7,3.9,1.2,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (114,5.8,2.8,5.1,2.4,'Virginica');
INSERT INTO `iris_modal_1` VALUES (149,5.9,3,5.1,1.8,'Virginica');
INSERT INTO `iris_modal_1` VALUES (4,5,3.6,1.4,0.2,'Setosa');
INSERT INTO `iris_modal_1` VALUES (42,4.4,3.2,1.3,0.2,'Setosa');
INSERT INTO `iris_modal_1` VALUES (62,6,2.2,4,1,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (77,6.7,3,5,1.7,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (103,6.3,2.9,5.6,1.8,'Virginica');
INSERT INTO `iris_modal_1` VALUES (129,7.2,3,5.8,1.6,'Virginica');
INSERT INTO `iris_modal_1` VALUES (32,5.2,4.1,1.5,0.1,'Setosa');
INSERT INTO `iris_modal_1` VALUES (64,5.6,2.9,3.6,1.3,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (13,4.3,3,1.1,0.1,'Setosa');
INSERT INTO `iris_modal_1` VALUES (24,4.8,3.4,1.9,0.2,'Setosa');
INSERT INTO `iris_modal_1` VALUES (71,6.1,2.8,4,1.3,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (105,7.6,3,6.6,2.1,'Virginica');
INSERT INTO `iris_modal_1` VALUES (78,6,2.9,4.5,1.5,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (99,5.7,2.8,4.1,1.3,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (38,4.4,3,1.3,0.2,'Setosa');
INSERT INTO `iris_modal_1` VALUES (65,6.7,3.1,4.4,1.4,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (73,6.1,2.8,4.7,1.2,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (95,5.7,3,4.2,1.2,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (128,6.4,2.8,5.6,2.1,'Virginica');
INSERT INTO `iris_modal_1` VALUES (43,5,3.5,1.6,0.6,'Setosa');
INSERT INTO `iris_modal_1` VALUES (68,6.2,2.2,4.5,1.5,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (74,6.4,2.9,4.3,1.3,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (139,6.9,3.1,5.4,2.1,'Virginica');
INSERT INTO `iris_modal_1` VALUES (6,4.6,3.4,1.4,0.3,'Setosa');
INSERT INTO `iris_modal_1` VALUES (81,5.5,2.4,3.7,1,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (115,6.4,3.2,5.3,2.3,'Virginica');
INSERT INTO `iris_modal_1` VALUES (140,6.7,3.1,5.6,2.4,'Virginica');
INSERT INTO `iris_modal_1` VALUES (22,4.6,3.6,1,0.2,'Setosa');
INSERT INTO `iris_modal_1` VALUES (56,6.3,3.3,4.7,1.6,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (83,6,2.7,5.1,1.6,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (75,6.6,3,4.4,1.4,'Versicolor');
INSERT INTO `iris_modal_1` VALUES (108,6.7,2.5,5.8,1.8,'Virginica');

