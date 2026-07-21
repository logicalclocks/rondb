-- RDRS2 test fixture: fine-grained FG/FV sharing (RONDB-1088).
--
-- This file is the CLEANED, SELF-CONTAINED import of the live-cluster
-- reference state dumped in docs/fine_grained_recordings/ (Hopsworks 5.0.1,
-- after all T2 grants were applied): the hopsworks metadata rows PLUS the
-- usera_project online feature-store database. It is loaded after
-- hopsworks_data.sql (see embeddings.go: HopsworksScheme) and is kept
-- separate from it on purpose: these rows mirror a real cluster and are
-- re-importable wholesale from fresh dumps.
--
-- All test rows have ids >= 100000 (an auto-increment floor was applied to
-- the cluster before creating them), so they cannot collide with the
-- hand-written fixture rows in hopsworks_data.sql.
--
-- Cleaning rules applied to the raw dumps (cluster system rows are NOT here):
--   users            only the test users usera..userl (uids 100000-100011)
--                    plus userm (100012, added 2026-07-17 for scenario D5);
--                    admin/agent/srvmanager/onlinefs/airflow dropped (the
--                    cluster admin uid 10000 collides with fixture user macho)
--   project_team     the 12 auto-added onlinefs/serving service memberships
--                    dropped; 6 owners + 3 restricted members kept
--   api_key          only the user*_api_key rows (ids 100012-100023 + 100025
--                    for userm); admin key and the auto-created serving_*
--                    keys dropped. Cleartext keys: USERA_API_KEY..
--                    USERM_API_KEY constants in embeddings.go.
--   api_key_scope    dropped entirely - RDRS never reads it
--   schemas/subjects only the 2 usera FG avro subjects; the per-project
--                    'inferenceschema' serving-infra rows dropped
--   cached_feature_group  orphan rows (ids < 100000) from wiped projects dropped
--   columns          columns the cluster has but our V83-level test schema
--                    lacks are stripped (e.g. project.namespace)
--   usera_project    only the two FG data tables; onlinefs infra tables
--                    (kafka_offsets, online_ingestion_result) dropped
--
-- Population summary (see docs/fg_fv_sharing_fine_grained_test_design.md):
--   usera_project (100000): producer - usera_customers_fg + usera_transactions_fg,
--                           FVs usera_customers_transactions_fv + usera_txncount_fv;
--                           userj/k/l are 'Feature store restricted' members
--   userb: whole store shared    userc: both FGs shared (+userc_own_fv)
--   userd: customers_fg shared   usere: customers whole + transactions subset
--                                       (+usere_own_fv)
--   userf: nothing shared        userk/l: restricted grants (+own FVs in
--                                usera_project); userj: restricted, no grants
--   userm (D5): restricted in usera_project (customers entirely) AND owner
--               of his own empty userm_project - restricted grants do not
--               travel to it (recording_D5.json)

USE hopsworks;

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

-- ---- users (12 rows) ----

INSERT INTO `users` SET
  `uid` = 100000,
  `username` = 'usera000',
  `password` = 'deccdc5ba89f92c7badc94b6bc163bda305a355f0544051fda966f8123dbcd71',
  `email` = 'usera@lc.com',
  `fname` = 'usera',
  `lname` = 'lc',
  `activated` = '2026-07-16 08:29:28',
  `title` = '-',
  `false_login` = 0,
  `status` = 2,
  `isonline` = 1,
  `secret` = 'JTMYV5BGSL6U74KO',
  `validation_key` = 'IXUaskCUyd2fSvvlRZoZg+zEI/0KzInU9yVobtUfJEC0eUnWHzmsqFsqw0rNqDEGEEgD9fVYX5IedP0pnTc60g==',
  `validation_key_updated` = '2026-07-16 08:29:28',
  `validation_key_type` = 'EMAIL',
  `mode` = 0,
  `password_changed` = '2026-07-16 08:29:28',
  `notes` = NULL,
  `max_num_projects` = 10,
  `num_active_projects` = 1,
  `two_factor` = 0,
  `tours_state` = 0,
  `salt` = 'bhp+R5yROErb7aDp4+/IXaXonF6TqeHmssoc7/tcj/U56tUd/QypSjHKknAiDjwRK3bNeg4KD76c7NrmepOvpw==',
  `last_visited_at` = '2026-07-16 08:29:28';
INSERT INTO `users` SET
  `uid` = 100001,
  `username` = 'userb000',
  `password` = 'deb0298cd852247d79a204881fe499639cb042d6f8e7fed57d656928976680b1',
  `email` = 'userb@lc.com',
  `fname` = 'userb',
  `lname` = 'lc',
  `activated` = '2026-07-16 08:29:47',
  `title` = '-',
  `false_login` = 0,
  `status` = 2,
  `isonline` = 1,
  `secret` = 'ATYQUDPBH5T2WHNS',
  `validation_key` = 'knqV6db69i25IKNt0ufdfh0oxc6FXeQDAofrHG2bODIsOEJB7PsftG6OaqhT3CztvnPioCPJ8CT24OB4HJTIxA==',
  `validation_key_updated` = '2026-07-16 08:29:47',
  `validation_key_type` = 'EMAIL',
  `mode` = 0,
  `password_changed` = '2026-07-16 08:29:47',
  `notes` = NULL,
  `max_num_projects` = 10,
  `num_active_projects` = 1,
  `two_factor` = 0,
  `tours_state` = 0,
  `salt` = 'ydgqTE2Df8EGfmbVP33F7Ob+BaQTPhBfVFeXMvreLvKfK5SYTZFL6z/P9pDmKecvVMQVS8bqsEvOsBXZgRlB+Q==',
  `last_visited_at` = '2026-07-16 08:29:47';
INSERT INTO `users` SET
  `uid` = 100002,
  `username` = 'userc000',
  `password` = 'cc93166285d72ebd2d4822f113ff142e4d0f3e44c4fbc6dc8bdeb1d3ca163aa7',
  `email` = 'userc@lc.com',
  `fname` = 'userc',
  `lname` = 'lc',
  `activated` = '2026-07-16 08:29:48',
  `title` = '-',
  `false_login` = 0,
  `status` = 2,
  `isonline` = 0,
  `secret` = 'A5SGS35S2C3U27MK',
  `validation_key` = 'lNHF/eCq28MiSoY717irBq2MOl+moeK6xh643ai5eeQeR1u6t06PvLhMh6f6qPWloXR5l5wDtvgV3JPoLzBMsA==',
  `validation_key_updated` = '2026-07-16 08:29:48',
  `validation_key_type` = 'EMAIL',
  `mode` = 0,
  `password_changed` = '2026-07-16 08:29:48',
  `notes` = NULL,
  `max_num_projects` = 10,
  `num_active_projects` = 1,
  `two_factor` = 0,
  `tours_state` = 0,
  `salt` = '3RQFlIapkZblcNjSrm5OP9zdSgtBDoVG5c9Tc89dhlR72O1nr3oTxQiHeDa1pYENbIFo4m4l56GoB8Hh53PtSA==',
  `last_visited_at` = '2026-07-16 08:29:48';
INSERT INTO `users` SET
  `uid` = 100003,
  `username` = 'userd000',
  `password` = 'd32cc34863a54608064db8b45d071f6b808f62a1e4590658d3f95d1759b3e399',
  `email` = 'userd@lc.com',
  `fname` = 'userd',
  `lname` = 'lc',
  `activated` = '2026-07-16 08:29:49',
  `title` = '-',
  `false_login` = 0,
  `status` = 2,
  `isonline` = 1,
  `secret` = 'JXWTPKEJ5YYP253K',
  `validation_key` = 'JVygqHAZKsqWnD/GlPhjVkJktyl6rA4bYuTtYNzB1+Bg466qzqHtCL9ICHgetzuR7sJ+p5u3V2yHfYotf2evOA==',
  `validation_key_updated` = '2026-07-16 08:29:49',
  `validation_key_type` = 'EMAIL',
  `mode` = 0,
  `password_changed` = '2026-07-16 08:29:49',
  `notes` = NULL,
  `max_num_projects` = 10,
  `num_active_projects` = 1,
  `two_factor` = 0,
  `tours_state` = 0,
  `salt` = 'A7ZW7XE/U0J6QyjrQAXAp1CAi2tBPFKrJSlzeSGA4+gY0WpoorNaUiODa6e08r5xvON0TKkGIeDeXdR1CTmDtg==',
  `last_visited_at` = '2026-07-16 08:29:49';
INSERT INTO `users` SET
  `uid` = 100004,
  `username` = 'usere000',
  `password` = '011d99885b62cc6501fd0caa53990cbe5e26a3b827fe75a28dae4efb9a2cc4f9',
  `email` = 'usere@lc.com',
  `fname` = 'usere',
  `lname` = 'lc',
  `activated` = '2026-07-16 08:29:50',
  `title` = '-',
  `false_login` = 0,
  `status` = 2,
  `isonline` = 0,
  `secret` = 'NIOLMXF2TM2B2XTF',
  `validation_key` = 'TOABeb0zL/eKw9O6n3MGEUTEgSw+OToYiUEYXOVdKP8k3IGWrhJkiUeK0zseYZfS/b7chZMUD/+42QcBLsiwyg==',
  `validation_key_updated` = '2026-07-16 08:29:50',
  `validation_key_type` = 'EMAIL',
  `mode` = 0,
  `password_changed` = '2026-07-16 08:29:50',
  `notes` = NULL,
  `max_num_projects` = 10,
  `num_active_projects` = 1,
  `two_factor` = 0,
  `tours_state` = 0,
  `salt` = 'efVK+IDqDnznru5/7qxEW/lx70G7/8J1HkE2m1LN2KZau/aX7fm7aEBlJbV1MPO/b6EfPGIOX/rIqMgQ4DiCrQ==',
  `last_visited_at` = '2026-07-16 08:29:50';
INSERT INTO `users` SET
  `uid` = 100005,
  `username` = 'userf000',
  `password` = 'ec13480c76948037afc9e4f4a5dab737108641632b56ad031a0c22f3385734f6',
  `email` = 'userf@lc.com',
  `fname` = 'userf',
  `lname` = 'lc',
  `activated` = '2026-07-16 08:29:52',
  `title` = '-',
  `false_login` = 0,
  `status` = 2,
  `isonline` = 1,
  `secret` = 'KAOP7Q4F3TBOTZ57',
  `validation_key` = '8DYBu1quT0T3tKBOuNYM3koyN4FukzvtnwGRaJ45UaLznQjAWWR7B4329onw0Uh/Qh7lGas2hjbNczkWmA+oAg==',
  `validation_key_updated` = '2026-07-16 08:29:52',
  `validation_key_type` = 'EMAIL',
  `mode` = 0,
  `password_changed` = '2026-07-16 08:29:52',
  `notes` = NULL,
  `max_num_projects` = 10,
  `num_active_projects` = 1,
  `two_factor` = 0,
  `tours_state` = 0,
  `salt` = 'sie7CpxLxHjTM4CcLMlatykDo4irfGhybL5p4WP+Iex0JN9ClPUWfc6eE6U93rrlq1C2OebJ0VRINxDM74++Gw==',
  `last_visited_at` = '2026-07-16 08:29:52';
INSERT INTO `users` SET
  `uid` = 100006,
  `username` = 'userg000',
  `password` = 'a765c91f3e868511d6b270a76a5d42611538fb0fd45839ed55aab6d08d3624c3',
  `email` = 'userg@lc.com',
  `fname` = 'userg',
  `lname` = 'lc',
  `activated` = '2026-07-16 08:29:53',
  `title` = '-',
  `false_login` = 0,
  `status` = 2,
  `isonline` = 1,
  `secret` = 'O3CNC2RQN6ST5KY2',
  `validation_key` = '2zZ3E/+YycCJ/M8P8yR9Jv11LjmWagvG9JIDkCBZ4hQrbRfPXyJM3VAuxeuZnEDbBJLvSC6CI00klYrFGrETxA==',
  `validation_key_updated` = '2026-07-16 08:29:53',
  `validation_key_type` = 'EMAIL',
  `mode` = 0,
  `password_changed` = '2026-07-16 08:29:53',
  `notes` = NULL,
  `max_num_projects` = 10,
  `num_active_projects` = 0,
  `two_factor` = 0,
  `tours_state` = 0,
  `salt` = 's2t0GOz5/Jv5qUD+qpvffAMEy3FxR81F6AqVRYA9gRd4x5avOzstp26mldV9murIRCxCFzRKb5fe3UxKP8+6IA==',
  `last_visited_at` = '2026-07-16 08:29:53';
INSERT INTO `users` SET
  `uid` = 100007,
  `username` = 'userh000',
  `password` = '606db785e9b296f13b7ed4a745094369795eb5902a214e119c673f8ba1adb360',
  `email` = 'userh@lc.com',
  `fname` = 'userh',
  `lname` = 'lc',
  `activated` = '2026-07-16 08:29:54',
  `title` = '-',
  `false_login` = 0,
  `status` = 2,
  `isonline` = 1,
  `secret` = 'IVPGOJ3HN5TAD4LS',
  `validation_key` = 'zRbqjoJxScrwoVfod9Il/gyKjYYo62zQdq9WwpZP7qJTS2E+MFczzAISoU7w5e6aWOtlvccZHlvvxCmj3wPoyw==',
  `validation_key_updated` = '2026-07-16 08:29:54',
  `validation_key_type` = 'EMAIL',
  `mode` = 0,
  `password_changed` = '2026-07-16 08:29:54',
  `notes` = NULL,
  `max_num_projects` = 10,
  `num_active_projects` = 0,
  `two_factor` = 0,
  `tours_state` = 0,
  `salt` = 'Sol9raQWQYRRhhO3AxP+GIqxqOXquyNV7oGUkF0ticIWSKqxjzGhSng8TJs6bQjd5hY7fgI+tdvHFb4lTkw6zg==',
  `last_visited_at` = '2026-07-16 08:29:54';
INSERT INTO `users` SET
  `uid` = 100008,
  `username` = 'useri000',
  `password` = '410666cfeabefc91f6cc75a550870e97c4b862396ecebf7287e15d79fd445d75',
  `email` = 'useri@lc.com',
  `fname` = 'useri',
  `lname` = 'lc',
  `activated` = '2026-07-16 08:29:55',
  `title` = '-',
  `false_login` = 0,
  `status` = 2,
  `isonline` = 1,
  `secret` = '4SYXOCKXUZGVZ7LB',
  `validation_key` = 'ylauS1eCZSrNt2LtX+xhHGA1tRAzyJjJQP/17dsoWhLjh7baMyqNSFOtxKcBgaOboYYW2xrfv4E4US1/CxfrwQ==',
  `validation_key_updated` = '2026-07-16 08:29:55',
  `validation_key_type` = 'EMAIL',
  `mode` = 0,
  `password_changed` = '2026-07-16 08:29:55',
  `notes` = NULL,
  `max_num_projects` = 10,
  `num_active_projects` = 0,
  `two_factor` = 0,
  `tours_state` = 0,
  `salt` = 'bTdcjsfVWt/a4IzsGhV0DFYAjjcw3gwkX0vtT7ur5ki2DakHBF43EMWkbyQxzggT/8TQ2YBKIFFKXwCocHRghg==',
  `last_visited_at` = '2026-07-16 08:29:55';
INSERT INTO `users` SET
  `uid` = 100009,
  `username` = 'userj000',
  `password` = '9b0a3222ebd40192b05493aa1033d46c77ea4bc523be0295a892baf2514b13e1',
  `email` = 'userj@lc.com',
  `fname` = 'userj',
  `lname` = 'lc',
  `activated` = '2026-07-16 08:29:56',
  `title` = '-',
  `false_login` = 0,
  `status` = 2,
  `isonline` = 0,
  `secret` = '7GXXGUZJRD7OIVSC',
  `validation_key` = 'RTDR9e+FuzXjWtmnAtt7GsLvLaz8my6MMsyOQg2AljDDLnOhbKzIBx5TJTws0EIk3qnwW6DICsSN8yjcQg8gRQ==',
  `validation_key_updated` = '2026-07-16 08:29:56',
  `validation_key_type` = 'EMAIL',
  `mode` = 0,
  `password_changed` = '2026-07-16 08:29:56',
  `notes` = NULL,
  `max_num_projects` = 10,
  `num_active_projects` = 0,
  `two_factor` = 0,
  `tours_state` = 0,
  `salt` = '4TkNzunBP1QMtOyDbzGr58vpjybPcsufGc9kn3ocsrTikcHAF+2Vi4g0dN7/JvmA3vqYFFwNGCunxcE1enMEOw==',
  `last_visited_at` = '2026-07-16 08:29:56';
INSERT INTO `users` SET
  `uid` = 100010,
  `username` = 'userk000',
  `password` = '04a39ffa123654444bf4471f6ad6a7c4b97a77aeae8a13986cdd08016962d523',
  `email` = 'userk@lc.com',
  `fname` = 'userk',
  `lname` = 'lc',
  `activated` = '2026-07-16 08:29:58',
  `title` = '-',
  `false_login` = 0,
  `status` = 2,
  `isonline` = 1,
  `secret` = '2IXJVMHXV4JEFCGG',
  `validation_key` = 'rQTKXXW0ISSwap2F2G6dRcLZTyW+Yc4AkogcvvOTOhwWVouys0I1Kq2bx0+mw6AKewowRiLM6icCyTk5U/ynTg==',
  `validation_key_updated` = '2026-07-16 08:29:58',
  `validation_key_type` = 'EMAIL',
  `mode` = 0,
  `password_changed` = '2026-07-16 08:29:58',
  `notes` = NULL,
  `max_num_projects` = 10,
  `num_active_projects` = 0,
  `two_factor` = 0,
  `tours_state` = 0,
  `salt` = 'hkVA1Klaosaa9Jm5HxgBbfkNLGz8QzkClnasSQ/qkgnyX7QpSYJup9kjMtkMmv1eSGOmU97zdyN3l7taH+TH+Q==',
  `last_visited_at` = '2026-07-16 08:29:58';
INSERT INTO `users` SET
  `uid` = 100011,
  `username` = 'userl000',
  `password` = 'ce38d3d63ce6e1fb48e5fe05f7322d03d674c4fdea734414df5851e2bbb1edc8',
  `email` = 'userl@lc.com',
  `fname` = 'userl',
  `lname` = 'lc',
  `activated` = '2026-07-16 08:29:59',
  `title` = '-',
  `false_login` = 0,
  `status` = 2,
  `isonline` = 1,
  `secret` = 'J2ALTOGFJOD2PXXA',
  `validation_key` = 'I/X3dIM1FUaon/aaj29/RSG2koUNjOF+ZRfJG7SaaqHjqJYZl3n7y4U1y8bvBKHqKSXyZk16NehAK9x/sQ9IeA==',
  `validation_key_updated` = '2026-07-16 08:29:59',
  `validation_key_type` = 'EMAIL',
  `mode` = 0,
  `password_changed` = '2026-07-16 08:29:59',
  `notes` = NULL,
  `max_num_projects` = 10,
  `num_active_projects` = 0,
  `two_factor` = 0,
  `tours_state` = 0,
  `salt` = 'BQpvTNf5Lh4zT/k9E2JDZv8D8UGeMWwHTSovObJncYFWj0gtq/zmJKz0iTcW6LNXNLNpBePaHAPaShwy8C+JPQ==',
  `last_visited_at` = '2026-07-16 08:29:59';
-- userm added 2026-07-17 for scenario D5 (recording_D5.json): restricted
-- member of usera_project who also owns his own userm_project
INSERT INTO `users` SET
  `uid` = 100012,
  `username` = 'userm000',
  `password` = 'f67295abac57690aa8028c825b1520da10e41324e65cf2a793b951b2ef833831',
  `email` = 'userm@lc.com',
  `fname` = 'userm',
  `lname` = 'lc',
  `activated` = '2026-07-17 08:38:13',
  `title` = '-',
  `false_login` = 0,
  `status` = 2,
  `isonline` = 1,
  `secret` = 'NX4N2YSR6B2QA4J4',
  `validation_key` = 'EmbzfB+0/SueGAAI+Ag4EchGIVeNUn/hgt6/REFXLJ30BR+mGf4J9dXWMjbMLfv5OviEN+IQ5lBrUhb5egIM+Q==',
  `validation_key_updated` = '2026-07-17 08:38:13',
  `validation_key_type` = 'EMAIL',
  `mode` = 0,
  `password_changed` = '2026-07-17 08:38:13',
  `notes` = NULL,
  `max_num_projects` = 10,
  `num_active_projects` = 1,
  `two_factor` = 0,
  `tours_state` = 0,
  `salt` = 'JubtIqjB/j54LXC6LKQzXHIwvoiz0XhrgdJ3J4thO/8gSAvG8K+gNKZLYyyvCpKMFLNKkXXwMNgsVDwScQIBBQ==',
  `last_visited_at` = '2026-07-17 08:38:13';

-- ---- project (7 rows) ----

INSERT INTO `project` SET
  `id` = 100000,
  `projectname` = 'usera_project',
  `username` = 'usera@lc.com',
  `created` = '2026-07-16 08:40:44',
  `description` = 'sharing test project',
  `payment_type` = 'NOLIMIT',
  `last_quota_update` = '2026-07-16 08:40:46',
  `kafka_max_num_topics` = 100,
  `topic_name` = NULL,
  `creation_status` = 0,
  `online_feature_store_available` = 1;
INSERT INTO `project` SET
  `id` = 100001,
  `projectname` = 'userb_project',
  `username` = 'userb@lc.com',
  `created` = '2026-07-16 08:41:06',
  `description` = 'sharing test project',
  `payment_type` = 'NOLIMIT',
  `last_quota_update` = '2026-07-16 08:41:08',
  `kafka_max_num_topics` = 100,
  `topic_name` = NULL,
  `creation_status` = 0,
  `online_feature_store_available` = 1;
INSERT INTO `project` SET
  `id` = 100002,
  `projectname` = 'userc_project',
  `username` = 'userc@lc.com',
  `created` = '2026-07-16 08:41:25',
  `description` = 'sharing test project',
  `payment_type` = 'NOLIMIT',
  `last_quota_update` = '2026-07-16 08:41:27',
  `kafka_max_num_topics` = 100,
  `topic_name` = NULL,
  `creation_status` = 0,
  `online_feature_store_available` = 1;
INSERT INTO `project` SET
  `id` = 100003,
  `projectname` = 'userd_project',
  `username` = 'userd@lc.com',
  `created` = '2026-07-16 08:41:44',
  `description` = 'sharing test project',
  `payment_type` = 'NOLIMIT',
  `last_quota_update` = '2026-07-16 08:41:46',
  `kafka_max_num_topics` = 100,
  `topic_name` = NULL,
  `creation_status` = 0,
  `online_feature_store_available` = 1;
INSERT INTO `project` SET
  `id` = 100004,
  `projectname` = 'usere_project',
  `username` = 'usere@lc.com',
  `created` = '2026-07-16 08:42:02',
  `description` = 'sharing test project',
  `payment_type` = 'NOLIMIT',
  `last_quota_update` = '2026-07-16 08:42:04',
  `kafka_max_num_topics` = 100,
  `topic_name` = NULL,
  `creation_status` = 0,
  `online_feature_store_available` = 1;
INSERT INTO `project` SET
  `id` = 100005,
  `projectname` = 'userf_project',
  `username` = 'userf@lc.com',
  `created` = '2026-07-16 08:42:20',
  `description` = 'sharing test project',
  `payment_type` = 'NOLIMIT',
  `last_quota_update` = '2026-07-16 08:42:22',
  `kafka_max_num_topics` = 100,
  `topic_name` = NULL,
  `creation_status` = 0,
  `online_feature_store_available` = 1;
-- userm's own project (D5); no online DB - it holds no feature groups
INSERT INTO `project` SET
  `id` = 100006,
  `projectname` = 'userm_project',
  `username` = 'userm@lc.com',
  `created` = '2026-07-17 08:39:25',
  `description` = 'D5: restricted member of usera_project with own project',
  `payment_type` = 'NOLIMIT',
  `last_quota_update` = '2026-07-17 08:39:27',
  `kafka_max_num_topics` = 100,
  `topic_name` = NULL,
  `creation_status` = 0,
  `online_feature_store_available` = 0;

-- ---- project_team (11 rows) ----

INSERT INTO `project_team` SET
  `project_id` = 100000,
  `team_member` = 'usera@lc.com',
  `team_role` = 'Data owner',
  `added` = '2026-07-16 08:40:46';
INSERT INTO `project_team` SET
  `project_id` = 100000,
  `team_member` = 'userj@lc.com',
  `team_role` = 'Feature store restricted',
  `added` = '2026-07-16 08:48:51';
INSERT INTO `project_team` SET
  `project_id` = 100000,
  `team_member` = 'userk@lc.com',
  `team_role` = 'Feature store restricted',
  `added` = '2026-07-16 08:48:58';
INSERT INTO `project_team` SET
  `project_id` = 100000,
  `team_member` = 'userl@lc.com',
  `team_role` = 'Feature store restricted',
  `added` = '2026-07-16 08:49:07';
INSERT INTO `project_team` SET
  `project_id` = 100001,
  `team_member` = 'userb@lc.com',
  `team_role` = 'Data owner',
  `added` = '2026-07-16 08:41:08';
INSERT INTO `project_team` SET
  `project_id` = 100002,
  `team_member` = 'userc@lc.com',
  `team_role` = 'Data owner',
  `added` = '2026-07-16 08:41:27';
INSERT INTO `project_team` SET
  `project_id` = 100003,
  `team_member` = 'userd@lc.com',
  `team_role` = 'Data owner',
  `added` = '2026-07-16 08:41:46';
INSERT INTO `project_team` SET
  `project_id` = 100004,
  `team_member` = 'usere@lc.com',
  `team_role` = 'Data owner',
  `added` = '2026-07-16 08:42:04';
INSERT INTO `project_team` SET
  `project_id` = 100005,
  `team_member` = 'userf@lc.com',
  `team_role` = 'Data owner',
  `added` = '2026-07-16 08:42:22';
-- userm (D5): normal owner of his own project, restricted in usera's
INSERT INTO `project_team` SET
  `project_id` = 100006,
  `team_member` = 'userm@lc.com',
  `team_role` = 'Data owner',
  `added` = '2026-07-17 08:39:27';
INSERT INTO `project_team` SET
  `project_id` = 100000,
  `team_member` = 'userm@lc.com',
  `team_role` = 'Feature store restricted',
  `added` = '2026-07-17 08:39:10';

-- ---- feature_store (7 rows) ----

INSERT INTO `feature_store` SET
  `id` = 100000,
  `name` = 'usera_project',
  `project_id` = 100000,
  `created` = '2026-07-16 08:40:58';
INSERT INTO `feature_store` SET
  `id` = 100001,
  `name` = 'userb_project',
  `project_id` = 100001,
  `created` = '2026-07-16 08:41:17';
INSERT INTO `feature_store` SET
  `id` = 100002,
  `name` = 'userc_project',
  `project_id` = 100002,
  `created` = '2026-07-16 08:41:36';
INSERT INTO `feature_store` SET
  `id` = 100003,
  `name` = 'userd_project',
  `project_id` = 100003,
  `created` = '2026-07-16 08:41:55';
INSERT INTO `feature_store` SET
  `id` = 100004,
  `name` = 'usere_project',
  `project_id` = 100004,
  `created` = '2026-07-16 08:42:13';
INSERT INTO `feature_store` SET
  `id` = 100005,
  `name` = 'userf_project',
  `project_id` = 100005,
  `created` = '2026-07-16 08:42:31';
INSERT INTO `feature_store` SET
  `id` = 100006,
  `name` = 'userm_project',
  `project_id` = 100006,
  `created` = '2026-07-17 08:39:35';

-- ---- schemas (2 rows) ----

INSERT INTO `schemas` SET
  `id` = 100024,
  `schema` = '{\"type\":\"record\",\"name\":\"usera_customers_fg_1\",\"namespace\":\"usera_project_featurestore.db\",\"fields\":[{\"name\":\"customer_id\",\"type\":[\"null\",\"long\"]},{\"name\":\"age\",\"type\":[\"null\",\"long\"]},{\"name\":\"country\",\"type\":[\"null\",\"string\"]},{\"name\":\"is_premium\",\"type\":[\"null\",\"long\"]},{\"name\":\"event_time\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}]}]}',
  `project_id` = 100000;
INSERT INTO `schemas` SET
  `id` = 100025,
  `schema` = '{\"type\":\"record\",\"name\":\"usera_transactions_fg_1\",\"namespace\":\"usera_project_featurestore.db\",\"fields\":[{\"name\":\"customer_id\",\"type\":[\"null\",\"long\"]},{\"name\":\"num_transactions_30d\",\"type\":[\"null\",\"long\"]},{\"name\":\"total_spend_30d\",\"type\":[\"null\",\"double\"]},{\"name\":\"avg_transaction_value_30d\",\"type\":[\"null\",\"double\"]},{\"name\":\"event_time\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}]}]}',
  `project_id` = 100000;

-- ---- subjects (2 rows) ----

INSERT INTO `subjects` SET
  `id` = 100024,
  `subject` = 'usera_customers_fg_1',
  `version` = 1,
  `schema_id` = 100024,
  `project_id` = 100000,
  `created_on` = '2026-07-16 08:53:38';
INSERT INTO `subjects` SET
  `id` = 100025,
  `subject` = 'usera_transactions_fg_1',
  `version` = 1,
  `schema_id` = 100025,
  `project_id` = 100000,
  `created_on` = '2026-07-16 08:53:54';

-- ---- feature_group (2 rows) ----

INSERT INTO `feature_group` SET
  `id` = 100000,
  `name` = 'usera_customers_fg',
  `feature_store_id` = 100000,
  `created` = '2026-07-16 08:53:36',
  `creator` = 100000,
  `version` = 1,
  `description` = 'Customer profile features',
  `feature_group_type` = 0,
  `on_demand_feature_group_id` = NULL,
  `cached_feature_group_id` = 100000,
  `stream_feature_group_id` = NULL,
  `event_time` = 'event_time',
  `online_enabled` = 1,
  `topic_name` = NULL,
  `notification_topic_name` = NULL,
  `deprecated` = 0,
  `for_log` = 0,
  `data_source_id` = 100000,
  `ttl` = NULL,
  `ttl_enabled` = 0,
  `sink_enabled` = 0;
INSERT INTO `feature_group` SET
  `id` = 100001,
  `name` = 'usera_transactions_fg',
  `feature_store_id` = 100000,
  `created` = '2026-07-16 08:53:53',
  `creator` = 100000,
  `version` = 1,
  `description` = 'Customer transaction aggregate features',
  `feature_group_type` = 0,
  `on_demand_feature_group_id` = NULL,
  `cached_feature_group_id` = 100001,
  `stream_feature_group_id` = NULL,
  `event_time` = 'event_time',
  `online_enabled` = 1,
  `topic_name` = NULL,
  `notification_topic_name` = NULL,
  `deprecated` = 0,
  `for_log` = 0,
  `data_source_id` = 100001,
  `ttl` = NULL,
  `ttl_enabled` = 0,
  `sink_enabled` = 0;

-- ---- cached_feature_group (2 rows) ----

INSERT INTO `cached_feature_group` SET
  `id` = 100000,
  `timetravel_format` = 2;
INSERT INTO `cached_feature_group` SET
  `id` = 100001,
  `timetravel_format` = 2;

-- ---- feature_view (6 rows) ----

INSERT INTO `feature_view` SET
  `id` = 100000,
  `name` = 'usera_customers_transactions_fv',
  `feature_store_id` = 100000,
  `created` = '2026-07-16 08:54:07',
  `creator` = 100000,
  `version` = 1,
  `description` = 'All columns of customers + transactions';
INSERT INTO `feature_view` SET
  `id` = 100001,
  `name` = 'usera_txncount_fv',
  `feature_store_id` = 100000,
  `created` = '2026-07-16 08:54:10',
  `creator` = 100000,
  `version` = 1,
  `description` = 'Only num_transactions_30d from transactions';
INSERT INTO `feature_view` SET
  `id` = 100002,
  `name` = 'userc_own_fv',
  `feature_store_id` = 100002,
  `created` = '2026-07-16 11:01:38',
  `creator` = 100002,
  `version` = 1,
  `description` = 'consumer-side FV over usera FGs (T2 scenario F)';
INSERT INTO `feature_view` SET
  `id` = 100003,
  `name` = 'usere_own_fv',
  `feature_store_id` = 100004,
  `created` = '2026-07-16 11:01:48',
  `creator` = 100004,
  `version` = 1,
  `description` = 'consumer-side FV over usera FGs (T2 scenario F)';
INSERT INTO `feature_view` SET
  `id` = 100004,
  `name` = 'userl_own_fv',
  `feature_store_id` = 100000,
  `created` = '2026-07-16 11:02:05',
  `creator` = 100011,
  `version` = 1,
  `description` = 'consumer-side FV over usera FGs (T2 scenario F)';
INSERT INTO `feature_view` SET
  `id` = 100005,
  `name` = 'userk_own_fv',
  `feature_store_id` = 100000,
  `created` = '2026-07-16 11:03:30',
  `creator` = 100010,
  `version` = 1,
  `description` = 'consumer-side FV over usera FGs (T2 scenario F)';

-- ---- training_dataset_join (11 rows) ----

INSERT INTO `training_dataset_join` SET
  `id` = 100000,
  `training_dataset` = NULL,
  `feature_group` = 100001,
  `left_feature_group` = 100000,
  `feature_group_commit_id` = NULL,
  `type` = 3,
  `idx` = 1,
  `parent_idx` = 0,
  `prefix` = NULL,
  `feature_view_id` = 100000,
  `lookback_key` = NULL,
  `lookback_start_window` = NULL,
  `lookback_end_window` = NULL;
INSERT INTO `training_dataset_join` SET
  `id` = 100001,
  `training_dataset` = NULL,
  `feature_group` = 100000,
  `left_feature_group` = NULL,
  `feature_group_commit_id` = NULL,
  `type` = 0,
  `idx` = 0,
  `parent_idx` = 0,
  `prefix` = NULL,
  `feature_view_id` = 100000,
  `lookback_key` = NULL,
  `lookback_start_window` = NULL,
  `lookback_end_window` = NULL;
INSERT INTO `training_dataset_join` SET
  `id` = 100002,
  `training_dataset` = NULL,
  `feature_group` = 100001,
  `left_feature_group` = 100000,
  `feature_group_commit_id` = NULL,
  `type` = 3,
  `idx` = 1,
  `parent_idx` = 0,
  `prefix` = NULL,
  `feature_view_id` = 100001,
  `lookback_key` = NULL,
  `lookback_start_window` = NULL,
  `lookback_end_window` = NULL;
INSERT INTO `training_dataset_join` SET
  `id` = 100003,
  `training_dataset` = NULL,
  `feature_group` = 100000,
  `left_feature_group` = NULL,
  `feature_group_commit_id` = NULL,
  `type` = 0,
  `idx` = 0,
  `parent_idx` = 0,
  `prefix` = NULL,
  `feature_view_id` = 100001,
  `lookback_key` = NULL,
  `lookback_start_window` = NULL,
  `lookback_end_window` = NULL;
INSERT INTO `training_dataset_join` SET
  `id` = 100004,
  `training_dataset` = NULL,
  `feature_group` = 100001,
  `left_feature_group` = 100000,
  `feature_group_commit_id` = NULL,
  `type` = 3,
  `idx` = 1,
  `parent_idx` = 0,
  `prefix` = NULL,
  `feature_view_id` = 100002,
  `lookback_key` = NULL,
  `lookback_start_window` = NULL,
  `lookback_end_window` = NULL;
INSERT INTO `training_dataset_join` SET
  `id` = 100005,
  `training_dataset` = NULL,
  `feature_group` = 100000,
  `left_feature_group` = NULL,
  `feature_group_commit_id` = NULL,
  `type` = 0,
  `idx` = 0,
  `parent_idx` = 0,
  `prefix` = NULL,
  `feature_view_id` = 100002,
  `lookback_key` = NULL,
  `lookback_start_window` = NULL,
  `lookback_end_window` = NULL;
INSERT INTO `training_dataset_join` SET
  `id` = 100006,
  `training_dataset` = NULL,
  `feature_group` = 100000,
  `left_feature_group` = NULL,
  `feature_group_commit_id` = NULL,
  `type` = 0,
  `idx` = 0,
  `parent_idx` = 0,
  `prefix` = NULL,
  `feature_view_id` = 100003,
  `lookback_key` = NULL,
  `lookback_start_window` = NULL,
  `lookback_end_window` = NULL;
INSERT INTO `training_dataset_join` SET
  `id` = 100007,
  `training_dataset` = NULL,
  `feature_group` = 100001,
  `left_feature_group` = 100000,
  `feature_group_commit_id` = NULL,
  `type` = 3,
  `idx` = 1,
  `parent_idx` = 0,
  `prefix` = NULL,
  `feature_view_id` = 100003,
  `lookback_key` = NULL,
  `lookback_start_window` = NULL,
  `lookback_end_window` = NULL;
INSERT INTO `training_dataset_join` SET
  `id` = 100008,
  `training_dataset` = NULL,
  `feature_group` = 100001,
  `left_feature_group` = 100000,
  `feature_group_commit_id` = NULL,
  `type` = 3,
  `idx` = 1,
  `parent_idx` = 0,
  `prefix` = NULL,
  `feature_view_id` = 100004,
  `lookback_key` = NULL,
  `lookback_start_window` = NULL,
  `lookback_end_window` = NULL;
INSERT INTO `training_dataset_join` SET
  `id` = 100009,
  `training_dataset` = NULL,
  `feature_group` = 100000,
  `left_feature_group` = NULL,
  `feature_group_commit_id` = NULL,
  `type` = 0,
  `idx` = 0,
  `parent_idx` = 0,
  `prefix` = NULL,
  `feature_view_id` = 100004,
  `lookback_key` = NULL,
  `lookback_start_window` = NULL,
  `lookback_end_window` = NULL;
INSERT INTO `training_dataset_join` SET
  `id` = 100010,
  `training_dataset` = NULL,
  `feature_group` = 100000,
  `left_feature_group` = NULL,
  `feature_group_commit_id` = NULL,
  `type` = 0,
  `idx` = 0,
  `parent_idx` = 0,
  `prefix` = NULL,
  `feature_view_id` = 100005,
  `lookback_key` = NULL,
  `lookback_start_window` = NULL,
  `lookback_end_window` = NULL;

-- ---- training_dataset_feature (25 rows) ----

INSERT INTO `training_dataset_feature` SET
  `id` = 100000,
  `training_dataset` = NULL,
  `feature_group` = 100001,
  `name` = 'avg_transaction_value_30d',
  `type` = 'double',
  `td_join` = 100000,
  `idx` = 6,
  `label` = 0,
  `inference_helper_column` = 0,
  `training_helper_column` = 0,
  `feature_view_id` = 100000,
  `on_demand_transformation` = NULL;
INSERT INTO `training_dataset_feature` SET
  `id` = 100001,
  `training_dataset` = NULL,
  `feature_group` = 100000,
  `name` = 'country',
  `type` = 'string',
  `td_join` = 100001,
  `idx` = 2,
  `label` = 0,
  `inference_helper_column` = 0,
  `training_helper_column` = 0,
  `feature_view_id` = 100000,
  `on_demand_transformation` = NULL;
INSERT INTO `training_dataset_feature` SET
  `id` = 100002,
  `training_dataset` = NULL,
  `feature_group` = 100000,
  `name` = 'age',
  `type` = 'bigint',
  `td_join` = 100001,
  `idx` = 1,
  `label` = 0,
  `inference_helper_column` = 0,
  `training_helper_column` = 0,
  `feature_view_id` = 100000,
  `on_demand_transformation` = NULL;
INSERT INTO `training_dataset_feature` SET
  `id` = 100003,
  `training_dataset` = NULL,
  `feature_group` = 100000,
  `name` = 'customer_id',
  `type` = 'bigint',
  `td_join` = 100001,
  `idx` = 0,
  `label` = 0,
  `inference_helper_column` = 0,
  `training_helper_column` = 0,
  `feature_view_id` = 100000,
  `on_demand_transformation` = NULL;
INSERT INTO `training_dataset_feature` SET
  `id` = 100004,
  `training_dataset` = NULL,
  `feature_group` = 100001,
  `name` = 'total_spend_30d',
  `type` = 'double',
  `td_join` = 100000,
  `idx` = 5,
  `label` = 0,
  `inference_helper_column` = 0,
  `training_helper_column` = 0,
  `feature_view_id` = 100000,
  `on_demand_transformation` = NULL;
INSERT INTO `training_dataset_feature` SET
  `id` = 100005,
  `training_dataset` = NULL,
  `feature_group` = 100001,
  `name` = 'num_transactions_30d',
  `type` = 'bigint',
  `td_join` = 100000,
  `idx` = 4,
  `label` = 0,
  `inference_helper_column` = 0,
  `training_helper_column` = 0,
  `feature_view_id` = 100000,
  `on_demand_transformation` = NULL;
INSERT INTO `training_dataset_feature` SET
  `id` = 100006,
  `training_dataset` = NULL,
  `feature_group` = 100000,
  `name` = 'is_premium',
  `type` = 'bigint',
  `td_join` = 100001,
  `idx` = 3,
  `label` = 0,
  `inference_helper_column` = 0,
  `training_helper_column` = 0,
  `feature_view_id` = 100000,
  `on_demand_transformation` = NULL;
INSERT INTO `training_dataset_feature` SET
  `id` = 100007,
  `training_dataset` = NULL,
  `feature_group` = 100000,
  `name` = 'age',
  `type` = 'bigint',
  `td_join` = 100003,
  `idx` = 1,
  `label` = 0,
  `inference_helper_column` = 0,
  `training_helper_column` = 0,
  `feature_view_id` = 100001,
  `on_demand_transformation` = NULL;
INSERT INTO `training_dataset_feature` SET
  `id` = 100008,
  `training_dataset` = NULL,
  `feature_group` = 100001,
  `name` = 'num_transactions_30d',
  `type` = 'bigint',
  `td_join` = 100002,
  `idx` = 2,
  `label` = 0,
  `inference_helper_column` = 0,
  `training_helper_column` = 0,
  `feature_view_id` = 100001,
  `on_demand_transformation` = NULL;
INSERT INTO `training_dataset_feature` SET
  `id` = 100009,
  `training_dataset` = NULL,
  `feature_group` = 100000,
  `name` = 'customer_id',
  `type` = 'bigint',
  `td_join` = 100003,
  `idx` = 0,
  `label` = 0,
  `inference_helper_column` = 0,
  `training_helper_column` = 0,
  `feature_view_id` = 100001,
  `on_demand_transformation` = NULL;
INSERT INTO `training_dataset_feature` SET
  `id` = 100010,
  `training_dataset` = NULL,
  `feature_group` = 100001,
  `name` = 'avg_transaction_value_30d',
  `type` = 'double',
  `td_join` = 100004,
  `idx` = 6,
  `label` = 0,
  `inference_helper_column` = 0,
  `training_helper_column` = 0,
  `feature_view_id` = 100002,
  `on_demand_transformation` = NULL;
INSERT INTO `training_dataset_feature` SET
  `id` = 100011,
  `training_dataset` = NULL,
  `feature_group` = 100000,
  `name` = 'country',
  `type` = 'string',
  `td_join` = 100005,
  `idx` = 2,
  `label` = 0,
  `inference_helper_column` = 0,
  `training_helper_column` = 0,
  `feature_view_id` = 100002,
  `on_demand_transformation` = NULL;
INSERT INTO `training_dataset_feature` SET
  `id` = 100012,
  `training_dataset` = NULL,
  `feature_group` = 100000,
  `name` = 'age',
  `type` = 'bigint',
  `td_join` = 100005,
  `idx` = 1,
  `label` = 0,
  `inference_helper_column` = 0,
  `training_helper_column` = 0,
  `feature_view_id` = 100002,
  `on_demand_transformation` = NULL;
INSERT INTO `training_dataset_feature` SET
  `id` = 100013,
  `training_dataset` = NULL,
  `feature_group` = 100000,
  `name` = 'customer_id',
  `type` = 'bigint',
  `td_join` = 100005,
  `idx` = 0,
  `label` = 0,
  `inference_helper_column` = 0,
  `training_helper_column` = 0,
  `feature_view_id` = 100002,
  `on_demand_transformation` = NULL;
INSERT INTO `training_dataset_feature` SET
  `id` = 100014,
  `training_dataset` = NULL,
  `feature_group` = 100001,
  `name` = 'num_transactions_30d',
  `type` = 'bigint',
  `td_join` = 100004,
  `idx` = 4,
  `label` = 0,
  `inference_helper_column` = 0,
  `training_helper_column` = 0,
  `feature_view_id` = 100002,
  `on_demand_transformation` = NULL;
INSERT INTO `training_dataset_feature` SET
  `id` = 100015,
  `training_dataset` = NULL,
  `feature_group` = 100001,
  `name` = 'total_spend_30d',
  `type` = 'double',
  `td_join` = 100004,
  `idx` = 5,
  `label` = 0,
  `inference_helper_column` = 0,
  `training_helper_column` = 0,
  `feature_view_id` = 100002,
  `on_demand_transformation` = NULL;
INSERT INTO `training_dataset_feature` SET
  `id` = 100016,
  `training_dataset` = NULL,
  `feature_group` = 100000,
  `name` = 'is_premium',
  `type` = 'bigint',
  `td_join` = 100005,
  `idx` = 3,
  `label` = 0,
  `inference_helper_column` = 0,
  `training_helper_column` = 0,
  `feature_view_id` = 100002,
  `on_demand_transformation` = NULL;
INSERT INTO `training_dataset_feature` SET
  `id` = 100017,
  `training_dataset` = NULL,
  `feature_group` = 100000,
  `name` = 'customer_id',
  `type` = 'bigint',
  `td_join` = 100006,
  `idx` = 0,
  `label` = 0,
  `inference_helper_column` = 0,
  `training_helper_column` = 0,
  `feature_view_id` = 100003,
  `on_demand_transformation` = NULL;
INSERT INTO `training_dataset_feature` SET
  `id` = 100018,
  `training_dataset` = NULL,
  `feature_group` = 100001,
  `name` = 'num_transactions_30d',
  `type` = 'bigint',
  `td_join` = 100007,
  `idx` = 2,
  `label` = 0,
  `inference_helper_column` = 0,
  `training_helper_column` = 0,
  `feature_view_id` = 100003,
  `on_demand_transformation` = NULL;
INSERT INTO `training_dataset_feature` SET
  `id` = 100019,
  `training_dataset` = NULL,
  `feature_group` = 100000,
  `name` = 'age',
  `type` = 'bigint',
  `td_join` = 100006,
  `idx` = 1,
  `label` = 0,
  `inference_helper_column` = 0,
  `training_helper_column` = 0,
  `feature_view_id` = 100003,
  `on_demand_transformation` = NULL;
INSERT INTO `training_dataset_feature` SET
  `id` = 100020,
  `training_dataset` = NULL,
  `feature_group` = 100000,
  `name` = 'age',
  `type` = 'bigint',
  `td_join` = 100009,
  `idx` = 1,
  `label` = 0,
  `inference_helper_column` = 0,
  `training_helper_column` = 0,
  `feature_view_id` = 100004,
  `on_demand_transformation` = NULL;
INSERT INTO `training_dataset_feature` SET
  `id` = 100021,
  `training_dataset` = NULL,
  `feature_group` = 100001,
  `name` = 'num_transactions_30d',
  `type` = 'bigint',
  `td_join` = 100008,
  `idx` = 2,
  `label` = 0,
  `inference_helper_column` = 0,
  `training_helper_column` = 0,
  `feature_view_id` = 100004,
  `on_demand_transformation` = NULL;
INSERT INTO `training_dataset_feature` SET
  `id` = 100022,
  `training_dataset` = NULL,
  `feature_group` = 100000,
  `name` = 'customer_id',
  `type` = 'bigint',
  `td_join` = 100009,
  `idx` = 0,
  `label` = 0,
  `inference_helper_column` = 0,
  `training_helper_column` = 0,
  `feature_view_id` = 100004,
  `on_demand_transformation` = NULL;
INSERT INTO `training_dataset_feature` SET
  `id` = 100023,
  `training_dataset` = NULL,
  `feature_group` = 100000,
  `name` = 'customer_id',
  `type` = 'bigint',
  `td_join` = 100010,
  `idx` = 0,
  `label` = 0,
  `inference_helper_column` = 0,
  `training_helper_column` = 0,
  `feature_view_id` = 100005,
  `on_demand_transformation` = NULL;
INSERT INTO `training_dataset_feature` SET
  `id` = 100024,
  `training_dataset` = NULL,
  `feature_group` = 100000,
  `name` = 'age',
  `type` = 'bigint',
  `td_join` = 100010,
  `idx` = 1,
  `label` = 0,
  `inference_helper_column` = 0,
  `training_helper_column` = 0,
  `feature_view_id` = 100005,
  `on_demand_transformation` = NULL;

-- ---- serving_key (11 rows) ----

INSERT INTO `serving_key` SET
  `id` = 100000,
  `prefix` = NULL,
  `feature_name` = 'customer_id',
  `join_on` = 'customer_id',
  `join_index` = 1,
  `feature_group_id` = 100001,
  `required` = 0,
  `feature_view_id` = 100000,
  `type` = 'bigint';
INSERT INTO `serving_key` SET
  `id` = 100001,
  `prefix` = NULL,
  `feature_name` = 'customer_id',
  `join_on` = NULL,
  `join_index` = 0,
  `feature_group_id` = 100000,
  `required` = 1,
  `feature_view_id` = 100000,
  `type` = 'bigint';
INSERT INTO `serving_key` SET
  `id` = 100002,
  `prefix` = NULL,
  `feature_name` = 'customer_id',
  `join_on` = 'customer_id',
  `join_index` = 1,
  `feature_group_id` = 100001,
  `required` = 0,
  `feature_view_id` = 100001,
  `type` = 'bigint';
INSERT INTO `serving_key` SET
  `id` = 100003,
  `prefix` = NULL,
  `feature_name` = 'customer_id',
  `join_on` = NULL,
  `join_index` = 0,
  `feature_group_id` = 100000,
  `required` = 1,
  `feature_view_id` = 100001,
  `type` = 'bigint';
INSERT INTO `serving_key` SET
  `id` = 100004,
  `prefix` = NULL,
  `feature_name` = 'customer_id',
  `join_on` = NULL,
  `join_index` = 0,
  `feature_group_id` = 100000,
  `required` = 1,
  `feature_view_id` = 100002,
  `type` = 'bigint';
INSERT INTO `serving_key` SET
  `id` = 100005,
  `prefix` = NULL,
  `feature_name` = 'customer_id',
  `join_on` = 'customer_id',
  `join_index` = 1,
  `feature_group_id` = 100001,
  `required` = 0,
  `feature_view_id` = 100002,
  `type` = 'bigint';
INSERT INTO `serving_key` SET
  `id` = 100006,
  `prefix` = NULL,
  `feature_name` = 'customer_id',
  `join_on` = NULL,
  `join_index` = 0,
  `feature_group_id` = 100000,
  `required` = 1,
  `feature_view_id` = 100003,
  `type` = 'bigint';
INSERT INTO `serving_key` SET
  `id` = 100007,
  `prefix` = NULL,
  `feature_name` = 'customer_id',
  `join_on` = 'customer_id',
  `join_index` = 1,
  `feature_group_id` = 100001,
  `required` = 0,
  `feature_view_id` = 100003,
  `type` = 'bigint';
INSERT INTO `serving_key` SET
  `id` = 100008,
  `prefix` = NULL,
  `feature_name` = 'customer_id',
  `join_on` = NULL,
  `join_index` = 0,
  `feature_group_id` = 100000,
  `required` = 1,
  `feature_view_id` = 100004,
  `type` = 'bigint';
INSERT INTO `serving_key` SET
  `id` = 100009,
  `prefix` = NULL,
  `feature_name` = 'customer_id',
  `join_on` = 'customer_id',
  `join_index` = 1,
  `feature_group_id` = 100001,
  `required` = 0,
  `feature_view_id` = 100004,
  `type` = 'bigint';
INSERT INTO `serving_key` SET
  `id` = 100010,
  `prefix` = NULL,
  `feature_name` = 'customer_id',
  `join_on` = NULL,
  `join_index` = 0,
  `feature_group_id` = 100000,
  `required` = 1,
  `feature_view_id` = 100005,
  `type` = 'bigint';

-- ---- api_key (12 rows) ----

INSERT INTO `api_key` SET
  `id` = 100012,
  `prefix` = 'ChwGeR9Hb49Krbm8',
  `secret` = 'ded5e195c8f54ccda5988fe959cc8163022a8ebf7612adc82cee7d3b6371bd21',
  `salt` = 'IaGEaMRCVvVgN7vnZ9pjRnINxz1IClylaustqfybScvT/gzn2J54H2yFVupnilNe0YWHboUJEmQjJOKc7+4Odw==',
  `created` = '2026-07-16 08:38:11',
  `modified` = '2026-07-16 08:38:11',
  `name` = 'usera_api_key',
  `user_id` = 100000,
  `reserved` = 0,
  `expiry` = NULL;
INSERT INTO `api_key` SET
  `id` = 100013,
  `prefix` = 'NJOTkByOD8jRApvR',
  `secret` = '0b64d5faeae7e16667d1433b2eb1612ed8ebd56b9802f53743d3b71bd62f37e8',
  `salt` = 'KmzSDT0IoQD2CYUtuwgbqqlkHa0UYQNT6eJBTNkwT9UE67pn8Y9qeXgo0NY3WlLE9RE1GJN2zlCZGA9NTxe9Ng==',
  `created` = '2026-07-16 08:38:12',
  `modified` = '2026-07-16 08:38:12',
  `name` = 'userb_api_key',
  `user_id` = 100001,
  `reserved` = 0,
  `expiry` = NULL;
INSERT INTO `api_key` SET
  `id` = 100014,
  `prefix` = 'keFK4Ay0SxBMNjVA',
  `secret` = 'c11c4f7863e7c3c46d01944fa45ae8829e62111054614594bb1a0c051a0f1176',
  `salt` = 'mKFRd6SBo3zhiUKsviBjOKw5K3dz2632CGztNnoiQQlRVlTXWUDWdCkljwuZvjFleHiWpPZ1FXgtQ79P678G8g==',
  `created` = '2026-07-16 08:38:12',
  `modified` = '2026-07-16 08:38:12',
  `name` = 'userc_api_key',
  `user_id` = 100002,
  `reserved` = 0,
  `expiry` = NULL;
INSERT INTO `api_key` SET
  `id` = 100015,
  `prefix` = 'RmanPkMndlooED6L',
  `secret` = '89d65f5b9c8443d1e4ed939a2c424b22d6ee200e6da592818eb7f1a65b35449d',
  `salt` = 'JAFnG2uTJ2tN8nRnWTfIUjlDbJ4kozB0ELFJg8bgXVC7wI2hegcbIaFENvz4Aihh0LzM9CLMOC7lyZL+ITKjXg==',
  `created` = '2026-07-16 08:38:12',
  `modified` = '2026-07-16 08:38:12',
  `name` = 'userd_api_key',
  `user_id` = 100003,
  `reserved` = 0,
  `expiry` = NULL;
INSERT INTO `api_key` SET
  `id` = 100016,
  `prefix` = 'lMweR23LdnqLr56h',
  `secret` = '7a5fab317cdc3d05ed12cf3dcc50f0b627187d5e862288a58b38b91bc7262c91',
  `salt` = 'tW75dThqKiu/wv8TDB+cCYfCKOmq45QUTL2kXJSPU50NiSaX1g/ov1PIQjsEiHnTTl2I1HejdhvcCc6BOfBIMg==',
  `created` = '2026-07-16 08:38:12',
  `modified` = '2026-07-16 08:38:12',
  `name` = 'usere_api_key',
  `user_id` = 100004,
  `reserved` = 0,
  `expiry` = NULL;
INSERT INTO `api_key` SET
  `id` = 100017,
  `prefix` = '18OJHwyAFFLOhswO',
  `secret` = '35fb261c9be77ee9cb599d41025f874e18846c1225ed9a9e4f2e6b6f5d3bf456',
  `salt` = '6w2oid9euAGqFKza3gG3toV0YDDFeMoy2qjuyCnd90Mdw4LIXQbKUPHVq3DUfGjfb32K+ZGBKZbDFglr1xw+Lg==',
  `created` = '2026-07-16 08:38:13',
  `modified` = '2026-07-16 08:38:13',
  `name` = 'userf_api_key',
  `user_id` = 100005,
  `reserved` = 0,
  `expiry` = NULL;
INSERT INTO `api_key` SET
  `id` = 100018,
  `prefix` = '70waGjjDEAC2RjS1',
  `secret` = 'd0ba4f44c440d03fae3ac0c21aadcdb67946ac9ad63a6f4c4f6e9382c4c8fd6c',
  `salt` = 'mnckFsQ2ll2XF/hFC8mx7Y9U4rbt3eXriZutkpy20GsxtkWFz1xAsPTgZlzbb8MuPdIxnPSB0FXzaQ1ZChbgLA==',
  `created` = '2026-07-16 08:38:13',
  `modified` = '2026-07-16 08:38:13',
  `name` = 'userg_api_key',
  `user_id` = 100006,
  `reserved` = 0,
  `expiry` = NULL;
INSERT INTO `api_key` SET
  `id` = 100019,
  `prefix` = 'senMNxLu0PggmzGZ',
  `secret` = '6af54eeb53de389cc7cf5665071c82e44e7563f0f5e97a13f001663c01954ebf',
  `salt` = '6h9/Qoyzpzjai++pP+uM3OMx47ko1x55l+ypmPAYepeAm9PDhVEVFgE8Djq3Y5LNfj2pqRYNEQw+dkxmCLLXEw==',
  `created` = '2026-07-16 08:38:13',
  `modified` = '2026-07-16 08:38:13',
  `name` = 'userh_api_key',
  `user_id` = 100007,
  `reserved` = 0,
  `expiry` = NULL;
INSERT INTO `api_key` SET
  `id` = 100020,
  `prefix` = 'npUExoYmLz4kJrJM',
  `secret` = '5a94c6467019bc994eb789a605a216bc4fc33cd2015d9ff9e7a53f75d498aadf',
  `salt` = 'lBSkJruW6wcQFJ2n/ylM3PMqXLGKBHIipADJLf2NVC8T2nhlwG6vgNmfCj9G/0/9q5QUffxQogeLahhR3bepiA==',
  `created` = '2026-07-16 08:38:13',
  `modified` = '2026-07-16 08:38:13',
  `name` = 'useri_api_key',
  `user_id` = 100008,
  `reserved` = 0,
  `expiry` = NULL;
INSERT INTO `api_key` SET
  `id` = 100021,
  `prefix` = 'AI074gWjUl5pZEa8',
  `secret` = '0c6145aa07b5af73941fc36067b1bba47b25201aaca81262e4d5813feb5154f0',
  `salt` = '61BPBoQkpz+0sFsP5NldRWxj3R63rVxhbr03BbjH4uQKpp8wM2h/qV66+vkAgVqnBbG047p1VT13akG0/R/VNA==',
  `created` = '2026-07-16 08:38:13',
  `modified` = '2026-07-16 08:38:13',
  `name` = 'userj_api_key',
  `user_id` = 100009,
  `reserved` = 0,
  `expiry` = NULL;
INSERT INTO `api_key` SET
  `id` = 100022,
  `prefix` = 'tr80O5ClBpOupXzI',
  `secret` = 'cf182115637923423e43a5d3066da2802850cb9ceb1fa8414b71b415ec6c9988',
  `salt` = 'uRkaHX4I8Tt2o7I0CVjBHeU329GLi7OX/SFyenTu61tlmHfFIqT0aAob1D4KPGL2w+0rUvfxL5LvisauKnaeFg==',
  `created` = '2026-07-16 08:38:14',
  `modified` = '2026-07-16 08:38:14',
  `name` = 'userk_api_key',
  `user_id` = 100010,
  `reserved` = 0,
  `expiry` = NULL;
INSERT INTO `api_key` SET
  `id` = 100023,
  `prefix` = 'yaRas68o9CmUeZDX',
  `secret` = '331bd0e068b2ae8b7965f71f49732a8a1246d7d791c93ea936bce45f40224a55',
  `salt` = 'ZCimFCzz6KIk8yKBFKI7zX94tO4AdzQ2Ps12V1mecPePJjYFbqxtiJ+sNrfrX3KYyTw7D07yzx9Ur0BCmprhVQ==',
  `created` = '2026-07-16 08:38:14',
  `modified` = '2026-07-16 08:38:14',
  `name` = 'userl_api_key',
  `user_id` = 100011,
  `reserved` = 0,
  `expiry` = NULL;
-- userm's key (D5); id 100025 - 100024 was consumed on the cluster by an
-- auto-created serving_ key, dropped per the cleaning rules
INSERT INTO `api_key` SET
  `id` = 100025,
  `prefix` = '9mNzTyVuQXb8Bwq3',
  `secret` = '08b8596f71ae84364ff929bff16edf63c7768e244acd9fc8bcda6ded8bc497d4',
  `salt` = 'FXr/xudIjpd+M63e+tvnOTB+tVohTHgaxK+85qCWd6LDUrwAMmY96ZCJbGEMCQt0SdhA4giU86HYz8niiVuAcw==',
  `created` = '2026-07-17 08:38:31',
  `modified` = '2026-07-17 08:38:31',
  `name` = 'userm_api_key',
  `user_id` = 100012,
  `reserved` = 0,
  `expiry` = NULL;

-- ---- shared_feature_store (4 rows) ----

INSERT INTO `shared_feature_store` SET
  `id` = 100001,
  `feature_store` = 100000,
  `shared_by` = 100000,
  `shared_on` = '2026-07-16 00:00:00',
  `shared_with_project` = 100001,
  `shared_entirely` = 1;
INSERT INTO `shared_feature_store` SET
  `id` = 100002,
  `feature_store` = 100000,
  `shared_by` = 100000,
  `shared_on` = '2026-07-16 00:00:00',
  `shared_with_project` = 100003,
  `shared_entirely` = 0;
INSERT INTO `shared_feature_store` SET
  `id` = 100003,
  `feature_store` = 100000,
  `shared_by` = 100000,
  `shared_on` = '2026-07-16 00:00:00',
  `shared_with_project` = 100002,
  `shared_entirely` = 0;
INSERT INTO `shared_feature_store` SET
  `id` = 100004,
  `feature_store` = 100000,
  `shared_by` = 100000,
  `shared_on` = '2026-07-16 00:00:00',
  `shared_with_project` = 100004,
  `shared_entirely` = 0;

-- ---- shared_feature_group (5 rows) ----

INSERT INTO `shared_feature_group` SET
  `id` = 100000,
  `feature_store` = 100000,
  `feature_group` = 100000,
  `shared_by` = 100000,
  `shared_on` = '2026-07-16 00:00:00',
  `shared_with_project` = 100003,
  `shared_entirely` = 1;
INSERT INTO `shared_feature_group` SET
  `id` = 100002,
  `feature_store` = 100000,
  `feature_group` = 100001,
  `shared_by` = 100000,
  `shared_on` = '2026-07-16 00:00:00',
  `shared_with_project` = 100002,
  `shared_entirely` = 1;
INSERT INTO `shared_feature_group` SET
  `id` = 100003,
  `feature_store` = 100000,
  `feature_group` = 100000,
  `shared_by` = 100000,
  `shared_on` = '2026-07-16 00:00:00',
  `shared_with_project` = 100002,
  `shared_entirely` = 1;
INSERT INTO `shared_feature_group` SET
  `id` = 100004,
  `feature_store` = 100000,
  `feature_group` = 100000,
  `shared_by` = 100000,
  `shared_on` = '2026-07-16 00:00:00',
  `shared_with_project` = 100004,
  `shared_entirely` = 1;
INSERT INTO `shared_feature_group` SET
  `id` = 100005,
  `feature_store` = 100000,
  `feature_group` = 100001,
  `shared_by` = 100000,
  `shared_on` = '2026-07-16 00:00:00',
  `shared_with_project` = 100004,
  `shared_entirely` = 0;

-- ---- shared_feature (3 rows) ----

INSERT INTO `shared_feature` SET
  `id` = 100000,
  `feature_group` = 100001,
  `feature` = 'customer_id',
  `shared_by` = 100000,
  `shared_on` = '2026-07-16 00:00:00',
  `shared_with_project` = 100004;
INSERT INTO `shared_feature` SET
  `id` = 100001,
  `feature_group` = 100001,
  `feature` = 'event_time',
  `shared_by` = 100000,
  `shared_on` = '2026-07-16 00:00:00',
  `shared_with_project` = 100004;
INSERT INTO `shared_feature` SET
  `id` = 100002,
  `feature_group` = 100001,
  `feature` = 'num_transactions_30d',
  `shared_by` = 100000,
  `shared_on` = '2026-07-16 00:00:00',
  `shared_with_project` = 100004;

-- ---- restricted_feature_group_access (3 rows) ----

INSERT INTO `restricted_feature_group_access` SET
  `id` = 100000,
  `feature_store` = 100000,
  `feature_group` = 100000,
  `granted_by` = 100000,
  `granted_on` = '2026-07-16 00:00:00',
  `granted_to_user` = 100010,
  `can_access_entirely` = 1;
INSERT INTO `restricted_feature_group_access` SET
  `id` = 100002,
  `feature_store` = 100000,
  `feature_group` = 100000,
  `granted_by` = 100000,
  `granted_on` = '2026-07-16 00:00:00',
  `granted_to_user` = 100011,
  `can_access_entirely` = 1;
INSERT INTO `restricted_feature_group_access` SET
  `id` = 100003,
  `feature_store` = 100000,
  `feature_group` = 100001,
  `granted_by` = 100000,
  `granted_on` = '2026-07-16 00:00:00',
  `granted_to_user` = 100011,
  `can_access_entirely` = 0;
-- userm (D5): customers entirely, mirror of userk's grant
INSERT INTO `restricted_feature_group_access` SET
  `id` = 100004,
  `feature_store` = 100000,
  `feature_group` = 100000,
  `granted_by` = 100000,
  `granted_on` = '2026-07-17 00:00:00',
  `granted_to_user` = 100012,
  `can_access_entirely` = 1;

-- ---- restricted_feature_access (3 rows) ----

INSERT INTO `restricted_feature_access` SET
  `id` = 100000,
  `restricted_feature_group_access` = 100003,
  `feature` = 'num_transactions_30d',
  `granted_by` = 100000,
  `granted_on` = '2026-07-16 00:00:00',
  `granted_to_user` = 100011;
INSERT INTO `restricted_feature_access` SET
  `id` = 100001,
  `restricted_feature_group_access` = 100003,
  `feature` = 'customer_id',
  `granted_by` = 100000,
  `granted_on` = '2026-07-16 00:00:00',
  `granted_to_user` = 100011;
INSERT INTO `restricted_feature_access` SET
  `id` = 100002,
  `restricted_feature_group_access` = 100003,
  `feature` = 'event_time',
  `granted_by` = 100000,
  `granted_on` = '2026-07-16 00:00:00',
  `granted_to_user` = 100011;

-- ============================================================
-- Online feature store database of the producer project.
-- Schema and data imported verbatim from
-- docs/fine_grained_recordings/final_state_usera_project_online.sql.
-- ============================================================

DROP DATABASE IF EXISTS usera_project;

CREATE DATABASE usera_project;

USE usera_project;

-- ---- usera_project.usera_customers_fg_1 ----

CREATE TABLE `usera_customers_fg_1` (
  `customer_id` bigint NOT NULL,
  `age` bigint DEFAULT NULL,
  `country` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `is_premium` bigint DEFAULT NULL,
  `event_time` timestamp NULL /*!50606 STORAGE MEMORY */ DEFAULT NULL,
  PRIMARY KEY (`customer_id`) USING HASH
) ENGINE=ndbcluster DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='NDB_TABLE=READ_BACKUP=1';

INSERT INTO `usera_customers_fg_1` SET
  `customer_id` = 3,
  `age` = 33,
  `country` = 'SE',
  `is_premium` = 1,
  `event_time` = '2026-07-16 08:53:35';
INSERT INTO `usera_customers_fg_1` SET
  `customer_id` = 1,
  `age` = 25,
  `country` = 'SE',
  `is_premium` = 0,
  `event_time` = '2026-07-16 08:53:35';
INSERT INTO `usera_customers_fg_1` SET
  `customer_id` = 2,
  `age` = 41,
  `country` = 'PK',
  `is_premium` = 1,
  `event_time` = '2026-07-16 08:53:35';
INSERT INTO `usera_customers_fg_1` SET
  `customer_id` = 4,
  `age` = 52,
  `country` = 'NO',
  `is_premium` = 0,
  `event_time` = '2026-07-16 08:53:35';

-- ---- usera_project.usera_transactions_fg_1 ----

CREATE TABLE `usera_transactions_fg_1` (
  `customer_id` bigint NOT NULL,
  `num_transactions_30d` bigint DEFAULT NULL,
  `total_spend_30d` double DEFAULT NULL,
  `avg_transaction_value_30d` double DEFAULT NULL,
  `event_time` timestamp NULL /*!50606 STORAGE MEMORY */ DEFAULT NULL,
  PRIMARY KEY (`customer_id`) USING HASH
) ENGINE=ndbcluster DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='NDB_TABLE=READ_BACKUP=1';

INSERT INTO `usera_transactions_fg_1` SET
  `customer_id` = 3,
  `num_transactions_30d` = 12,
  `total_spend_30d` = 880.2,
  `avg_transaction_value_30d` = 73.35,
  `event_time` = '2026-07-16 08:53:35';
INSERT INTO `usera_transactions_fg_1` SET
  `customer_id` = 1,
  `num_transactions_30d` = 3,
  `total_spend_30d` = 120.5,
  `avg_transaction_value_30d` = 40.17,
  `event_time` = '2026-07-16 08:53:35';
INSERT INTO `usera_transactions_fg_1` SET
  `customer_id` = 2,
  `num_transactions_30d` = 8,
  `total_spend_30d` = 540,
  `avg_transaction_value_30d` = 67.5,
  `event_time` = '2026-07-16 08:53:35';
INSERT INTO `usera_transactions_fg_1` SET
  `customer_id` = 4,
  `num_transactions_30d` = 1,
  `total_spend_30d` = 45.9,
  `avg_transaction_value_30d` = 45.9,
  `event_time` = '2026-07-16 08:53:35';

-- back to the metadata DB: content appended after this file (dynamic
-- add-project / api-key templates) assumes hopsworks is the current DB
USE hopsworks;

/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;
/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;
