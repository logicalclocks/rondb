-- RONDB-1158: rate limit group fixture for the C++ expansion tests
-- (rate_limit_groups_test). Self-contained entities: two users and two
-- projects of their own (no feature stores), so the dynamically registered
-- projects and macho's memberships - which vary per suite run - never leak
-- into the expansion assertions. The What rows reference the fixed stores
-- and feature groups above (fsdb002 = store 1091, feature group 31 =
-- sample_complex_type v1 in fsdb002).
--
-- The expected expansions:
--   group 1 rlg_team_rl   (20.00, SELECTED): Who = project RLGrp01
--     -> accounts RLGrp01_rlga1b2, RLGrp01_rlgb3c4; scope --database=fsdb002
--   group 2 rlg_clicks_rl (2.00, SELECTED): Who = uid 10100 (rlga1b2)
--     -> every membership of rlga1b2: FSDB003_rlga1b2 (original case),
--        RLGrp01_rlga1b2, and the clip31 of
--        RateLimitGroupVeryLongProj_rlga1b2 (34 chars -> 31);
--        scope --table=fsdb002.sample_complex_type_1
--   group 3 rlg_all_rl    (5.00, ALL): Who = uid 10101 (rlgb3c4)
--     -> RLGrp01_rlgb3c4; no scopes
--   group 4 rlg_inert_rl  (0.50, SELECTED, no What rows): inert
--
-- The version row is id 1, version 42 (what Hopsworks bumps on every save).

INSERT INTO `users` SET
  `uid` = 10100,
  `username` = 'rlga1b2',
  `password` = '12fa520ec8f65d3a6feacfa97a705e622e1fea95b80b521ec016e43874dfed5a',
  `email` = 'rlga1b2@hopsworks.ai',
  `fname` = 'Rate',
  `lname` = 'Limit',
  `activated` = '2015-05-15 10:22:36',
  `title` = 'Mr',
  `false_login` = 0,
  `status` = 2,
  `isonline` = 0,
  `secret` = 'V3WBPS4G2WMQ53VA',
  `validation_key` = NULL,
  `validation_key_updated` = NULL,
  `validation_key_type` = NULL,
  `mode` = 0,
  `password_changed` = '2015-04-28 15:18:42',
  `notes` = NULL,
  `max_num_projects` = 5,
  `num_active_projects` = 2,
  `two_factor` = 0,
  `tours_state` = 3,
  `salt` = '+9mTLmYSpnZROFEJEaednw8+GDH/s2J1QuRZy8okxW5myI/q8ek8Xu+ab5CyE9GzhWX6Sa4cr7KX8cAHi5IC4g==',
  `last_visited_at` = '2026-07-14 14:08:48';

INSERT INTO `users` SET
  `uid` = 10101,
  `username` = 'rlgb3c4',
  `password` = '12fa520ec8f65d3a6feacfa97a705e622e1fea95b80b521ec016e43874dfed5a',
  `email` = 'rlgb3c4@hopsworks.ai',
  `fname` = 'Rate',
  `lname` = 'Limit2',
  `activated` = '2015-05-15 10:22:36',
  `title` = 'Mr',
  `false_login` = 0,
  `status` = 2,
  `isonline` = 0,
  `secret` = 'V3WBPS4G2WMQ53VA',
  `validation_key` = NULL,
  `validation_key_updated` = NULL,
  `validation_key_type` = NULL,
  `mode` = 0,
  `password_changed` = '2015-04-28 15:18:42',
  `notes` = NULL,
  `max_num_projects` = 5,
  `num_active_projects` = 1,
  `two_factor` = 0,
  `tours_state` = 3,
  `salt` = '+9mTLmYSpnZROFEJEaednw8+GDH/s2J1QuRZy8okxW5myI/q8ek8Xu+ab5CyE9GzhWX6Sa4cr7KX8cAHi5IC4g==',
  `last_visited_at` = '2026-07-14 14:08:48';

INSERT INTO `project` SET
  `id` = 1200,
  `projectname` = 'RLGrp01',
  `username` = 'rlga1b2@hopsworks.ai',
  `created` = '2026-11-16 10:00:00',
  `description` = 'Rate limit group expansion fixture',
  `payment_type` = 'NOLIMIT',
  `last_quota_update` = '2026-11-16 10:00:00',
  `kafka_max_num_topics` = 100,
  `topic_name` = 'RLGrp01',
  `creation_status` = 0,
  `online_feature_store_available` = 0;

INSERT INTO `project` SET
  `id` = 1201,
  `projectname` = 'RateLimitGroupVeryLongProj',
  `username` = 'rlga1b2@hopsworks.ai',
  `created` = '2026-11-16 10:00:00',
  `description` = 'Rate limit group clip31 fixture',
  `payment_type` = 'NOLIMIT',
  `last_quota_update` = '2026-11-16 10:00:00',
  `kafka_max_num_topics` = 100,
  `topic_name` = 'RateLimitGroupVeryLongProj',
  `creation_status` = 0,
  `online_feature_store_available` = 0;

INSERT INTO `project_team` SET
  `project_id` = 1200,
  `team_member` = 'rlga1b2@hopsworks.ai',
  `team_role` = 'Data scientist',
  `added` = '2026-11-16 10:00:00';

INSERT INTO `project_team` SET
  `project_id` = 1003,
  `team_member` = 'rlga1b2@hopsworks.ai',
  `team_role` = 'Data scientist',
  `added` = '2026-11-16 10:00:00';

INSERT INTO `project_team` SET
  `project_id` = 1201,
  `team_member` = 'rlga1b2@hopsworks.ai',
  `team_role` = 'Data owner',
  `added` = '2026-11-16 10:00:00';

INSERT INTO `project_team` SET
  `project_id` = 1200,
  `team_member` = 'rlgb3c4@hopsworks.ai',
  `team_role` = 'Data scientist',
  `added` = '2026-11-16 10:00:00';

INSERT INTO `rate_limit_group` SET
  `id` = 1,
  `name` = 'rlg_team_rl',
  `budget_percent` = 20.00,
  `data_scope` = 'SELECTED',
  `created_by` = 10100;

INSERT INTO `rate_limit_group` SET
  `id` = 2,
  `name` = 'rlg_clicks_rl',
  `budget_percent` = 2.00,
  `data_scope` = 'SELECTED',
  `created_by` = 10100;

INSERT INTO `rate_limit_group` SET
  `id` = 3,
  `name` = 'rlg_all_rl',
  `budget_percent` = 5.00,
  `data_scope` = 'ALL',
  `created_by` = NULL;

INSERT INTO `rate_limit_group` SET
  `id` = 4,
  `name` = 'rlg_inert_rl',
  `budget_percent` = 0.50,
  `data_scope` = 'SELECTED',
  `created_by` = 10101;

INSERT INTO `rate_limit_group_user` SET
  `id` = 1, `group_id` = 1, `uid` = NULL, `project_id` = 1200;
INSERT INTO `rate_limit_group_user` SET
  `id` = 2, `group_id` = 2, `uid` = 10100, `project_id` = NULL;
INSERT INTO `rate_limit_group_user` SET
  `id` = 3, `group_id` = 3, `uid` = 10101, `project_id` = NULL;
INSERT INTO `rate_limit_group_user` SET
  `id` = 4, `group_id` = 4, `uid` = 10101, `project_id` = NULL;

INSERT INTO `rate_limit_group_data` SET
  `id` = 1, `group_id` = 1, `feature_group_id` = NULL, `feature_store_id` = 1091;
INSERT INTO `rate_limit_group_data` SET
  `id` = 2, `group_id` = 2, `feature_group_id` = 31, `feature_store_id` = NULL;

INSERT INTO `rate_limit_group_version` SET
  `id` = 1, `version` = 42;
