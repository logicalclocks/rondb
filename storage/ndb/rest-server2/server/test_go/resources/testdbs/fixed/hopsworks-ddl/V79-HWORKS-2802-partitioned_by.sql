-- HWORKS-2802 — backend-native partitioned_by for feature groups.
--
-- Adds two columns to cached_feature_group:
--   partitioned_by             — JSON-encoded ordered list of time-grain names
--                                (subset of "hour","day","week","month","year").
--                                NULL when the FG was not created with the
--                                partitioned_by parameter.
--   online_partition_columns   — when 0 (default), the synthetic grain
--                                features carry offline_only=1 so the online
--                                ingestion pipeline excludes them from RonDB.
--                                When 1, the grain features flow through to
--                                online storage like any other feature.
--
-- Adds one column to cached_feature:
--   offline_only               — when 1, the online ingestion path drops this
--                                feature from the RonDB write. Default 0
--                                keeps all existing features flowing to the
--                                online store unchanged.

ALTER TABLE hopsworks.cached_feature_group
    ADD COLUMN partitioned_by VARCHAR(255) NULL,
    ADD COLUMN online_partition_columns TINYINT(1) NOT NULL DEFAULT 0;

ALTER TABLE hopsworks.cached_feature
    ADD COLUMN offline_only TINYINT(1) NOT NULL DEFAULT 0;
