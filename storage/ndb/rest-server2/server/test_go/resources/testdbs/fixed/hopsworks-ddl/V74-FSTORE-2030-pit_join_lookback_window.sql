-- Per-join lookback window for PIT queries.
--
-- A join's lookback window bounds which rows of the joined feature group are eligible to
-- match in the PIT join. `lookback_key` selects which column the predicate is emitted
-- against (EVENT_TIME or PARTITION_KEY); `lookback_start_window` is the required lower
-- bound in epoch milliseconds; `lookback_end_window` is the optional upper bound (null →
-- fall back to the query's end_time

ALTER TABLE `hopsworks`.`training_dataset_join`
  ADD COLUMN `lookback_key`          VARCHAR(20) NULL,
  ADD COLUMN `lookback_start_window` BIGINT      NULL,
  ADD COLUMN `lookback_end_window`   BIGINT      NULL;
