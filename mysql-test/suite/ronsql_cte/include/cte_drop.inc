# Teardown for the data-driven CTE suite schema (see cte_schema.inc).
--disable_query_log
--disable_warnings
DROP TABLE IF EXISTS evlog;
DROP TABLE IF EXISTS lineitem;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS part;
DROP TABLE IF EXISTS supplier;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS nation;
DROP TABLE IF EXISTS region;
--enable_warnings
--enable_query_log
