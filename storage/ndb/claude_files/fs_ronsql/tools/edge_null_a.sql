# Isolation of finding F1 (smoke run 2, 2026-09-09): RDRS aborted in
# NdbSqlUtil::cmpLongvarchar on PROBE EDGE-NULL-A.  Each line is one
# ronsql_cli process; the full statement is last.  Database: test, loaded
# with `.fs_load 0.01 --db test` or the ronsql_fs include.
SELECT COUNT(*) AS cnt_all, COUNT(i1) AS cnt_i1 FROM edge_hist_1 WHERE entity_id = 1;
SELECT COUNT(s_val) AS cnt_s FROM edge_hist_1 WHERE entity_id = 1;
SELECT SUM(i1) AS i1_sum, MIN(i2) AS i2_min, MAX(i3) AS i3_max FROM edge_hist_1 WHERE entity_id = 1;
SELECT MIN(dec_val) AS dec_min, MAX(dec_val) AS dec_max FROM edge_hist_1 WHERE entity_id = 1;
SELECT MAX(s_val) AS s_max FROM edge_hist_1 WHERE entity_id = 1;
SELECT MIN(s_val) AS s_min FROM edge_hist_1 WHERE entity_id = 1;
SELECT MAX(GREATEST(i1, i2, i3)) AS g_max FROM edge_hist_1 WHERE entity_id = 1;
SELECT MIN(LEAST(i1, i2, i3)) AS l_min FROM edge_hist_1 WHERE entity_id = 1;
SELECT MAX(s_val) AS s_max FROM edge_hist_1 WHERE entity_id = 3;
SELECT MAX(s_val) AS s_max FROM edge_hist_1 WHERE entity_id = 2;
SELECT MAX(s_val) AS s_max FROM edge_hist_1;
SELECT MAX(category) AS c_max FROM transactions_1 WHERE customer_id = 31;
SELECT MAX(s_val) AS s_max, COUNT(s_val) AS cnt_s FROM edge_hist_1 WHERE entity_id = 1;
SELECT COUNT(*) AS cnt_all, COUNT(i1) AS cnt_i1, COUNT(s_val) AS cnt_s, SUM(i1) AS i1_sum, MIN(i2) AS i2_min, MAX(i3) AS i3_max, MIN(dec_val) AS dec_min, MAX(dec_val) AS dec_max, MAX(s_val) AS s_max, MAX(GREATEST(i1, i2, i3)) AS g_max, MIN(LEAST(i1, i2, i3)) AS l_min FROM edge_hist_1 WHERE entity_id = 1;
