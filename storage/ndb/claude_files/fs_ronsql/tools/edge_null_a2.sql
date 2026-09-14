# F1 round 2: the full statement crashes but every single aggregate passes.
# Pairs of MAX(s_val) with each other construct, order swapped, plus slot-count probes.
SELECT MAX(s_val) AS s_max, MAX(GREATEST(i1, i2, i3)) AS g_max FROM edge_hist_1 WHERE entity_id = 1;
SELECT MAX(GREATEST(i1, i2, i3)) AS g_max, MAX(s_val) AS s_max FROM edge_hist_1 WHERE entity_id = 1;
SELECT MAX(s_val) AS s_max, MIN(LEAST(i1, i2, i3)) AS l_min FROM edge_hist_1 WHERE entity_id = 1;
SELECT MAX(s_val) AS s_max, MAX(GREATEST(i1, i2)) AS g_max FROM edge_hist_1 WHERE entity_id = 1;
SELECT MAX(s_val) AS s_max, MAX(CASE WHEN i1 > i2 THEN i1 ELSE i2 END) AS c_max FROM edge_hist_1 WHERE entity_id = 1;
SELECT MAX(s_val) AS s_max, MAX(i1 + i2) AS a_max FROM edge_hist_1 WHERE entity_id = 1;
SELECT MAX(s_val) AS s_max, MIN(dec_val) AS dec_min FROM edge_hist_1 WHERE entity_id = 1;
SELECT MIN(dec_val) AS dec_min, MAX(s_val) AS s_max FROM edge_hist_1 WHERE entity_id = 1;
SELECT MAX(s_val) AS s_max, MAX(GREATEST(i1, i2, i3)) AS g_max FROM edge_hist_1 WHERE entity_id = 3;
SELECT MAX(category) AS c_max, MAX(GREATEST(amount, fee)) AS g_max FROM transactions_1 WHERE customer_id = 31;
SELECT MAX(GREATEST(amount, fee)) AS g_max, MAX(category) AS c_max FROM transactions_1 WHERE customer_id = 31;
SELECT COUNT(*) AS cnt_all, COUNT(i1) AS cnt_i1, COUNT(s_val) AS cnt_s, SUM(i1) AS i1_sum, MIN(i2) AS i2_min, MAX(i3) AS i3_max, MIN(dec_val) AS dec_min, MAX(dec_val) AS dec_max, MAX(s_val) AS s_max FROM edge_hist_1 WHERE entity_id = 1;
SELECT COUNT(*) AS cnt_all, COUNT(i1) AS cnt_i1, COUNT(s_val) AS cnt_s, SUM(i1) AS i1_sum, MIN(i2) AS i2_min, MAX(i3) AS i3_max, MIN(dec_val) AS dec_min, MAX(dec_val) AS dec_max, MAX(s_val) AS s_max, MAX(i1) AS i1_max FROM edge_hist_1 WHERE entity_id = 1;
SELECT COUNT(*) AS cnt_all, COUNT(i1) AS cnt_i1, COUNT(s_val) AS cnt_s, SUM(i1) AS i1_sum, MIN(i2) AS i2_min, MAX(i3) AS i3_max, MIN(dec_val) AS dec_min, MAX(dec_val) AS dec_max, MAX(GREATEST(i1, i2, i3)) AS g_max, MIN(LEAST(i1, i2, i3)) AS l_min FROM edge_hist_1 WHERE entity_id = 1;
SELECT MAX(s_val) AS s_max, COUNT(*) AS cnt_all, COUNT(i1) AS cnt_i1, COUNT(s_val) AS cnt_s, SUM(i1) AS i1_sum, MIN(i2) AS i2_min, MAX(i3) AS i3_max, MIN(dec_val) AS dec_min, MAX(dec_val) AS dec_max, MAX(GREATEST(i1, i2, i3)) AS g_max, MIN(LEAST(i1, i2, i3)) AS l_min FROM edge_hist_1 WHERE entity_id = 1;
SELECT COUNT(*) AS cnt_all, COUNT(i1) AS cnt_i1, COUNT(s_val) AS cnt_s, SUM(i1) AS i1_sum, MIN(i2) AS i2_min, MAX(i3) AS i3_max, MIN(dec_val) AS dec_min, MAX(dec_val) AS dec_max, MAX(s_val) AS s_max, MAX(GREATEST(i1, i2, i3)) AS g_max FROM edge_hist_1 WHERE entity_id = 1;
