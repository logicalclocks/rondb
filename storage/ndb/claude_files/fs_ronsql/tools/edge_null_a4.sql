# F1 round 4: the string register is loaded once (for the first aggregate over
# the column) and reused by a later MIN/MAX after other column loads.
SELECT COUNT(s_val) AS cnt_s, SUM(i1) AS i1_sum, MAX(s_val) AS s_max FROM edge_hist_1 WHERE entity_id = 1;
SELECT COUNT(s_val) AS cnt_s, MAX(s_val) AS s_max FROM edge_hist_1 WHERE entity_id = 1;
SELECT MIN(s_val) AS s_min, SUM(i1) AS i1_sum, MAX(s_val) AS s_max FROM edge_hist_1 WHERE entity_id = 1;
SELECT MAX(s_val) AS s_max, SUM(i1) AS i1_sum, COUNT(s_val) AS cnt_s FROM edge_hist_1 WHERE entity_id = 1;
SELECT MAX(s_val) AS s_max, MIN(s_val) AS s_min FROM edge_hist_1 WHERE entity_id = 1;
SELECT COUNT(s_val) AS cnt_s, SUM(i1) AS i1_sum, MAX(s_val) AS s_max FROM edge_hist_1 WHERE entity_id = 3;
SELECT MIN(category) AS c_min, SUM(amount) AS a_sum, MAX(category) AS c_max FROM transactions_1 WHERE customer_id = 31;
SELECT MIN(category) AS c_min, SUM(amount) AS a_sum, MAX(category) AS c_max FROM transactions_1;
SELECT COUNT(tier) AS cnt_t, SUM(age) AS age_sum, MAX(tier) AS t_max FROM customers_1 WHERE customer_id = 21;
