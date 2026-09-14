# F1 round 3: register pressure (REGS = 8 in AggregationAPICompiler).
# Distinct-column fillers before MAX(s_val): 7, 8 and 9 aggregates.
SELECT SUM(i1), SUM(i2), SUM(i3), SUM(big_val), SUM(f_float), SUM(f_double), MAX(s_val) AS s_max FROM edge_hist_1 WHERE entity_id = 1;
SELECT SUM(i1), SUM(i2), SUM(i3), SUM(big_val), SUM(f_float), SUM(f_double), MAX(d_date), MAX(s_val) AS s_max FROM edge_hist_1 WHERE entity_id = 1;
SELECT SUM(i1), SUM(i2), SUM(i3), SUM(big_val), SUM(f_float), SUM(f_double), MAX(d_date), MAX(dec_val), MAX(s_val) AS s_max FROM edge_hist_1 WHERE entity_id = 1;
# Same-column fillers (one register shared): 9 aggregates but few loads.
SELECT SUM(i1), MIN(i1), MAX(i1), COUNT(i1), SUM(i2), MIN(i2), MAX(i2), COUNT(i2), MAX(s_val) AS s_max FROM edge_hist_1 WHERE entity_id = 1;
# COUNT(*) fillers (constant register): 9 aggregates.
SELECT COUNT(*), COUNT(*), COUNT(*), COUNT(*), COUNT(*), COUNT(*), COUNT(*), COUNT(*), MAX(s_val) AS s_max FROM edge_hist_1 WHERE entity_id = 1;
# String in the middle of 9 distinct loads.
SELECT SUM(i1), SUM(i2), SUM(i3), SUM(big_val), MAX(s_val) AS s_max, SUM(f_float), SUM(f_double), MAX(d_date), MAX(dec_val) FROM edge_hist_1 WHERE entity_id = 1;
# No NULLs at all (entity 3) with 9 distinct loads, string last.
SELECT SUM(i1), SUM(i2), SUM(i3), SUM(big_val), SUM(f_float), SUM(f_double), MAX(d_date), MAX(dec_val), MAX(s_val) AS s_max FROM edge_hist_1 WHERE entity_id = 3;
# Bulk table, no NULL strings, full table scan, 9 distinct loads, string last.
SELECT SUM(amount), SUM(fee), COUNT(merchant_id), SUM(score), SUM(amount_dec), SUM(flag), MAX(event_time), MIN(customer_id), MAX(category) AS c_max FROM transactions_1 WHERE customer_id = 31;
# Two string aggregates among 9 distinct loads.
SELECT SUM(i1), SUM(i2), SUM(i3), SUM(big_val), SUM(f_float), SUM(f_double), MAX(d_date), MIN(s_val) AS s_min, MAX(s_val) AS s_max FROM edge_hist_1 WHERE entity_id = 1;
