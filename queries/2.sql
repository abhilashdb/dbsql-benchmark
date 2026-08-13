-- Query 2: Aggregation with group by
SELECT o_orderstatus, o_orderpriority, COUNT(*) AS order_count, SUM(o_totalprice) AS total_value, AVG(o_totalprice) AS avg_value
FROM samples.tpch.orders
GROUP BY o_orderstatus, o_orderpriority
ORDER BY total_value DESC;
