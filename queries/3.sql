-- Query 3: Multi-way join with aggregation
SELECT n.n_name AS nation, COUNT(DISTINCT c.c_custkey) AS customers, COUNT(o.o_orderkey) AS orders, SUM(o.o_totalprice) AS total_revenue
FROM samples.tpch.customer c
JOIN samples.tpch.orders o ON c.c_custkey = o.o_custkey
JOIN samples.tpch.nation n ON c.c_nationkey = n.n_nationkey
GROUP BY n.n_name
ORDER BY total_revenue DESC;
