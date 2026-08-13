-- Query 4: Heavier join across lineitem, orders and part with aggregation
SELECT p.p_brand, p.p_type, COUNT(*) AS line_count, SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM samples.tpch.lineitem l
JOIN samples.tpch.orders o ON l.l_orderkey = o.o_orderkey
JOIN samples.tpch.part p ON l.l_partkey = p.p_partkey
WHERE o.o_orderdate >= DATE'1995-01-01' AND o.o_orderdate < DATE'1995-04-01'
GROUP BY p.p_brand, p.p_type
ORDER BY revenue DESC
LIMIT 200;
