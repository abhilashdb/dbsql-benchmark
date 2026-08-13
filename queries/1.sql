-- Query 1: Lightweight point filter
SELECT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate
FROM samples.tpch.orders
WHERE o_orderstatus = 'O'
ORDER BY o_orderdate DESC
LIMIT 100;
