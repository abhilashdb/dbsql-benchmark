-- Query 5: Window function - rank customer orders by value
SELECT c_custkey, o_orderkey, o_orderdate, o_totalprice,
       RANK() OVER (PARTITION BY c_custkey ORDER BY o_totalprice DESC) AS value_rank
FROM samples.tpch.orders o
JOIN samples.tpch.customer c ON o.o_custkey = c.c_custkey
QUALIFY value_rank <= 3;
