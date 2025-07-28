create index idx_city_branch_id on dim_customer(branch_id);
cluster dim_customer using idx_city_branch_id;


explain analyze 
select * from dim_customer where branch_id=9
--Seq Scan on dim_customer  (cost=0.00..222.00 rows=1029 width=43) (actual time=0.006..0.592 rows=1029 loops=1)
--  Filter: (branch_id = 9)
--  Rows Removed by Filter: 8971
--Planning Time: 0.066 ms
--Execution Time: 0.633 ms


explain analyze 
select * from dim_customer where branch_id=9
--Bitmap Heap Scan on dim_customer  (cost=16.26..126.12 rows=1029 width=43) (actual time=0.020..0.071 rows=1029 loops=1)
--  Recheck Cond: (branch_id = 9)
--  Heap Blocks: exact=11
--  ->  Bitmap Index Scan on idx_city_branch_id  (cost=0.00..16.00 rows=1029 width=0) (actual time=0.016..0.016 rows=1029 loops=1)
--        Index Cond: (branch_id = 9)
--Planning Time: 0.143 ms
--Execution Time: 0.102 ms

--index maintenance
vaccum analyze <table_name>
reindex index <index_name>
