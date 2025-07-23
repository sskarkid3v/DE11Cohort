--slow query example
explain analyze
select * from orders
where extract(year from order_date) = 2023;
--Execution Time: 0.702 ms


--making SARgable filters
explain analyze
select * from orders
where order_date between '2023-01-01' and '2023-12-31'
Execution Time: 0.641 ms

-- synatx for creating index
--create index <index_name> on <table_name>(<column_name>)

create index idx_order_date on orders(order_date)

--dropping index
drop index idx_order_date

--faster query using index
explain analyze
select * from orders
where order_date between '2023-01-01' and '2023-12-31'
Execution Time: 0.496 ms

--example of query not using the inex
explain analyze
select * from orders
where extract(year from order_date) = 2023;








