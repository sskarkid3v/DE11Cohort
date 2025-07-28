--view example
create view vw_transact_agg as
select c.id as customer_id, c.name,
	count(*) as num_transactions,
	sum(ft.amount) as total_spent
from fact_transactions ft 
join dim_account a on ft.account_id = a.id
join dim_customer c on a.customer_id =c.id
group by c.id, c.name;

--materialized view example
create materialized view mv_transact_agg as
select c.id as customer_id, c.name,
	count(*) as num_transactions,
	sum(ft.amount) as total_spent
from fact_transactions ft 
join dim_account a on ft.account_id = a.id
join dim_customer c on a.customer_id =c.id
group by c.id, c.name;

explain analyze
select * from vw_transact_agg
--Execution Time: 60.533 ms

explain analyze
select * from mv_transact_agg
--Execution Time: 0.374 ms

select count(*) from fact_transactions ft 

explain analyze
select c.id as customer_id, c.name,
	count(*) as num_transactions,
	sum(ft.amount) as total_spent
from fact_transactions ft 
join dim_account a on ft.account_id = a.id
join dim_customer c on a.customer_id =c.id
group by c.id, c.name;
--Execution Time: 61.132 ms

--update the materialized view
refresh materizlied view mv_transact_agg;




