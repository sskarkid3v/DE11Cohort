create table fact_transact_partition (
	id serial,
	account_id int,
	transaction_date date not null,
	amount float,
	transaction_type varchar(10),
	primary key (id, transaction_date)
) partition by range (transaction_date);

create table fact_transact_2025_06 partition of fact_transact_partition
	for values from ('2025-06-01') to ('2025-07-01');


create table fact_transact_2025_07 partition of fact_transact_partition
	for values from ('2025-07-01') to ('2025-08-01');


create index idx_fact_transact_2025_06
	on fact_transact_2025_06(account_id);

insert into fact_transact_partition
select * from fact_transactions
where transaction_date >='2025-06-01'

explain analyze
select * from fact_transact_partition
where transaction_date='2025-06-27' and account_id=1095
--Execution Time: 1.015 ms
--without index --> Execution Time: 2.469 ms
--with index --> Execution Time: 0.038 ms


explain analyze
select * from fact_transactions
where transaction_date='2025-06-27'
--Execution Time: 7.768 ms
