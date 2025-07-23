--subquery example
select name, email from customers
where customer_id in (
	select customer_id from orders
	where order_date > current_date - interval '30 days'
	)
and customer_id in (
	select customer_id from payments
	where payment_date > current_date - interval '30 days'
	)

--CTEs example
with recent_customers as (
	select customer_id from orders
	where order_date > current_date - interval '30 days'
	intersect
	select customer_id from payments
	where payment_date > current_date - interval '30 days'
)
select name, email from customers
where customer_id in (select customer_id from recent_customers)

--window_functions
--row_number

select *, row_number() over (partition by customer_id order by order_date desc) as rn
from orders;

--rank() and dense_rank()
select product_id, rank() over(order by sum(quantity) desc)
from order_items group by product_id;

select product_id, dense_rank() over(order by sum(quantity) desc)
from order_items group by product_id;

--lead and lag fucntion
select customer_id, order_date,
	lag(order_date) over (partition by customer_id order by order_date) as previous_order,
	lead(order_date) over (partition by customer_id order by order_date) as next_order
from orders;

--first_value
select order_id, customer_id,order_date,
	first_value(order_date) over (partition by customer_id order by order_date) as first_order
from orders;

--json and array data
create table customer_profiles (
	customer_id int primary key,
	preferences jsonb,
	tags text[]
);


insert into customer_profiles (customer_id, preferences, tags)
values
(2, '{"interests":["books", "electronics", "games"],"settings": {"notifications": true, "theme":"dark"}}', array['vip', 'newsletter'])

--selecting json data
select * from customer_profiles
where preferences -> 'settings' ->> 'theme' = 'dark'

select * from customer_profiles
where preferences -> 'settings' ->> 'notifications' = 'true'
  
--selecting array data
select * from customer_profiles
where 'vip' = any(tags)

--unpacking array data
select customer_id, jsonb_array_elements(preferences->'interests') as interests
from customer_profiles;

