
  create view "salesdb"."staging"."stg_customers_latest__dbt_tmp"
    
    
  as (
    with unioned as (
  select customer_nk, first_name, last_name, email, city, country, signup_ts, null::timestamp as change_ts
  from "salesdb"."staging"."stg_customers_initial"
  union all
  select customer_nk, first_name, last_name, email, city, country, signup_ts, change_ts
  from "salesdb"."staging"."stg_customers_day1"
  union all
  select customer_nk, first_name, last_name, email, city, country, signup_ts, change_ts
  from "salesdb"."staging"."stg_customers_day2"
),
ranked as (
  select *,
         row_number() over (partition by customer_nk order by coalesce(change_ts, signup_ts) desc) as rn
  from unioned
)
select
  customer_nk,
  first_name,
  last_name,
  email,
  city,
  country,
  signup_ts
from ranked
where rn = 1
  );