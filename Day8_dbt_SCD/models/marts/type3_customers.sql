{{ config(
    materialzed='incremental',
    unique_key='customer_nk'
)}}

with unioned as (
  select customer_nk, first_name, last_name, email, city, country, coalesce(null::timestamp, signup_ts) as change_ts
  from {{ ref('stg_customers_initial') }}
  union all
  select customer_nk, first_name, last_name, email, city, country, change_ts
  from {{ ref('stg_customers_day1') }}
  union all
  select customer_nk, first_name, last_name, email, city, country, change_ts
  from {{ ref('stg_customers_day2') }}
),
ordered as (
  select *,
         row_number() over (partition by customer_nk order by coalesce(change_ts, '1900-01-01') desc) as rn,
         lead(city) over (partition by customer_nk order by coalesce(change_ts, '1900-01-01') desc) as prev_city
  from unioned
)
select 
  customer_nk,
  first_name,
  last_name,
  email,
  city as current_city,
  prev_city
from ordered
where rn = 1


{% if is_incremental() %}
;delete from {{this}}
using (
    select distinct customer_nk from ranked
) r
where {{this}}.customer_nk = r.customer_nk
{% endif %}