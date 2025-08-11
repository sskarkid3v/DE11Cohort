{{ config(
    materialzed='incremental',
    unique_key='customer_nk'
)}}

with base as (
    select * from {{ ref('stg_customers_initial') }}
    union all
    select customer_nk,first_name,last_name,email,city,country,signup_ts
     from {{ ref('stg_customers_day1') }}
    union all
    select customer_nk,first_name,last_name,email,city,country,signup_ts
     from {{ ref('stg_customers_day2') }}
),
latest as (
    select customer_nk,first_name,last_name,email,city,country,signup_ts,
        coalesce(nullif('1900-01-01','1900-01-01')::timestamp, signup_ts) as change_ts
    from {{ref('stg_customers_initial') }}
    union all
    select customer_nk,first_name,last_name,email,city,country,signup_ts, change_ts
    from {{ref('stg_customers_day1') }}
    union all
    select customer_nk,first_name,last_name,email,city,country,signup_ts, change_ts
    from {{ref('stg_customers_day2') }}
),
ranked as (
    select *,
        row_number() over (partition by customer_nk order by coalesce(change_ts, signup_ts) desc) as rn
    from latest
)

select customer_nk, first_name, last_name, email, city, country, signup_ts
from ranked where rn =1

{% if is_incremental() %}
;delete from {{this}}
using (
    select distinct customer_nk from ranked
) r
where {{this}}.customer_nk = r.customer_nk
{% endif %}
