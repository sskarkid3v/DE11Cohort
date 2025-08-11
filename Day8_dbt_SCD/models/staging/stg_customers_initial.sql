select
    customer_nk,first_name,last_name,email,city,country,
    signup_ts::timestamp as signup_ts
from {{source('public_raw_data','customers_initial')}}
