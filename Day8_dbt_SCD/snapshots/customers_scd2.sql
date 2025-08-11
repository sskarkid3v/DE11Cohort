{% snapshot customers_scd2 %}
{{
    config(
        target_schema='snapshots',
        unique_key='customer_nk',
        strategy='check',
        check_cols=['first_name', 'last_name', 'email', 'city', 'country', 'signup_ts'],
        invalidate_hard_deletes=True 
    )
}}

select
    customer_nk,
    first_name,
    last_name,
    email,
    city,
    country,
    signup_ts
    from {{ ref('stg_customers_latest') }}

{% endsnapshot %}