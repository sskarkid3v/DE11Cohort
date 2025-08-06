select
    id as department_id,
    name as department_name
FROM {{ source('public_raw_data','departments') }}