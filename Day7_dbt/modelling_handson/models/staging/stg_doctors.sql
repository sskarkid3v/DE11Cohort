select
    id as doctor_id,
    name,
    specialization,
    department_id
FROM {{ source('public_raw_data','doctors') }}