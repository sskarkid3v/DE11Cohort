select patient_id,
name,
gender,
dob,
city,
date_part('year', age(current_date, dob)) as age from {{ ref('stg_patients') }}