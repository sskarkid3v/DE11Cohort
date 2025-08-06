select
id as patient_id,
name,
gender,
dob,
city
FROM {{source('public_raw_data','patients')}}