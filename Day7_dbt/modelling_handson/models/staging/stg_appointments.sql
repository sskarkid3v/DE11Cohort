select
id as appointment_id,
patient_id,
doctor_id,
appointment_date,
status,
department_id
from {{ source('public_raw_data', 'appointments') }}