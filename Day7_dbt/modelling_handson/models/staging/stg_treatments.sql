select
id as treatment_id,
appointment_id,
treatment,
cost
from {{ source('public_raw_data', 'treatments') }}