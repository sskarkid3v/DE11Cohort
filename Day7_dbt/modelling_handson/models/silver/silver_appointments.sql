select * from {{ ref('bronze_appointments') }}
where appointment_date is not null and status != 'cancelled'