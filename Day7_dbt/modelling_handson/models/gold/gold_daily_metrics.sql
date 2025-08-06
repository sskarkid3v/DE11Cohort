select
a.appointment_date,
count(distinct a.appointment_id) as total_appointments,
count(distinct t.treatment_id) as total_treatments,
sum(t.cost) as revenue
from {{ ref('silver_appointments') }} a
left join {{ ref('silver_treatments') }} t
on a.appointment_id = t.appointment_id
group by a.appointment_date