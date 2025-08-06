select d.*, dep.department_name
from {{ ref('stg_doctors') }} d
left join {{ ref('stg_departments') }} dep
on d.department_id = dep.department_id