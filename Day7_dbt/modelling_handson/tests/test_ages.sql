select *
from {{ref("dim_patients")}}
where age < 0