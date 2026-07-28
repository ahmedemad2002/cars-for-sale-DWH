-- B6 (error): scd_valid_from cannot be in the future.
select id, scd_valid_from
from {{ source('dubizzle', 'cars_scd') }}
where date(scd_valid_from) > current_date
