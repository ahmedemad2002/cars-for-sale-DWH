-- B1 (error): an active row must not have a closing date, or it will never
-- appear in current-inventory queries.
select id, scd_valid_to
from {{ source('dubizzle', 'cars_scd') }}
where status = 'active'
  and scd_valid_to is not null
