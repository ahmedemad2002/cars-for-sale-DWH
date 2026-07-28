-- B3 (error): a version cannot close before it opened.
select id, scd_valid_from, scd_valid_to
from {{ source('dubizzle', 'cars_scd') }}
where scd_valid_to is not null
  and date(scd_valid_from) > date(scd_valid_to)
