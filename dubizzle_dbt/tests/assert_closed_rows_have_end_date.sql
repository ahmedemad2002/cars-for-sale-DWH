-- B2 (error): closed rows (updated/deleted) must carry a closing date;
-- days_to_sell breaks for deleted rows without one.
select id, status
from {{ source('dubizzle', 'cars_scd') }}
where status in ('updated', 'deleted')
  and scd_valid_to is null
