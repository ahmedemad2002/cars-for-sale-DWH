-- B4 (error): no row version can start before the listing was first seen.
select id, first_seen_date, scd_valid_from
from {{ source('dubizzle', 'cars_scd') }}
where date(first_seen_date) > date(scd_valid_from)
