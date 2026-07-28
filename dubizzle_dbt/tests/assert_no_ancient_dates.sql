-- B10 (error): no pipeline date can pre-date the pipeline launch era.
select id, first_seen_date, scd_valid_from
from {{ source('dubizzle', 'cars_scd') }}
where date(first_seen_date) < date '2020-01-01'
   or date(scd_valid_from) < date '2020-01-01'
