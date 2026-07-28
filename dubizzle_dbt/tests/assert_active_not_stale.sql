-- B12 (warn): active listings older than a year are either genuine outliers
-- or a sign the delisting detection is missing them.
{{ config(severity='warn') }}

select id, first_seen_date
from {{ source('dubizzle', 'cars_scd') }}
where status = 'active'
  and date(first_seen_date) < current_date - interval '365' day
