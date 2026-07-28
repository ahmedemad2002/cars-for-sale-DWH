-- B11 (warn): a car can't plausibly exceed ~60k km per year of age.
{{ config(severity='warn') }}

select id, year, kilometers
from {{ source('dubizzle', 'cars_scd') }}
where year is not null
  and kilometers is not null
  and year(current_date) - cast(year as integer) >= 0
  and kilometers > greatest(year(current_date) - cast(year as integer), 1) * 60000
