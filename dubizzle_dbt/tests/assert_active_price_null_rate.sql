-- B9 (warn): more than 10% of active listings missing Price suggests the
-- scraper or flattening step is dropping the field.
{{ config(severity='warn') }}

select
    cast(sum(case when price is null then 1 else 0 end) as double) / count(*) as null_rate
from {{ source('dubizzle', 'cars_scd') }}
where status = 'active'
having cast(sum(case when price is null then 1 else 0 end) as double) / count(*) > 0.10
