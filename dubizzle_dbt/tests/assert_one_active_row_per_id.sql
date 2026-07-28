-- B5 (error): more than one active row per listing means the SCD merge
-- inserted duplicates — the core integrity guarantee of the table.
select id, count(*) as active_rows
from {{ source('dubizzle', 'cars_scd') }}
where status = 'active'
group by id
having count(*) > 1
