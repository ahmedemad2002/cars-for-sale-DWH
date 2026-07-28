-- B8 (error): active inventory collapsing below the pipeline's MIN_SILVER_ROWS
-- threshold signals a bad scrape day / mass false-delisting.
select count(*) as active_count
from {{ source('dubizzle', 'cars_scd') }}
where status = 'active'
having count(*) < 500
