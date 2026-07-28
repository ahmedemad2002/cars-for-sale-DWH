-- C2 (error): days_to_sell must be populated for exactly the closed-deleted
-- rows and null everywhere else.
select id, status, scd_valid_to, days_to_sell
from {{ ref('cars_scd_analytics') }}
where (days_to_sell is not null and status <> 'deleted')
   or (status = 'deleted' and scd_valid_to is not null and days_to_sell is null)
