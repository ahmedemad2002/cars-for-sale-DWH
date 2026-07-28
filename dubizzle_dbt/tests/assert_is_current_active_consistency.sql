-- C5 (error): is_current_active must be true exactly when status='active'
-- and the row is open.
select id, status, scd_valid_to, is_current_active
from {{ ref('cars_scd_analytics') }}
where is_current_active <> (status = 'active' and scd_valid_to is null)
