-- B7 (error): closed version periods of the same listing must not overlap.
-- Window-function replacement for the O(n^2) pandas loop in Test_gold_layer.py.
with closed as (
    select
        id,
        date(scd_valid_from) as valid_from,
        date(scd_valid_to)   as valid_to,
        lead(date(scd_valid_from)) over (
            partition by id
            order by date(scd_valid_from)
        ) as next_valid_from
    from {{ source('dubizzle', 'cars_scd') }}
    where scd_valid_to is not null
)
select id, valid_from, valid_to, next_valid_from
from closed
where next_valid_from is not null
  and next_valid_from < valid_to
