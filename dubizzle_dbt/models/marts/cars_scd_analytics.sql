{#
    BI-ready view over the Gold SCD table.
    Ported from the hand-written Athena view (SHOW CREATE VIEW dubizzle.cars_scd_analytics)
    so the definition is version-controlled. feature_* columns are enumerated at
    compile time, so columns added by the pipeline's schema evolution are included
    automatically on the next dbt run.
#}

{%- set relation = source('dubizzle', 'cars_scd') -%}
{%- set feature_cols = [] -%}
{%- for col in adapter.get_columns_in_relation(relation) -%}
    {%- if col.name.startswith('feature_') -%}
        {%- do feature_cols.append(col.name) -%}
    {%- endif -%}
{%- endfor -%}

select
    id,
    externalid,
    title,
    brand,
    model,
    year,
    price,
    kilometers,
    "power (hp)"          as power_hp,
    "engine capacity (cc)" as engine_cc,
    "body type"           as body_type,
    "transmission type"   as transmission_type,
    "fuel type"           as fuel_type,
    color,
    condition,
    status,
    first_seen_date,
    scd_valid_from,
    scd_valid_to,
    createdat,
    updatedat,

    coalesce(scd_valid_to, cast(current_date as varchar)) as last_seen_date,

    date_diff(
        'day',
        date(from_iso8601_timestamp(createdat)),
        coalesce(date(from_iso8601_timestamp(scd_valid_to)), current_date)
    ) as days_listed,

    case
        when status = 'deleted' and scd_valid_to is not null
        then date_diff(
            'day',
            date(from_iso8601_timestamp(createdat)),
            date(from_iso8601_timestamp(scd_valid_to))
        )
    end as days_to_sell,

    cast(year(current_date) - cast(year as integer) as integer) as age_in_years,

    case
        when kilometers > 0
        then round(cast(price / kilometers as decimal(10, 2)), 2)
    end as price_per_km,

    {% for col in feature_cols -%}
    cast(coalesce("{{ col }}", false) as integer){{ " +" if not loop.last }}
    {% endfor -%}
    as feature_count,

    ("feature_sunroof" or "feature_navigation system" or "feature_touch screen")
        as has_premium_features,

    ("feature_abs" and "feature_airbags" and "feature_esp" and "feature_parking sensors")
        as has_safety_suite,

    case
        when price < 500000  then 'Budget'
        when price < 1500000 then 'Mid-range'
        else 'Premium'
    end as price_segment,

    case
        when kilometers < 50000  then 'Low Mileage'
        when kilometers < 150000 then 'Medium Mileage'
        else 'High Mileage'
    end as mileage_category,

    (status = 'active' and scd_valid_to is null) as is_current_active

from {{ source('dubizzle', 'cars_scd') }}
