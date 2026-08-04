-- All page loads across TodayTix and the whitelabel storefronts, one row per page view.
-- Sentinel nulls ('', 'N/A', 'NULL') collapsed; page_type normalized to snake_case.
-- Every leg projects the same columns in the same order. Adding a whitelabel brand today
-- means a new source block, a new base model, and a new leg below — see README, "Challenge 2".
with todaytix as (
    select
        'ttg' as source_key,
        page_id,
        nullif(nullif(nullif(trim(account_id), ''), 'N/A'), 'NULL') as account_id,
        nullif(nullif(nullif(trim(customer_id), ''), 'N/A'), 'NULL') as customer_id,
        cast(null as text) as visitor_id,
        lower(regexp_replace(trim(page_type), '\s+', '_', 'g')) as page_type,
        occurred_at,
        nullif(nullif(nullif(trim(event_id), ''), 'N/A'), 'NULL') as event_id,
        nullif(nullif(nullif(trim(showtime_id), ''), 'N/A'), 'NULL') as showtime_id,
        cast(null as text) as utm_source,
        cast(null as text) as utm_medium
    from {{ source('raw', 'pages') }}
)

select * from todaytix
union all
select * from {{ ref('base_wl_arcadia__pages') }}
union all
select * from {{ ref('base_wl_northgate__pages') }}
union all
select * from {{ ref('base_wl_lumen__pages') }}
union all
select * from {{ ref('base_partner_orpheum__pages') }}
