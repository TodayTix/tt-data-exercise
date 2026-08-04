-- Lumen storefront page loads, projected into the shape stg_pages unions.
-- Lumen's tracker also sends consent_state and device_type, which are outside the
-- standard column set and are dropped here.
select
    'wl_lumen' as source_key,
    page_id,
    nullif(nullif(nullif(trim(account_id), ''), 'N/A'), 'NULL') as account_id,
    cast(null as text) as customer_id,
    nullif(trim(visitor_id), '') as visitor_id,
    lower(regexp_replace(trim(page_type), '\s+', '_', 'g')) as page_type,
    occurred_at,
    nullif(nullif(nullif(trim(event_id), ''), 'N/A'), 'NULL') as event_id,
    nullif(nullif(nullif(trim(showtime_id), ''), 'N/A'), 'NULL') as showtime_id,
    nullif(trim(utm_source), '') as utm_source,
    nullif(trim(utm_medium), '') as utm_medium
from {{ source('wl_lumen', 'pages') }}
