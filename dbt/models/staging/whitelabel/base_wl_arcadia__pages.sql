-- Arcadia storefront page loads, projected into the shape stg_pages unions.
-- One of three hand-maintained base models, one per whitelabel brand. See README, "Challenge 2".
select
    'wl_arcadia' as source_key,
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
from {{ source('wl_arcadia', 'pages') }}
