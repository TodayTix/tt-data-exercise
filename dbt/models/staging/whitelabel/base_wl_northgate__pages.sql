-- Northgate storefront page loads, projected into the shape stg_pages unions.
-- Northgate runs an older tracker that never sends showtime_id or utm_*, so those
-- columns do not exist on the table and are filled with typed nulls here.
select
    'wl_northgate' as source_key,
    page_id,
    nullif(nullif(nullif(trim(account_id), ''), 'N/A'), 'NULL') as account_id,
    cast(null as text) as customer_id,
    nullif(trim(visitor_id), '') as visitor_id,
    lower(regexp_replace(trim(page_type), '\s+', '_', 'g')) as page_type,
    occurred_at,
    nullif(nullif(nullif(trim(event_id), ''), 'N/A'), 'NULL') as event_id,
    cast(null as text) as showtime_id,
    cast(null as text) as utm_source,
    cast(null as text) as utm_medium
from {{ source('wl_northgate', 'pages') }}
