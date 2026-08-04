-- Existing platform (already built). Do not modify as part of this exercise.
-- Sentinel nulls ('', 'N/A', 'NULL') collapsed; page_type normalized to snake_case.
select
    page_id,
    nullif(nullif(nullif(trim(account_id), ''), 'N/A'), 'NULL') as account_id,
    nullif(nullif(nullif(trim(customer_id), ''), 'N/A'), 'NULL') as customer_id,
    lower(regexp_replace(trim(page_type), '\s+', '_', 'g')) as page_type,
    occurred_at,
    nullif(nullif(nullif(trim(event_id), ''), 'N/A'), 'NULL') as event_id,
    nullif(nullif(nullif(trim(showtime_id), ''), 'N/A'), 'NULL') as showtime_id
from {{ source('raw', 'pages') }}
