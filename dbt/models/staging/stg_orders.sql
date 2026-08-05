-- Existing platform (already built). Do not modify as part of this exercise.
select
    order_id,
    account_id,
    nullif(trim(showtime_id), '') as showtime_id,
    created_at,
    replace(replace(trim(total_amount), '$', ''), ',', '')::numeric as total_amount
from {{ source('raw', 'orders') }}
