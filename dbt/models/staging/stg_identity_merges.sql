-- Existing platform (already built). Do not modify as part of this exercise.
select
    from_customer_id,
    to_customer_id,
    merged_at
from {{ source('raw', 'identity_merges') }}
