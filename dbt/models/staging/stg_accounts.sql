-- Existing platform (already built). Do not modify as part of this exercise.
select
    account_id,
    nullif(trim(email), '') as email,
    created_at
from {{ source('raw', 'accounts') }}
