-- Existing platform (already built). Do not modify as part of this exercise.
select
    account_id as customer_key,
    email,
    created_at,
    'todaytix' as source_system
from {{ ref('stg_accounts') }}
