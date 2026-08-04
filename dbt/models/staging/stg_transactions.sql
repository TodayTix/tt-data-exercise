-- Existing platform (already built). Do not modify as part of this exercise.
select
    transaction_id,
    order_id,
    replace(replace(trim(amount), '$', ''), ',', '')::numeric as amount,
    occurred_at
from {{ source('raw', 'transactions') }}
