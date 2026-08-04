-- Existing platform (already built). Certified mart — downstream dashboards and the AI/agent
-- layer query this directly. Extend it (or union into it) without breaking its grain or columns.
-- Grain: one row per TTG transaction (payment).
with orders as (
    select * from {{ ref('stg_orders') }}
),
transactions as (
    select * from {{ ref('stg_transactions') }}
),
showtimes as (
    select * from {{ ref('stg_showtimes') }}
),
events as (
    select * from {{ ref('stg_events') }}
)

select
    t.transaction_id,
    t.order_id,
    o.account_id as customer_key,
    e.event_id,
    e.name as event_name,
    o.showtime_id,
    s.start_at as showtime_start_at,
    t.amount as amount_usd,
    'USD' as currency,
    'todaytix' as source_system,
    t.occurred_at
from transactions t
join orders o on o.order_id = t.order_id
left join showtimes s on s.showtime_id = o.showtime_id
left join events e on e.event_id = s.event_id
