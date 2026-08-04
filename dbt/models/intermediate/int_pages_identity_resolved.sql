-- Existing platform (already built). Do not modify as part of this exercise.
-- Resolves TTG pages.customer_id to a canonical id via the identity_merges log (up to 2 hops).
with pages as (
    select * from {{ ref('stg_pages') }}
),
merges as (
    select * from {{ ref('stg_identity_merges') }}
),
resolved as (
    select
        p.page_id,
        p.account_id,
        p.customer_id,
        coalesce(m2.to_customer_id, m1.to_customer_id, p.customer_id) as customer_id_resolved,
        p.page_type,
        p.occurred_at,
        p.event_id,
        p.showtime_id
    from pages p
    left join merges m1 on p.customer_id = m1.from_customer_id
    left join merges m2 on m1.to_customer_id = m2.from_customer_id
)
select * from resolved
