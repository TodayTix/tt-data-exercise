-- Orpheum page views, projected into the shape stg_pages unions.
-- Orpheum predates the whitelabel tracking standard: its schema carries no wl_ prefix,
-- its relation is page_views rather than pages, and page intent has to be read off the
-- URL path because the tracker never emitted a page type. Out of scope for Challenge 2 —
-- this model stays hand-written whatever else changes.
select
    'partner_orpheum' as source_key,
    view_id as page_id,
    nullif(nullif(nullif(trim(member_ref), ''), 'N/A'), 'NULL') as account_id,
    cast(null as text) as customer_id,
    nullif(trim(cookie), '') as visitor_id,
    case
        when path = '/' then 'home'
        when path like '/checkout/confirmation%' then 'checkout_complete'
        when path like '/checkout%' then 'checkout_start'
        when path like '/show/%/performances' then 'viewed_showtime'
        when path like '/show/%' then 'viewed_product_page'
        else 'other'
    end as page_type,
    viewed_at as occurred_at,
    nullif(nullif(nullif(trim(production_ref), ''), 'N/A'), 'NULL') as event_id,
    cast(null as text) as showtime_id,
    cast(null as text) as utm_source,
    cast(null as text) as utm_medium
from {{ source('partner_orpheum', 'page_views') }}
