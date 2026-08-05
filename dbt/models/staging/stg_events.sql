-- Existing platform (already built). Do not modify as part of this exercise.
select
    event_id,
    trim(name) as name,
    trim(slug) as slug
from {{ source('raw', 'events') }}
