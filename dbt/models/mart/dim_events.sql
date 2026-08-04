-- Existing platform (already built). Do not modify as part of this exercise.
select
    e.event_id,
    e.name as event_name,
    e.slug,
    count(distinct s.showtime_id) as showtime_count,
    'todaytix' as source_system
from {{ ref('stg_events') }} e
left join {{ ref('stg_showtimes') }} s on s.event_id = e.event_id
group by 1, 2, 3
