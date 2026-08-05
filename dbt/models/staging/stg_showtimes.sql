-- Existing platform (already built). Do not modify as part of this exercise.
select
    showtime_id,
    event_id,
    start_at
from {{ source('raw', 'showtimes') }}
