-- Categorical tile source: event counts per (date, type). Works for ALL events, so this is the
-- always-available momentum signal (no dependency on language).

{{ config(materialized='table') }}

select
    event_date,
    event_type,
    count(*) as event_count,
    count(distinct repo_id) as distinct_repos,
    count(distinct actor_login) as distinct_actors
from {{ ref('fct_events') }}
where event_date >= cast('{{ var("start_date", "2024-01-01") }}' as date)
group by event_date, event_type
