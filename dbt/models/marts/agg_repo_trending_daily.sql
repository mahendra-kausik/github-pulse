-- Trending-repos tile source: WatchEvent (star) counts per (date, repo).
-- A GitHub "star" fires a WatchEvent; counting these per repo per day gives the trending signal.

{{ config(materialized='table') }}

select
    event_date,
    repo_id,
    repo_name,
    count(*) as star_count
from {{ ref('fct_events') }}
where event_date >= cast('{{ var("start_date", "2024-01-01") }}' as date)
  and event_type = 'WatchEvent'
group by event_date, repo_id, repo_name
