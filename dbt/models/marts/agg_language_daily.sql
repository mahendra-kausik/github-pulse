-- Language-momentum tile source: PR-event counts per (date, language).
-- Language is ONLY present on PullRequestEvent (see CLAUDE.md), so we filter to PR events and
-- drop nulls. This is the temporal "which languages are gaining momentum" signal.

{{ config(materialized='table') }}

select
    event_date,
    language,
    count(*) as pr_event_count,
    count(distinct repo_id) as distinct_repos
from {{ ref('fct_events') }}
where event_date >= cast('{{ var("start_date", "2024-01-01") }}' as date)
  and event_type = 'PullRequestEvent'
  and language is not null
group by event_date, language
