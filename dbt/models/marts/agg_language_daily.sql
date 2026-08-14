-- Language-momentum tile source: PR-event counts per (date, language).
-- `fct_events.language` is always NULL on live data (GitHub stopped sending it on PR event
-- payloads), so language comes from the repo_language cache instead, fetched via the
-- GitHub REST API by ingestion/enrich_language.py. This is the temporal "which languages are
-- gaining momentum" signal.

{{ config(materialized='table') }}

select
    e.event_date,
    l.language,
    count(*) as pr_event_count,
    count(distinct e.repo_id) as distinct_repos
from {{ ref('fct_events') }} as e
inner join {{ ref('stg_repo_language') }} as l on e.repo_id = l.repo_id
where
    e.event_date >= cast('{{ var("start_date", "2024-01-01") }}' as date)
    and e.event_type = 'PullRequestEvent'
    and l.language is not null
group by e.event_date, l.language
