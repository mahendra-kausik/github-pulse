-- Repo dimension: one row per repo. language comes from the repo_language cache (fetched via the
-- GitHub REST API by ingestion/enrich_language.py) — event payloads no longer carry it.
-- Null for repos not yet enriched or genuinely language-less.

{{ config(materialized='table') }}

with ranked as (

    select
        repo_id,
        repo_name,
        row_number() over (partition by repo_id order by created_at desc) as _rn
    from {{ ref('stg_github_events') }}
    where repo_id is not null

)

select
    r.repo_id,
    r.repo_name,
    l.language
from ranked as r
left join {{ ref('stg_repo_language') }} as l on r.repo_id = l.repo_id
where r._rn = 1
