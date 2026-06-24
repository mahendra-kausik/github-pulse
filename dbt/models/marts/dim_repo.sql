-- Repo dimension: one row per repo. language is taken from the most recent PR event that
-- carried it (it is null for repos that had no PR activity in the window).

{{ config(materialized='table') }}

with events as (

    select
        repo_id,
        repo_name,
        language,
        created_at
    from {{ ref('stg_github_events') }}
    where repo_id is not null

),

ranked as (

    select
        repo_id,
        repo_name,
        language,
        row_number() over (
            partition by repo_id
            order by case when language is not null then 0 else 1 end, created_at desc
        ) as _rn
    from events

)

select
    repo_id,
    repo_name,
    language
from ranked
where _rn = 1
