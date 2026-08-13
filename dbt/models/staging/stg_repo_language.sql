-- Staging: dedupe the language cache to one row per repo_id, freshest fetch wins.
-- Source is append-only (ingestion/enrich_language.py); this is the read-side dedup.

with source as (

    select *
    from {{ source('raw', 'repo_language') }}

),

deduped as (

    select
        *,
        row_number() over (partition by repo_id order by fetched_at desc) as _rn
    from source

)

select
    repo_id,
    language
from deduped
where _rn = 1
