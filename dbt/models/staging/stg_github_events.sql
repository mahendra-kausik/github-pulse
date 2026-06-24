-- Staging: cast types and dedupe to one row per event id.
-- Source rows are already a slim projection (see ingestion/transform.py); here we just clean.
-- A `var('start_date')` lower bound keeps the partition filter present so scans stay cheap.

with source as (

    select *
    from {{ source('raw', 'events') }}
    where event_date >= cast('{{ var("start_date", "2024-01-01") }}' as date)

),

deduped as (

    select
        *,
        row_number() over (partition by id order by created_at) as _rn
    from source
    where id is not null

)

select
    id              as event_id,
    event_type,
    created_at,
    event_date,
    actor_login,
    repo_id,
    repo_name,
    language
from deduped
where _rn = 1
