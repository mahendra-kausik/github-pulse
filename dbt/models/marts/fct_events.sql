-- Fact table: grain = one GH Archive event.
-- Partitioned by event_date + clustered by event_type to mirror the raw table and keep
-- downstream queries (and Looker Studio) cheap on the free tier.

{{
    config(
        materialized='table',
        partition_by={'field': 'event_date', 'data_type': 'date'},
        cluster_by=['event_type'],
        require_partition_filter=true,
    )
}}

select
    event_id,
    event_type,
    created_at,
    event_date,
    actor_login,
    repo_id,
    repo_name,
    language
from {{ ref('stg_github_events') }}
