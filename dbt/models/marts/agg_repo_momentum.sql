-- Momentum-burst tile source: cross-signal activity per (date, repo).
-- burst_score = watch_count + fork_count + pr_count. Repos spiking across all three
-- signals on the same day are likely going viral — a single-signal spike (e.g. pure stars)
-- is less interesting than a coordinated cross-signal surge.

{{ config(materialized='table') }}

select
    event_date,
    repo_id,
    repo_name,
    countif(event_type = 'WatchEvent') as watch_count,
    countif(event_type = 'ForkEvent') as fork_count,
    countif(event_type = 'PullRequestEvent') as pr_count,
    countif(event_type in ('WatchEvent', 'ForkEvent', 'PullRequestEvent')) as burst_score
from {{ ref('fct_events') }}
where event_date >= cast('{{ var("start_date", "2024-01-01") }}' as date)
group by event_date, repo_id, repo_name
-- The cross-signal requirement is the whole point: an additive score alone ranks a repo with
-- 7k automated PRs and zero human interest above a genuinely viral one. Requiring all three
-- signals on the same day is what makes the metric hard to game (and drops 99.6% of rows —
-- almost every repo-day touches only one signal).
having watch_count > 0 and fork_count > 0 and pr_count > 0
