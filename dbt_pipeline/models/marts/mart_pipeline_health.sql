with health as (
    select
        run_at::date as run_date,
        count(*) as total_runs,
        sum(anomalies_detected) as total_anomalies,
        avg(anomalies_detected) as avg_anomalies_per_run,
        sum(case when status = 'healthy' then 1 else 0 end) as healthy_runs,
        sum(case when status != 'healthy' then 1 else 0 end) as unhealthy_runs,
        max(records_received) as max_records_received
    from public.pipeline_health_log
    group by run_at::date
)

select
    run_date,
    total_runs,
    total_anomalies,
    round(avg_anomalies_per_run::numeric, 2) as avg_anomalies_per_run,
    healthy_runs,
    unhealthy_runs,
    max_records_received,
    round(
        healthy_runs::numeric / nullif(total_runs, 0) * 100,
        1
    ) as health_score_pct,
    current_timestamp as dbt_updated_at
from health
order by run_date desc
