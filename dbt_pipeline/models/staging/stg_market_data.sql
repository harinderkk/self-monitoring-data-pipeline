with source as (
    select
        id,
        series_id,
        series_name,
        date,
        value,
        frequency,
        ingested_at
    from public.raw_market_data
    where value is not null
),

cleaned as (
    select
        id,
        series_id,
        series_name,
        date::date as date,
        value::numeric(20,6) as value,
        frequency,
        ingested_at,
        current_timestamp as dbt_updated_at
    from source
)

select * from cleaned
