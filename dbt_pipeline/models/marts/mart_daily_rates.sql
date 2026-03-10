with exchange_rates as (
    select
        date,
        max(case when series_id = 'FXUSDCAD' then value end) as usd_cad,
        max(case when series_id = 'FXEURCAD' then value end) as eur_cad,
        max(case when series_id = 'FXGBPCAD' then value end) as gbp_cad
    from {{ ref('stg_market_data') }}
    where series_id in ('FXUSDCAD', 'FXEURCAD', 'FXGBPCAD')
    group by date
),

with_changes as (
    select
        date,
        usd_cad,
        eur_cad,
        gbp_cad,
        usd_cad - lag(usd_cad) over (order by date) as usd_cad_daily_change,
        eur_cad - lag(eur_cad) over (order by date) as eur_cad_daily_change,
        gbp_cad - lag(gbp_cad) over (order by date) as gbp_cad_daily_change
    from exchange_rates
)

select * from with_changes
order by date desc
