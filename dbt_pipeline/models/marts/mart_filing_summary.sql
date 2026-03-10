with filings as (
    select
        company_name,
        form_type,
        count(*) as total_filings,
        min(filing_date) as first_filing_date,
        max(filing_date) as latest_filing_date,
        max(filing_date) - min(filing_date) as days_of_history
    from {{ ref('stg_edgar_filings') }}
    group by company_name, form_type
)

select
    company_name,
    form_type,
    total_filings,
    first_filing_date,
    latest_filing_date,
    days_of_history,
    current_timestamp as dbt_updated_at
from filings
order by company_name
