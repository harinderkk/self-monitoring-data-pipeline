with source as (
    select
        id,
        company_name,
        cik,
        form_type,
        filing_date,
        accession_number,
        period_ending,
        document_url,
        ingested_at
    from public.raw_edgar_filings
    where company_name is not null
    and filing_date is not null
),

cleaned as (
    select
        id,
        trim(company_name) as company_name,
        cik,
        form_type,
        filing_date::date as filing_date,
        accession_number,
        period_ending::date as period_ending,
        document_url,
        ingested_at,
        current_timestamp as dbt_updated_at
    from source
)

select * from cleaned
