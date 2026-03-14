import requests
import pandas as pd
from datetime import datetime, timedelta
import logging
import os
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BASE_URL = "https://www.bankofcanada.ca/valet"

SERIES = {
    "FXUSDCAD":        "USD/CAD Exchange Rate",
    "FXEURCAD":        "EUR/CAD Exchange Rate",
    "FXGBPCAD":        "GBP/CAD Exchange Rate",
    "V122514":         "Overnight Rate",
    "INDINF_CPITOT_M": "Commodity Price Index Total",
}

EDGAR_HEADERS = {'User-Agent': 'yourname@email.com'}

CANADIAN_COMPANIES = {
    'Royal Bank of Canada':  '0001000275',
    'Toronto Dominion Bank': '0000947263',
    'CIBC':                  '0001045520',
    'Suncor Energy':         '0000311337',
    'TC Energy':             '0001232384',
    'Teck Resources':        '0000886986',
    'BCE Inc':               '0000718940',
    'Sun Life Financial':    '0001097362',
}

# ─────────────────────────────────────────────
# DB CONNECTION
# ─────────────────────────────────────────────

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=os.getenv('POSTGRES_PORT', 5432),
        database=os.getenv('POSTGRES_DB', 'sedar_db'),
        user=os.getenv('POSTGRES_USER', 'sedar_user'),
        password=os.getenv('POSTGRES_PASSWORD', 'sedar_pass')
    )

# ─────────────────────────────────────────────
# WATERMARK FUNCTIONS  ← this is the new core
# ─────────────────────────────────────────────

def get_watermark(source_type, source_id):
    """
    Read the last successfully processed date for a given source.
    Returns a date string like '2025-03-01', or None if first run.
    
    This is what Delta Lake does automatically with transaction logs.
    Here we're doing it explicitly so we understand the mechanism.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT last_successful_date 
        FROM pipeline_watermarks
        WHERE source_type = %s AND source_id = %s
    """, (source_type, source_id))
    row = cur.fetchone()
    cur.close()
    conn.close()
    
    if row and row[0]:
        logger.info(f"Watermark found for {source_id}: {row[0]}")
        return row[0].strftime("%Y-%m-%d")
    
    logger.info(f"No watermark for {source_id} — this is a first run")
    return None

def update_watermark(source_type, source_id, last_date, records_inserted):
    """
    Update watermark ONLY after confirmed successful insert.
    
    Critical: if we update this before the insert succeeds, 
    a crash means we'd skip data on the next run — silent data loss.
    The order of operations matters.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO pipeline_watermarks 
            (source_type, source_id, last_successful_date, last_run_at, records_inserted)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (source_type, source_id) DO UPDATE SET
            last_successful_date = EXCLUDED.last_successful_date,
            last_run_at          = EXCLUDED.last_run_at,
            records_inserted     = EXCLUDED.records_inserted
    """, (source_type, source_id, last_date, datetime.now(), records_inserted))
    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"Watermark updated for {source_id} → {last_date}")

# ─────────────────────────────────────────────
# BANK OF CANADA INGESTION
# ─────────────────────────────────────────────

def fetch_series(series_id, start_date, end_date):
    """Fetch a single Bank of Canada series between two dates."""
    url = f"{BASE_URL}/observations/{series_id}/json"
    params = {"start_date": start_date, "end_date": end_date}

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        observations = data.get("observations", [])

        if not observations:
            logger.warning(f"No data returned for {series_id} between {start_date} and {end_date}")
            return pd.DataFrame()

        records = []
        for obs in observations:
            value = obs.get(series_id, {}).get("v")
            records.append({
                "series_id":   series_id,
                "series_name": SERIES.get(series_id, series_id),
                "date":        obs["d"],
                "value":       float(value) if value else None,
                "ingested_at": datetime.now().isoformat()
            })

        return pd.DataFrame(records)

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch {series_id}: {e}")
        return pd.DataFrame()

def fetch_and_store_series(series_id, frequency):
    """
    Fetch a single series using its watermark as the start date.
    Update the watermark only after successful storage.
    
    This is the incremental loading pattern:
    1. Read watermark (where did I stop?)
    2. Fetch only NEW data (from watermark → today)
    3. Store it
    4. Update watermark (record where I stopped)
    
    If step 3 crashes, step 4 never runs.
    Next run will retry from the same watermark. No gaps, no duplicates.
    """
    end_date = datetime.now().strftime("%Y-%m-%d")
    
    # Step 1: Read watermark
    watermark = get_watermark('market_data', series_id)
    
    if watermark:
        # Start from the day AFTER our last successful date
        start_dt = datetime.strptime(watermark, "%Y-%m-%d") + timedelta(days=1)
        start_date = start_dt.strftime("%Y-%m-%d")
        logger.info(f"{series_id}: incremental load from {start_date}")
    else:
        # First run — use default lookback
        if frequency == 'daily':
            start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        else:
            start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
        logger.info(f"{series_id}: first run, loading from {start_date}")
    
    # If watermark is already today, nothing to do
    if start_date > end_date:
        logger.info(f"{series_id}: already up to date, skipping")
        return 0
    
    # Step 2: Fetch only new data
    df = fetch_series(series_id, start_date, end_date)
    if df.empty:
        return 0
    df["frequency"] = frequency
    
    # Step 3: Store it
    inserted = store_market_data_for_series(df)
    
    # Step 4: Update watermark — only runs if step 3 succeeded
    if inserted >= 0:  # 0 is valid (conflict on all rows), -1 would be error
        latest_date = df['date'].max()
        update_watermark('market_data', series_id, latest_date, inserted)
    
    return inserted

def fetch_all_series():
    """
    Fetch all series incrementally using watermarks.
    Each series is independent — a failure in one doesn't affect others.
    """
    daily_series   = ["FXUSDCAD", "FXEURCAD", "FXGBPCAD"]
    monthly_series = ["V122514", "INDINF_CPITOT_M"]
    
    total = 0
    for sid in daily_series:
        total += fetch_and_store_series(sid, 'daily')
    for sid in monthly_series:
        total += fetch_and_store_series(sid, 'monthly')
    
    logger.info(f"Total new market records ingested: {total}")
    return total

def store_market_data_for_series(df):
    """Store market data rows, return count inserted."""
    if df.empty:
        return 0
    
    conn = get_db_connection()
    cur  = conn.cursor()

    records = [
        (row['series_id'], row['series_name'], row['date'], row['value'], row['frequency'])
        for _, row in df.iterrows()
    ]

    execute_values(cur, """
        INSERT INTO raw_market_data (series_id, series_name, date, value, frequency)
        VALUES %s
        ON CONFLICT (series_id, date) DO NOTHING
    """, records)

    inserted = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()

    logger.info(f"Stored {inserted} new records for {df['series_id'].iloc[0]}")
    return inserted

# Keep this for backward compatibility with DAG
def store_market_data(df):
    return store_market_data_for_series(df)

# ─────────────────────────────────────────────
# EDGAR INGESTION
# ─────────────────────────────────────────────

def fetch_edgar_filings(cik, company_name, form_type='40-F', limit=10):
    """Fetch EDGAR filings. Watermark filtering happens in fetch_and_store_edgar."""
    try:
        r = requests.get(
            f'https://data.sec.gov/submissions/CIK{cik}.json',
            headers=EDGAR_HEADERS
        )
        data = r.json()
        filings = []

        def extract_filings(filing_data):
            forms      = filing_data['form']
            dates      = filing_data['filingDate']
            accessions = filing_data['accessionNumber']
            reports    = filing_data.get('reportDate', [''] * len(forms))
            for form, date, acc, report in zip(forms, dates, accessions, reports):
                if form_type in form:
                    filings.append({
                        'company_name':     company_name,
                        'cik':              cik,
                        'form_type':        form,
                        'filing_date':      date,
                        'accession_number': acc,
                        'period_ending':    report or None,
                        'document_url':     f"https://www.sec.gov/Archives/edgar/full-index/{date[:4]}/{acc}"
                    })
                if len(filings) >= limit:
                    return

        extract_filings(data['filings']['recent'])

        archive_files = data['filings'].get('files', [])
        for archive in archive_files:
            if len(filings) >= limit:
                break
            archive_url = f"https://data.sec.gov/submissions/{archive['name']}"
            r2 = requests.get(archive_url, headers=EDGAR_HEADERS)
            extract_filings(r2.json())

        logger.info(f"Found {len(filings)} {form_type} filings for {company_name}")
        return filings

    except Exception as e:
        logger.error(f"Error fetching EDGAR filings for {company_name}: {e}")
        return []

def fetch_and_store_edgar(company_name, cik):
    """
    Fetch and store EDGAR filings for one company, using watermark.
    Same pattern as market data — read watermark, fetch new, store, update.
    """
    # Step 1: Read watermark
    watermark = get_watermark('edgar_filings', cik)
    
    # Step 2: Fetch filings
    filings = fetch_edgar_filings(cik, company_name, limit=10)
    
    # If no filings returned, just exit (could be API issue or no data)
    if not filings:
        return 0
    
    # Filter to only new filings if we have a watermark
    if watermark:
        new_filings = [f for f in filings if f['filing_date'] > watermark]
        logger.info(f"{company_name}: {len(new_filings)} new filings after {watermark}")
    else:
        new_filings = filings
        logger.info(f"{company_name}: first run, storing all {len(new_filings)} filings")
    
    if not new_filings:
        return 0
    
    # Step 3: Store
    inserted = store_edgar_filings(new_filings)
    
    # Step 4: Update watermark only after successful store
    if inserted >= 0:
        latest_date = max(f['filing_date'] for f in new_filings)
        update_watermark('edgar_filings', cik, latest_date, inserted)
    
    return inserted


def store_edgar_filings(filings):
    """Store EDGAR filings in PostgreSQL."""

    # If no filings to store then return 
    if not filings:
        return 0

    conn = get_db_connection()
    cur  = conn.cursor()

    records = [
        (f['company_name'], f['cik'], f['form_type'],
         f['filing_date'], f['accession_number'], 
         f.get('period_ending') or None,f.get('document_url'))
        for f in filings
    ]

    execute_values(cur, """
        INSERT INTO raw_edgar_filings
            (company_name, cik, form_type, filing_date, accession_number,  period_ending, document_url)
        VALUES %s
        ON CONFLICT (accession_number) DO NOTHING
    """, records)

    inserted = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()

    logger.info(f"Stored {inserted} new EDGAR filings")
    return inserted


if __name__ == "__main__":
    print("=== Ingesting Bank of Canada Data (Incremental) ===")
    total_market = fetch_all_series()
    print(f"New market records: {total_market}")

    print("\n=== Ingesting EDGAR Filings (Incremental) ===")
    total_edgar = 0
    for company, cik in CANADIAN_COMPANIES.items():
        total_edgar += fetch_and_store_edgar(company, cik)
    print(f"New EDGAR filings: {total_edgar}")

    print("\n=== Watermark State ===")
    conn = get_db_connection()
    cur  = conn.cursor()
    cur.execute("SELECT source_type, source_id, last_successful_date, last_run_at FROM pipeline_watermarks ORDER BY source_type, source_id")
    for row in cur.fetchall():
        print(f"  {row[0]:15} | {row[1]:25} | last date: {row[2]} | run at: {row[3]}")
    cur.close()
    conn.close()