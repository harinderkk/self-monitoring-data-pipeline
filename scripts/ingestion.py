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

# Bank of Canada config
BASE_URL = "https://www.bankofcanada.ca/valet"

SERIES = {
    "FXUSDCAD": "USD/CAD Exchange Rate",
    "FXEURCAD": "EUR/CAD Exchange Rate",
    "FXGBPCAD": "GBP/CAD Exchange Rate",
    "V122514":  "Overnight Rate",
    "INDINF_CPITOT_M": "Commodity Price Index Total",
}

# EDGAR config
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

def get_db_connection():
    """Get PostgreSQL connection."""
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=os.getenv('POSTGRES_PORT', 5432),
        database=os.getenv('POSTGRES_DB', 'sedar_db'),
        user=os.getenv('POSTGRES_USER', 'sedar_user'),
        password=os.getenv('POSTGRES_PASSWORD', 'sedar_pass')
    )

def fetch_series(series_id, start_date=None, end_date=None):
    """Fetch a single Bank of Canada series."""
    if not start_date:
        start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")

    url = f"{BASE_URL}/observations/{series_id}/json"
    params = {"start_date": start_date, "end_date": end_date}

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        observations = data.get("observations", [])

        if not observations:
            logger.warning(f"No data returned for {series_id}")
            return pd.DataFrame()

        records = []
        for obs in observations:
            value = obs.get(series_id, {}).get("v")
            records.append({
                "series_id": series_id,
                "series_name": SERIES.get(series_id, series_id),
                "date": obs["d"],
                "value": float(value) if value else None,
                "ingested_at": datetime.now().isoformat()
            })

        return pd.DataFrame(records)

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch {series_id}: {e}")
        return pd.DataFrame()

def fetch_all_series():
    """Fetch all Bank of Canada series."""
    all_data = []

    daily_series  = ["FXUSDCAD", "FXEURCAD", "FXGBPCAD"]
    monthly_series = ["V122514", "INDINF_CPITOT_M"]

    daily_start   = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    monthly_start = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    end_date      = datetime.now().strftime("%Y-%m-%d")

    for sid in daily_series:
        df = fetch_series(sid, daily_start, end_date)
        if not df.empty:
            df["frequency"] = "daily"
            all_data.append(df)

    for sid in monthly_series:
        df = fetch_series(sid, monthly_start, end_date)
        if not df.empty:
            df["frequency"] = "monthly"
            all_data.append(df)

    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

def store_market_data(df):
    """Store Bank of Canada data in PostgreSQL."""
    if df.empty:
        logger.warning("No market data to store")
        return 0

    conn = get_db_connection()
    cur  = conn.cursor()

    records = [
        (
            row['series_id'],
            row['series_name'],
            row['date'],
            row['value'],
            row.get('frequency', 'daily')
        )
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

    logger.info(f"Stored {inserted} new market data records")
    return inserted


def fetch_edgar_filings(cik, company_name, form_type='40-F', limit=10):
    """Fetch EDGAR filings including archive files."""
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
            for form, date, acc in zip(forms, dates, accessions):
                if form_type in form:
                    filings.append({
                        'company_name':     company_name,
                        'cik':              cik,
                        'form_type':        form,
                        'filing_date':      date,
                        'accession_number': acc,
                        'document_url':     f"https://www.sec.gov/Archives/edgar/full-index/{date[:4]}/{acc}"
                    })
                if len(filings) >= limit:
                    return

        # Search recent filings first
        extract_filings(data['filings']['recent'])

        # If we need more, search archive files
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

def store_edgar_filings(filings):
    """Store EDGAR filings in PostgreSQL."""
    if not filings:
        return 0

    conn = get_db_connection()
    cur  = conn.cursor()

    records = [
        (
            f['company_name'],
            f['cik'],
            f['form_type'],
            f['filing_date'],
            f['accession_number'],
            f.get('document_url')
        )
        for f in filings
    ]

    execute_values(cur, """
        INSERT INTO raw_edgar_filings
            (company_name, cik, form_type, filing_date, accession_number, document_url)
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
    print("=== Ingesting Bank of Canada Data ===")
    market_df = fetch_all_series()
    stored_market = store_market_data(market_df)
    print(f"Market data records stored: {stored_market}")

    print("\n=== Ingesting EDGAR Filings ===")
    all_filings = []
    for company, cik in CANADIAN_COMPANIES.items():
        filings = fetch_edgar_filings(cik, company, limit=5)
        all_filings.extend(filings)

    stored_edgar = store_edgar_filings(all_filings)
    print(f"EDGAR filings stored: {stored_edgar}")

    print("\n=== Verifying Database ===")
    conn = get_db_connection()
    cur  = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM raw_market_data")
    print(f"Total market data rows: {cur.fetchone()[0]}")

    cur.execute("SELECT COUNT(*) FROM raw_edgar_filings")
    print(f"Total EDGAR filing rows: {cur.fetchone()[0]}")

    cur.execute("""
        SELECT company_name, form_type, filing_date
        FROM raw_edgar_filings
        ORDER BY filing_date DESC
        LIMIT 10
    """)
    print("\nMost recent filings in database:")
    for row in cur.fetchall():
        print(f"  {row[2]} | {row[1]} | {row[0]}")

    cur.close()
    conn.close()