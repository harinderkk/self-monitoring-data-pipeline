import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import logging
import os
import uuid
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

EXPECTED_COMPANIES = [
    'Royal Bank of Canada',
    'Toronto Dominion Bank',
    'CIBC',
    'Suncor Energy',
    'TC Energy',
    'Teck Resources',
    'BCE Inc',
    'Sun Life Financial',
]

EXPECTED_SERIES = [
    'FXUSDCAD',
    'FXEURCAD',
    'FXGBPCAD',
    'V122514',
    'INDINF_CPITOT_M',
]

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=os.getenv('POSTGRES_PORT', 5432),
        database=os.getenv('POSTGRES_DB', 'sedar_db'),
        user=os.getenv('POSTGRES_USER', 'sedar_user'),
        password=os.getenv('POSTGRES_PASSWORD', 'sedar_pass')
    )

def check_missing_companies(conn):
    """Check if any expected companies are missing from recent filings."""
    anomalies = []
    cur = conn.cursor()

    cur.execute("""
        SELECT DISTINCT company_name 
        FROM raw_edgar_filings
        WHERE filing_date >= NOW() - INTERVAL '400 days'
    """)

    companies_in_db = {row[0] for row in cur.fetchall()}
    missing = [c for c in EXPECTED_COMPANIES if c not in companies_in_db]

    if missing:
        anomalies.append({
            'type': 'missing_companies',
            'severity': 'high',
            'description': f"{len(missing)} companies have no recent filings: {', '.join(missing)}",
            'details': missing
        })
        logger.warning(f"Missing companies: {missing}")
    else:
        logger.info("All expected companies have recent filings")

    cur.close()
    return anomalies

def check_volume_anomalies(conn):
    """Check if record counts are abnormally low or data is stale."""
    anomalies = []
    cur = conn.cursor()

    cur.execute("""
        SELECT 
            series_id,
            frequency,
            COUNT(*) as record_count,
            MAX(date) as latest_date
        FROM raw_market_data
        GROUP BY series_id, frequency
    """)

    rows = cur.fetchall()

    for row in rows:
        series_id, frequency, count, latest_date = row

        # Context aware threshold — monthly series allowed to be older
        stale_threshold = 5 if frequency == 'daily' else 40

        if latest_date:
            days_old = (datetime.now().date() - latest_date).days
            if days_old > stale_threshold:
                anomalies.append({
                    'type': 'stale_data',
                    'severity': 'medium',
                    'description': f"{series_id} ({frequency}) last updated {days_old} days ago. Threshold is {stale_threshold} days.",
                    'details': {'series_id': series_id, 'days_old': days_old}
                })
                logger.warning(f"Stale data: {series_id} is {days_old} days old")

        if count < 5:
            anomalies.append({
                'type': 'low_volume',
                'severity': 'high',
                'description': f"{series_id} only has {count} records. Expected significantly more.",
                'details': {'series_id': series_id, 'count': count}
            })
            logger.warning(f"Low volume: {series_id} has only {count} records")

    cur.close()
    return anomalies

def check_null_values(conn):
    """Check for unexpected null values in critical fields."""
    anomalies = []
    cur = conn.cursor()

    cur.execute("""
        SELECT 
            series_id,
            COUNT(*) as total,
            SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) as null_count
        FROM raw_market_data
        GROUP BY series_id
        HAVING SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) > 0
    """)

    rows = cur.fetchall()

    for row in rows:
        series_id, total, null_count = row
        pct = round((null_count / total) * 100, 1)
        anomalies.append({
            'type': 'null_values',
            'severity': 'high' if pct > 10 else 'medium',
            'description': f"{series_id} has {null_count} null values ({pct}% of {total} records).",
            'details': {'series_id': series_id, 'null_count': null_count, 'pct': pct}
        })
        logger.warning(f"Null values in {series_id}: {null_count}/{total} ({pct}%)")

    if not rows:
        logger.info("No null values detected in market data")

    cur.close()
    return anomalies

def check_duplicate_records(conn):
    """Check for duplicate records in market data."""
    anomalies = []
    cur = conn.cursor()

    cur.execute("""
        SELECT series_id, date, COUNT(*) as cnt
        FROM raw_market_data
        GROUP BY series_id, date
        HAVING COUNT(*) > 1
    """)

    rows = cur.fetchall()

    if rows:
        anomalies.append({
            'type': 'duplicate_records',
            'severity': 'medium',
            'description': f"{len(rows)} duplicate series/date combinations found in market data.",
            'details': [{'series_id': r[0], 'date': str(r[1]), 'count': r[2]} for r in rows]
        })
        logger.warning(f"Found {len(rows)} duplicate records")
    else:
        logger.info("No duplicate records detected")

    cur.close()
    return anomalies

def check_value_anomalies(conn):
    """Check for values that are statistical outliers using z-score."""
    anomalies = []
    cur = conn.cursor()

    cur.execute("""
        SELECT 
            series_id,
            date,
            value,
            AVG(value) OVER (PARTITION BY series_id) as avg_val,
            STDDEV(value) OVER (PARTITION BY series_id) as std_val
        FROM raw_market_data
        WHERE frequency = 'daily'
        AND value IS NOT NULL
    """)

    rows = cur.fetchall()

    for series_id, date, value, avg_val, std_val in rows:
        if std_val and std_val > 0:
            z_score = abs((value - avg_val) / std_val)
            if z_score > 3:
                anomalies.append({
                    'type': 'value_anomaly',
                    'severity': 'high',
                    'description': f"{series_id} on {date} has value {value} which is {round(z_score, 1)} standard deviations from mean ({round(avg_val, 4)}). Possible data corruption.",
                    'details': {
                        'series_id': series_id,
                        'date': str(date),
                        'value': float(value),
                        'avg': float(avg_val),
                        'z_score': float(z_score)
                    }
                })
                logger.warning(f"Value anomaly: {series_id} on {date} z-score={round(z_score, 1)}")

    if not anomalies:
        logger.info("No value anomalies detected")

    cur.close()
    return anomalies

def log_anomalies(conn, anomalies, run_id):
    """Store detected anomalies — skip duplicates from last 24 hours."""
    if not anomalies:
        return

    cur = conn.cursor()
    logged = 0

    for anomaly in anomalies:
        # Check if same anomaly was already logged in last 24 hours
        cur.execute("""
            SELECT COUNT(*) FROM anomaly_log
            WHERE anomaly_type = %s
            AND description = %s
            AND detected_at >= NOW() - INTERVAL '24 hours'
        """, (anomaly['type'], anomaly['description']))

        already_exists = cur.fetchone()[0] > 0

        if not already_exists:
            cur.execute("""
                INSERT INTO anomaly_log
                    (run_id, anomaly_type, severity, description)
                VALUES (%s, %s, %s, %s)
            """, (
                run_id,
                anomaly['type'],
                anomaly['severity'],
                anomaly['description']
            ))
            logged += 1
        else:
            logger.info(f"Skipping duplicate anomaly: {anomaly['type']}")

    conn.commit()
    cur.close()
    logger.info(f"Logged {logged} new anomalies (skipped duplicates)")

def log_pipeline_health(conn, run_id, anomaly_count, status):
    """Log overall pipeline health for this run."""
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM raw_market_data")
    market_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM raw_edgar_filings")
    edgar_count = cur.fetchone()[0]

    cur.execute("""
        INSERT INTO pipeline_health_log
            (run_id, pipeline_name, status, records_expected,
             records_received, anomalies_detected)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        run_id,
        'sedar_pipeline',
        status,
        len(EXPECTED_COMPANIES) + len(EXPECTED_SERIES),
        market_count + edgar_count,
        anomaly_count
    ))

    conn.commit()
    cur.close()

def run_all_checks():
    """Run all monitoring checks and return results."""
    run_id = str(uuid.uuid4())[:8]
    logger.info(f"Starting monitoring run {run_id}")

    conn = get_db_connection()
    all_anomalies = []

    checks = [
        ("Missing Companies", check_missing_companies),
        ("Volume Anomalies",  check_volume_anomalies),
        ("Null Values",       check_null_values),
        ("Duplicate Records", check_duplicate_records),
        ("Value Anomalies",   check_value_anomalies),
    ]

    for check_name, check_fn in checks:
        logger.info(f"Running check: {check_name}")
        try:
            anomalies = check_fn(conn)
            all_anomalies.extend(anomalies)
        except Exception as e:
            logger.error(f"Check {check_name} failed: {e}")
            all_anomalies.append({
                'type': 'check_failed',
                'severity': 'critical',
                'description': f"Monitor check '{check_name}' threw an error: {str(e)}",
                'details': {}
            })

    status = 'healthy' if not all_anomalies else 'anomalies_detected'
    log_anomalies(conn, all_anomalies, run_id)
    log_pipeline_health(conn, run_id, len(all_anomalies), status)

    conn.close()

    print(f"\n{'='*50}")
    print(f"PIPELINE HEALTH REPORT — Run {run_id}")
    print(f"{'='*50}")
    print(f"Status: {status.upper()}")
    print(f"Total anomalies detected: {len(all_anomalies)}")

    if all_anomalies:
        print(f"\nAnomalies found:")
        for a in all_anomalies:
            print(f"  [{a['severity'].upper()}] {a['type']}: {a['description']}")
    else:
        print("\nAll checks passed. Pipeline is healthy.")

    return all_anomalies

if __name__ == "__main__":
    run_all_checks()