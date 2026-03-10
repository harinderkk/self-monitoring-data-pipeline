import os
import json
import psycopg2
from openai import OpenAI
from dotenv import load_dotenv
import logging
from datetime import datetime

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

client = OpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=os.getenv("OPENROUTER_API_KEY"),
)

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=os.getenv('POSTGRES_PORT', 5432),
        database=os.getenv('POSTGRES_DB', 'sedar_db'),
        user=os.getenv('POSTGRES_USER', 'sedar_user'),
        password=os.getenv('POSTGRES_PASSWORD', 'sedar_pass')
    )

def get_pipeline_context(conn):
    """Pull current pipeline state to give the LLM context."""
    cur = conn.cursor()

    cur.execute("""
        SELECT series_id, MAX(date) as latest, COUNT(*) as records
        FROM raw_market_data
        GROUP BY series_id
    """)
    market_summary = cur.fetchall()

    cur.execute("""
        SELECT company_name, MAX(filing_date) as latest, COUNT(*) as filings
        FROM raw_edgar_filings
        GROUP BY company_name
        ORDER BY latest DESC
    """)
    filing_summary = cur.fetchall()

    cur.close()

    context = "CURRENT PIPELINE STATE:\n\n"
    context += "Bank of Canada Series:\n"
    for series_id, latest, records in market_summary:
        context += f"  - {series_id}: {records} records, latest date {latest}\n"

    context += "\nEDGAR Filings by Company:\n"
    for company, latest, filings in filing_summary:
        context += f"  - {company}: {filings} filings, most recent {latest}\n"

    return context

def explain_anomaly(anomaly, pipeline_context):
    """Send anomaly to LLM and get plain English explanation."""

    prompt = f"""You are a senior data engineer reviewing a pipeline monitoring alert.

{pipeline_context}

ANOMALY DETECTED:
Type: {anomaly['anomaly_type']}
Severity: {anomaly['severity']}
Description: {anomaly['description']}

Write a plain English explanation of this anomaly for a data engineering team.
Your response must be a JSON object with exactly these three fields:
{{
  "explanation": "2-3 sentences explaining what happened and why it likely occurred",
  "impact": "1 sentence on what downstream impact this could have",
  "suggested_fix": "1-2 concrete actionable steps to resolve this"
}}

Be specific. Reference the actual series names, company names, and dates from the description.
Do not use technical jargon like 'null values' — explain it as a business person would understand.
Return only valid JSON, nothing else."""

    try:
        response = client.chat.completions.create(
            model="anthropic/claude-3-haiku",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=500,
        )

        raw = response.choices[0].message.content.strip()

        # Strip markdown code blocks if present
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip()

        result = json.loads(raw)
        logger.info(f"LLM explanation generated for {anomaly['anomaly_type']}")
        return result

    except Exception as e:
        logger.error(f"LLM call failed: {e}")
        return {
            "explanation": anomaly['description'],
            "impact": "Unknown impact — LLM explanation unavailable.",
            "suggested_fix": "Review pipeline logs manually."
        }

def update_anomaly_with_explanation(conn, anomaly_id, explanation, suggested_fix):
    """Store LLM explanation back into the anomaly_log table."""
    cur = conn.cursor()
    cur.execute("""
        UPDATE anomaly_log
        SET llm_explanation = %s,
            suggested_fix = %s
        WHERE id = %s
    """, (explanation, suggested_fix, anomaly_id))
    conn.commit()
    cur.close()

def process_unexplained_anomalies():
    """Find all anomalies without LLM explanations and explain them."""
    conn = get_db_connection()
    cur = conn.cursor()

    # Get all anomalies that haven't been explained yet
    cur.execute("""
        SELECT id, run_id, anomaly_type, severity, description
        FROM anomaly_log
        WHERE llm_explanation IS NULL
        ORDER BY detected_at DESC
    """)

    anomalies = cur.fetchall()
    cur.close()

    if not anomalies:
        logger.info("No unexplained anomalies found")
        conn.close()
        return []

    logger.info(f"Found {len(anomalies)} anomalies to explain")
    pipeline_context = get_pipeline_context(conn)

    results = []

    for anomaly_id, run_id, anomaly_type, severity, description in anomalies:
        anomaly = {
            'anomaly_type': anomaly_type,
            'severity': severity,
            'description': description
        }

        print(f"\n{'='*60}")
        print(f"ANOMALY: {anomaly_type} [{severity.upper()}]")
        print(f"Raw description: {description}")
        print(f"{'='*60}")

        llm_result = explain_anomaly(anomaly, pipeline_context)

        print(f"\nLLM EXPLANATION:")
        print(f"  What happened: {llm_result.get('explanation')}")
        print(f"  Impact:        {llm_result.get('impact')}")
        print(f"  Suggested fix: {llm_result.get('suggested_fix')}")

        update_anomaly_with_explanation(
            conn,
            anomaly_id,
            llm_result.get('explanation'),
            llm_result.get('suggested_fix')
        )

        results.append({
            'anomaly_id': anomaly_id,
            'type': anomaly_type,
            'severity': severity,
            'explanation': llm_result.get('explanation'),
            'impact': llm_result.get('impact'),
            'suggested_fix': llm_result.get('suggested_fix')
        })

    conn.close()
    logger.info(f"Explained {len(results)} anomalies")
    return results


def send_slack_alert(anomaly_result):
    """Send formatted anomaly alert to Slack."""
    from slack_sdk import WebClient
    
    slack_client = WebClient(token=os.getenv('SLACK_BOT_TOKEN'))
    
    severity_emoji = {
        'critical': '🔴',
        'high':     '🟠',
        'medium':   '🟡',
        'low':      '🟢'
    }
    
    emoji = severity_emoji.get(anomaly_result['severity'].lower(), '⚪')
    
    message = f"""{emoji} *PIPELINE ALERT — {anomaly_result['type'].upper().replace('_', ' ')}*
*Severity:* {anomaly_result['severity'].upper()}

*What happened:*
{anomaly_result['explanation']}

*Business Impact:*
{anomaly_result['impact']}

*Suggested Fix:*
{anomaly_result['suggested_fix']}

_Detected at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC_"""

    result = slack_client.chat_postMessage(
        channel='#pipeline-alerts',
        text=message,
        mrkdwn=True
    )
    
    logger.info(f"Slack alert sent: {result['ok']}")
    return result['ok']

def mark_slack_sent(conn, anomaly_id):
    """Mark anomaly as having been sent to Slack."""
    cur = conn.cursor()
    cur.execute("""
        UPDATE anomaly_log
        SET slack_sent = TRUE
        WHERE id = %s
    """, (anomaly_id,))
    conn.commit()
    cur.close()


if __name__ == "__main__":
    from datetime import datetime
    
    conn = get_db_connection()
    results = process_unexplained_anomalies()
    
    # Send Slack alerts for any explained anomalies not yet sent
    cur = conn.cursor()
    cur.execute("""
        SELECT id, anomaly_type, severity, description, 
               llm_explanation, suggested_fix
        FROM anomaly_log
        WHERE slack_sent = FALSE
        AND llm_explanation IS NOT NULL
    """)
    
    unsent = cur.fetchall()
    cur.close()
    
    print(f"\nSending {len(unsent)} Slack alerts...")
    
    for row in unsent:
        anomaly_id, anomaly_type, severity, description, explanation, suggested_fix = row
        
        anomaly_result = {
            'type':         anomaly_type,
            'severity':     severity,
            'description':  description,
            'explanation':  explanation,
            'impact':       'See pipeline dashboard for downstream effects.',
            'suggested_fix': suggested_fix
        }
        
        sent = send_slack_alert(anomaly_result)
        
        if sent:
            mark_slack_sent(conn, anomaly_id)
            print(f"  Alert sent for {anomaly_type}")
    
    conn.close()
    print("\nDone.")
