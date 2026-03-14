# 🇨🇦 AI-Powered Self-Monitoring Canadian Financial Data Pipeline

Most portfolio pipelines ingest data and stop. This one watches itself.

It ingests Canadian financial data from two sources, runs 5 automated health 
checks on every run, explains anomalies in plain English using an LLM, and 
fires Slack alerts with suggested fixes — all orchestrated by Airflow and 
visualized in a live Streamlit dashboard.

---

## 🏗️ Architecture
```
Bank of Canada API ─┐
                    ├──► PostgreSQL ──► dbt models ──► Streamlit Dashboard
SEC EDGAR API      ─┘         │
                              ▼
                    Monitor detects anomaly
                              │
                              ▼
                    LLM explains in plain English
                              │
                              ▼
                    Slack alert sent 🔔
```

**Orchestrated by Apache Airflow — runs automatically every weekday at 9am.**

---

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Python, SEC EDGAR API, Bank of Canada API |
| Storage | PostgreSQL 15 |
| Transformation | dbt |
| Orchestration | Apache Airflow 2.8 |
| AI Layer | OpenRouter API (Claude Haiku) |
| Alerting | Slack SDK |
| Dashboard | Streamlit + Plotly |
| Infrastructure | Docker + Docker Compose |

---

## Data Sources

### Bank of Canada API
- USD/CAD, EUR/CAD, GBP/CAD exchange rates (daily)
- Overnight interest rate (monthly)
- Commodity Price Index (monthly)

### SEC EDGAR — 8 Canadian Companies (40-F filers)
| Company | Sector |
|---|---|
| Royal Bank of Canada | Banking |
| Toronto Dominion Bank | Banking |
| CIBC | Banking |
| Suncor Energy | Energy |
| TC Energy | Pipelines |
| Teck Resources | Mining |
| BCE Inc | Telecom |
| Sun Life Financial | Insurance |

---

## Self-Monitoring Layer

The monitor runs 5 automated checks every pipeline run:
1. **Missing companies** — are all 8 companies reporting?
2. **Volume anomalies** — is data arriving on schedule?
3. **Null values** — are any critical fields empty?
4. **Duplicate records** — is idempotency working?
5. **Value anomalies** — are any values statistically unusual (z-score > 3)?

When an anomaly is detected the LLM layer transforms this:
```
"INDINF_CPITOT_M last updated 95 days ago. Threshold is 40 days."
```
Into this:
```
What happened: The monthly inflation data series from the Bank of Canada 
has not been updated in 95 days, exceeding the 40-day threshold.

Impact: Having outdated inflation data could lead to incorrect economic 
analysis and forecasting.

Suggested fix: Investigate why this series has not been updated. Reach 
out to the Bank of Canada to understand if there are delays in their 
data publishing process.
```

That explanation is then fired to Slack automatically.

---

## Incremental Loading with Watermarks

Every run is idempotent and crash-safe.

The pipeline uses a `pipeline_watermarks` table to track the last 
successfully processed date per source. On each run:

1. Read watermark → "I last processed FXUSDCAD up to 2026-03-13"
2. Fetch only data newer than that date
3. Store it
4. Update the watermark — **only after confirmed successful insert**

If the pipeline crashes in step 3, the watermark never updates. 
The next run retries from the exact same point — no gaps, no duplicates.

Each source has an independent watermark, so a failure in one series 
does not affect others.

This is what Delta Lake handles automatically via transaction logs. 
Building it manually makes the underlying mechanism explicit.
```sql
SELECT source_type, source_id, last_successful_date, last_run_at
FROM pipeline_watermarks
ORDER BY source_type, source_id;
```

---

## Chaos Testing

`scripts/chaos.py` injects 6 types of controlled failures to verify 
the monitor actually catches problems:

- Missing values
- Duplicate records
- Schema drift
- Late data
- Corrupted values
- Missing series

---

## Quick Start

**Prerequisites:** Docker Desktop, OpenRouter API key, Slack Bot token

1. Clone the repo:
```bash
git clone https://github.com/harinderkk/self-monitoring-data-pipeline.git
cd self-monitoring-data-pipeline
```

2. Create the watermarks table in PostgreSQL:
```sql
CREATE TABLE IF NOT EXISTS pipeline_watermarks (
    source_type         VARCHAR(50),
    source_id           VARCHAR(100),
    last_successful_date DATE,
    last_run_at         TIMESTAMP,
    records_inserted    INTEGER DEFAULT 0,
    PRIMARY KEY (source_type, source_id)
);
```

3. Create your `.env` file:
```bash
cp .env.example .env
# Add your OPENROUTER_API_KEY and SLACK_BOT_TOKEN
```

4. Start the full stack:
```bash
docker-compose up -d
```

5. Access the services:
- **Streamlit Dashboard:** http://localhost:8501
- **Airflow UI:** http://localhost:8080
- **PostgreSQL:** localhost:5432

6. Run the pipeline manually:
```bash
docker exec sedar_airflow airflow dags trigger sedar_pipeline
```

---

## 📁 Project Structure
```
sedar-pipeline/
├── scripts/
│   ├── ingestion.py      # Incremental ingestion with watermark pattern
│   ├── monitor.py        # 5 automated pipeline health checks
│   ├── alerts.py         # LLM explanation + Slack alerting
│   └── chaos.py          # Controlled failure injection for testing
├── dags/
│   └── pipeline_dag.py   # Airflow DAG — daily schedule
├── dbt_pipeline/
│   ├── models/staging/   # stg_market_data, stg_edgar_filings
│   └── models/marts/     # mart_daily_rates, mart_filing_summary, mart_pipeline_health
├── app.py                # Streamlit dashboard
├── Dockerfile            # Custom Airflow image with dependencies
└── docker-compose.yml    # Full stack — postgres + airflow + streamlit
```

---

## 🗄️ Database Schema
```sql
-- Raw tables
raw_market_data        -- Bank of Canada time series
raw_edgar_filings      -- SEC EDGAR filing metadata + period_ending
pipeline_health_log    -- Every pipeline run logged
anomaly_log            -- Every anomaly with LLM explanation
pipeline_watermarks    -- Incremental load checkpoints per source

-- dbt models (analytics schema)
stg_market_data        -- Cleaned market data (view)
stg_edgar_filings      -- Cleaned filing metadata (view)
mart_daily_rates       -- Exchange rates with daily changes (table)
mart_filing_summary    -- One row per company (table)
mart_pipeline_health   -- Daily health scores (table)
```

---

## Airflow DAG
```
ingest_market_data ─┐
                    ├──► run_monitor ──► run_alerts
ingest_edgar_filings─┘
```
Schedule: `0 9 * * 1-5` — 9am every weekday

---

## Key Engineering Decisions

**Why SEC EDGAR instead of SEDAR+?**
SEDAR+ (Canada's official filing system) was the original data source. 
It was abandoned after Radware bot protection blocked all programmatic 
access. So, pivoted to SEC EDGAR. Canadian companies cross-listed on US 
exchanges file 40-F forms, which are functionally equivalent to annual 
reports and contain the same financial data.

**Why context-aware staleness thresholds?**
The monitor uses different thresholds for daily vs monthly data — 
5 days for exchange rates, 40 days for the overnight rate and commodity 
index. A single threshold would generate false positives for monthly 
series that are working perfectly but haven't published yet.

**Why SequentialExecutor in Airflow?**
LocalExecutor causes SIGSEGV crashes on macOS Apple Silicon due to a 
known multiprocessing issue. SequentialExecutor runs tasks one at a 
time and is stable. 

**Why raw psycopg2 instead of SQLAlchemy?**
pandas and SQLAlchemy had a version incompatibility that caused silent 
failures on insert. Switched to psycopg2 directly with `execute_values` 
for bulk inserts explicit, fast, and no hidden ORM behaviour.

**Why update the watermark after the insert, not before?**
If the watermark updated before the insert and the pipeline crashed, 
the next run would skip that data permanently — silent data loss with 
no error. Updating after confirmed success means the worst case is a 
duplicate attempt on the next run, which `ON CONFLICT DO NOTHING` 
handles safely.
