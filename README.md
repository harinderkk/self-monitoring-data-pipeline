# 🇨🇦 AI-Powered Self-Monitoring Canadian Financial Data Pipeline

A production-grade data pipeline that ingests Canadian financial data, automatically detects anomalies, explains them in plain English using an LLM, and fires Slack alerts with suggested fixes — all orchestrated by Apache Airflow and visualized in a live Streamlit dashboard.

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
                    Slack alert fired 🔔
```

**Orchestrated by Apache Airflow — runs automatically every weekday at 9am.**

---

## 🛠️ Tech Stack

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

## 📊 Data Sources

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

## 🤖 What Makes This Different

Most portfolio pipelines ingest data and stop. This one watches itself.

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
has not been updated in 95 days, exceeding the 40-day threshold. This 
means the most recent data is over 3 months old.

Impact: Having outdated inflation data could lead to incorrect economic 
analysis and forecasting.

Suggested fix: Investigate why this series has not been updated. Reach 
out to the Bank of Canada to understand if there are delays in their 
data publishing process.
```

That explanation is then fired to Slack automatically.

---

## 🚀 Quick Start

**Prerequisites:** Docker Desktop, OpenRouter API key, Slack Bot token

1. Clone the repo:
```bash
git clone https://github.com/yourusername/sedar-pipeline.git
cd sedar-pipeline
```

2. Create your `.env` file:
```bash
cp .env.example .env
# Add your OPENROUTER_API_KEY and SLACK_BOT_TOKEN
```

3. Start the full stack:
```bash
docker-compose up -d
```

4. Access the services:
- **Streamlit Dashboard:** http://localhost:8501
- **Airflow UI:** http://localhost:8080
- **PostgreSQL:** localhost:5432

5. Run the pipeline manually:
```bash
docker exec sedar_airflow airflow dags trigger sedar_pipeline
```

---

## 📁 Project Structure
```
sedar-pipeline/
├── scripts/
│   ├── ingestion.py      # Bank of Canada + EDGAR data ingestion
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
raw_market_data        -- Bank of Canada time series (86+ records)
raw_edgar_filings      -- SEC EDGAR filing metadata (57+ records)
pipeline_health_log    -- Every pipeline run logged
anomaly_log            -- Every anomaly with LLM explanation
```

**dbt models (analytics schema):**
```sql
stg_market_data        -- Cleaned market data (view)
stg_edgar_filings      -- Cleaned filing metadata (view)
mart_daily_rates       -- Exchange rates with daily changes (table)
mart_filing_summary    -- One row per company (table)
mart_pipeline_health   -- Daily health scores (table)
```

---

## ⚙️ Airflow DAG
```
ingest_market_data ─┐
                    ├──► run_monitor ──► run_alerts
ingest_edgar_filings─┘
```

Schedule: `0 9 * * 1-5` — 9am every weekday

---

## 🧪 Chaos Testing

`scripts/chaos.py` injects 6 types of controlled failures to test the monitor:
- Missing values
- Duplicate records  
- Schema drift
- Late data
- Corrupted values
- Missing series

---

## 📝 Notes

- Airflow is configured with SequentialExecutor for local development
- In production this would use CeleryExecutor or KubernetesExecutor on Linux
- SEDAR+ (Canadian filing system) was evaluated but blocked by Radware bot protection — SEC EDGAR was chosen as the data source for Canadian cross-listed companiesx