import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

st.set_page_config(
    page_title="Canadian Financial Pipeline Monitor",
    page_icon="🇨🇦",
    layout="wide"
)


@st.cache_data(ttl=300)
def query(sql):
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=os.getenv('POSTGRES_PORT', 5432),
        database=os.getenv('POSTGRES_DB', 'sedar_db'),
        user=os.getenv('POSTGRES_USER', 'sedar_user'),
        password=os.getenv('POSTGRES_PASSWORD', 'sedar_pass')
    )
    cur = conn.cursor()
    cur.execute(sql)
    cols = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return pd.DataFrame(rows, columns=cols)

# ── Header ──────────────────────────────────────────────
st.title("🇨🇦 Canadian Financial Pipeline Monitor")
st.caption(f"Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# ── Top KPI Cards ────────────────────────────────────────
health_df = query("SELECT * FROM analytics.mart_pipeline_health ORDER BY run_date DESC")
anomaly_df = query("""
    SELECT * FROM anomaly_log 
    ORDER BY detected_at DESC 
    LIMIT 50
""")
filings_df = query("SELECT * FROM analytics.mart_filing_summary ORDER BY company_name")
rates_df = query("SELECT * FROM analytics.mart_daily_rates ORDER BY date DESC")

col1, col2, col3, col4 = st.columns(4)

with col1:
    total_runs = int(health_df['total_runs'].sum()) if not health_df.empty else 0
    st.metric("Total Pipeline Runs", total_runs)

with col2:
    total_anomalies = int(anomaly_df.shape[0])
    st.metric("Anomalies Detected", total_anomalies)

with col3:
    companies = int(filings_df['company_name'].nunique()) if not filings_df.empty else 0
    st.metric("Companies Tracked", companies)

with col4:
    if not health_df.empty and 'health_score_pct' in health_df.columns:
        score = float(health_df['health_score_pct'].iloc[0])
        st.metric("Latest Health Score", f"{score}%")
    else:
        st.metric("Latest Health Score", "N/A")

st.divider()

# ── Exchange Rates Chart ─────────────────────────────────
st.subheader("📈 CAD Exchange Rates")

if not rates_df.empty:
    rates_melted = rates_df[['date', 'usd_cad', 'eur_cad', 'gbp_cad']].melt(
        id_vars='date',
        var_name='pair',
        value_name='rate'
    ).dropna()

    rates_melted['pair'] = rates_melted['pair'].map({
        'usd_cad': 'USD/CAD',
        'eur_cad': 'EUR/CAD',
        'gbp_cad': 'GBP/CAD'
    })

    fig = px.line(
        rates_melted,
        x='date',
        y='rate',
        color='pair',
        title='Exchange Rates vs CAD (Last 30 Days)',
        labels={'rate': 'Rate', 'date': 'Date', 'pair': 'Currency Pair'}
    )
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No exchange rate data available yet.")

st.divider()

# ── Filing Summary + Pipeline Health ────────────────────
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("🏦 EDGAR Filings by Company")
    if not filings_df.empty:
        fig2 = px.bar(
            filings_df,
            x='company_name',
            y='total_filings',
            title='Total Filings per Company',
            color='total_filings',
            color_continuous_scale='Blues'
        )
        fig2.update_layout(
            height=400,
            xaxis_tickangle=-45,
            showlegend=False
        )
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No filing data available yet.")

with col_right:
    st.subheader("🔍 Pipeline Health Over Time")
    if not health_df.empty:
        fig3 = go.Figure()
        fig3.add_trace(go.Bar(
            x=health_df['run_date'],
            y=health_df['total_anomalies'],
            name='Anomalies',
            marker_color='crimson'
        ))
        fig3.add_trace(go.Scatter(
            x=health_df['run_date'],
            y=health_df['health_score_pct'],
            name='Health Score %',
            yaxis='y2',
            line=dict(color='green', width=2)
        ))
        fig3.update_layout(
            height=400,
            yaxis=dict(title='Anomalies'),
            yaxis2=dict(
                title='Health Score %',
                overlaying='y',
                side='right'
            )
        )
        st.plotly_chart(fig3, use_container_width=True)
    else:
        st.info("No health log data available yet.")

st.divider()

# ── Anomaly Log Table ────────────────────────────────────
st.subheader("⚠️ Recent Anomalies")

if not anomaly_df.empty:
    display_cols = ['detected_at', 'anomaly_type', 'severity',
                    'description', 'llm_explanation', 'suggested_fix', 'slack_sent']
    available = [c for c in display_cols if c in anomaly_df.columns]

    def highlight_severity(row):
        if row.get('severity') == 'critical':
            return ['background-color: #ffcccc'] * len(row)
        elif row.get('severity') == 'high':
            return ['background-color: #ffe5cc'] * len(row)
        elif row.get('severity') == 'medium':
            return ['background-color: #fffacc'] * len(row)
        return [''] * len(row)

    styled = anomaly_df[available].style.apply(highlight_severity, axis=1)
    st.dataframe(styled, use_container_width=True, height=300)
else:
    st.success("✅ No anomalies detected.")

st.divider()

# ── Filing Details Table ─────────────────────────────────
st.subheader("📋 Company Filing Details")

if not filings_df.empty:
    st.dataframe(filings_df, use_container_width=True)

# ── Footer ───────────────────────────────────────────────
st.caption("Data sources: SEC EDGAR · Bank of Canada API · PostgreSQL · dbt · Apache Airflow")