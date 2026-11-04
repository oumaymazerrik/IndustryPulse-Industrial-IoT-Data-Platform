import json
import base64
import math
import os
from datetime import datetime, timezone

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import requests
import streamlit as st

st.set_page_config(page_title="IndustryPulse | Industrial Intelligence", page_icon="🏭", layout="wide", initial_sidebar_state="expanded")

DB = dict(host=os.getenv("DB_HOST", "industrypulse-timescaledb"), port=int(os.getenv("DB_PORT", "5432")), dbname=os.getenv("DB_NAME", "industrypulse"), user=os.getenv("DB_USER", "industrypulse"), password=os.getenv("DB_PASSWORD", ""), connect_timeout=4)
API = os.getenv("API_URL", "http://industrypulse-ml-api:8000").rstrip("/")
PROM = os.getenv("PROMETHEUS_URL", "http://industrypulse-prometheus:9090").rstrip("/")
DRIFT = os.getenv("DRIFT_REPORT_PATH", "/app/reports/data_drift.json")
GRAFANA = os.getenv("GRAFANA_URL", "http://localhost:3001")
MLFLOW = os.getenv("MLFLOW_URL", "http://localhost:5000")
REPO = "https://github.com/oumaymazerrik/IndustryPulse-Industrial-IoT-Data-Platform"
PALETTE = ["#398bff", "#6c45ff", "#24c9cb", "#a28bff", "#ffae55"]

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
:root {color-scheme:dark;}
html, body, [class*="css"], .stApp {font-family:'Inter',sans-serif!important;}
.stApp {background:radial-gradient(circle at 55% -25%,#142753 0%,transparent 42%),#080e1b;color:#eaf1ff;}
[data-testid="stSidebar"] {background:#0c1423;border-right:1px solid #1c2a41;}
[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p {color:#a7b7ce;}
[data-testid="stSidebar"] .stRadio label {color:#dce6f6!important;}
[data-testid="stSidebar"] [role="radiogroup"] label {border-radius:12px;padding:7px 9px;}
[data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) {background:#193968;}
.block-container {max-width:1500px;padding:1.5rem 2rem 3rem;}
#MainMenu, footer {visibility:hidden;}
h1,h2,h3,h4 {font-family:'Inter',sans-serif!important;letter-spacing:-.035em!important;color:#f5f8ff!important;}
h1 {font-size:2.05rem!important;font-weight:800!important;}
h3 {font-size:1.1rem!important;}
[data-testid="stMetric"] {background:linear-gradient(140deg,#111d32,#0d1729);border:1px solid #213149;border-radius:14px;padding:17px 16px;min-height:112px;}
[data-testid="stMetricLabel"] p {color:#a8bad2!important;font-size:.82rem!important;}
[data-testid="stMetricValue"] {color:#f1f6ff!important;font-weight:650;font-size:1.8rem!important;}
[data-testid="stPlotlyChart"] {background:#0d1729;border:1px solid #1e2d44;border-radius:15px;padding:10px;}
[data-testid="stDataFrame"] {border:1px solid #253651;border-radius:12px;}
[data-testid="stAlert"] {border-radius:12px;}
.stButton button[kind="primary"],.stDownloadButton button {background:#2569da!important;color:white!important;border:0!important;border-radius:9px!important;font-weight:650;}
[data-baseweb="select"]>div, [data-baseweb="input"]>div {background:#101e33!important;border-color:#2b4263!important;color:white!important;}
[data-testid="stForm"] {background:#0d1729;border:1px solid #233650;border-radius:14px;padding:18px;}
.topline {display:flex;justify-content:space-between;align-items:center;padding:3px 0 20px;gap:12px;}
.brandline {font-size:1.15rem;font-weight:800;color:#f3f8ff;letter-spacing:-.5px;}
.brandline span {color:#4e9dff;}
.subtle {color:#8095b1;font-size:.78rem;}
.pretitle {color:#5e9eff;font-size:.7rem;letter-spacing:2.2px;text-transform:uppercase;font-weight:750;}
.page-title {font-size:2rem;color:#f4f8ff;font-weight:800;letter-spacing:-1px;margin:5px 0 6px;}
.page-desc {color:#9eafc6;font-size:.87rem;margin-bottom:18px;line-height:1.7;}
.section-title {color:#f0f5ff;font-weight:750;font-size:1.01rem;margin:24px 0 12px;}
.panel {background:#0d1729;border:1px solid #1e2e47;border-radius:14px;padding:16px 17px;margin-bottom:10px;}
.panel-title {font-weight:700;color:#dce7f7;font-size:.85rem;margin-bottom:9px;}
.panel-sub {color:#8fa4be;font-size:.77rem;line-height:1.7;}
.mini-card {background:#101d31;border:1px solid #253954;border-radius:12px;padding:13px;margin-bottom:9px;}
.mini-name {font-size:.84rem;color:#e5efff;font-weight:700;}
.mini-info {color:#8ca6c6;font-size:.73rem;margin-top:5px;}
.status-dot {display:inline-block;width:8px;height:8px;border-radius:50%;background:#19d99b;margin-right:7px;}
.badge {display:inline-block;color:#85b8ff;background:#102e54;border:1px solid #24538d;border-radius:100px;padding:5px 10px;font-size:.7rem;margin:0 5px 5px 0;}
.pipeline-step {display:inline-block;background:#132543;border:1px solid #2c5082;border-radius:9px;color:#c9ddfa;padding:11px 12px;margin:5px 4px 5px 0;font-size:.79rem;font-weight:650;}
.pipeline-arrow {color:#3e98ff;margin-right:8px;}
.sidebar-foot {color:#728ba8;font-size:.74rem;line-height:2.2;}
.sidebar-foot b {color:#dbe8fb;}

/* IndustryOS-inspired visual system */
.stApp {background:radial-gradient(ellipse at 65% -25%,#102654 0%,transparent 55%),#070e1d!important;}
[data-testid="stSidebar"] {background:linear-gradient(180deg,#0a1730,#0b1c3b 80%,#071329)!important;border-right:1px solid #28456a!important;}
[data-testid="stSidebar"]>div:first-child {background:transparent;}
.block-container {max-width:1700px!important;padding:1.35rem 2rem 2rem!important;}
[data-testid="stMetric"] {background:linear-gradient(125deg,#0e2546,#111b38 72%,#151d3b)!important;border:1px solid #2a5286!important;box-shadow:inset 0 1px 0 #ffffff0c,0 8px 30px #00000020!important;border-radius:16px!important;min-height:116px!important;}
[data-testid="stMetricValue"] {font-size:2rem!important;font-weight:800!important;}
[data-testid="stMetricLabel"] p {font-size:.83rem!important;color:#c1d8f7!important;}
[data-testid="stPlotlyChart"] {border:1px solid #2b5081!important;background:linear-gradient(145deg,#0d2243,#0b1731)!important;border-radius:17px!important;box-shadow:0 10px 30px #0002;}
.page-title {font-size:2.35rem!important;letter-spacing:-1.2px!important;}
.pretitle {color:#53caff!important;}
.panel {background:linear-gradient(125deg,#0e2343,#101b35)!important;border-color:#294b78!important;}
.mini-card {background:linear-gradient(100deg,#0d2141,#101c36)!important;border-color:#2b4c78!important;border-radius:13px!important;}
.mini-card:hover {border-color:#4b98ff!important;transition:border-color .2s;}
.section-title {font-size:1.1rem!important;color:#f4f8ff!important;}
[data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) {background:linear-gradient(100deg,#087cc0,#183a8a)!important;box-shadow:0 3px 14px #0054bc33;}
.sidebar-art {margin-top:18px;width:100%;border-radius:14px;overflow:hidden;opacity:.95;}
.header-tags {display:flex;flex-wrap:wrap;gap:7px;margin:5px 0 14px;}
.header-tags span {border:1px solid #2564a0;background:linear-gradient(90deg,#0b2b4e,#182048);color:#d1e8ff;border-radius:30px;padding:6px 11px;font-size:11px;font-weight:650;}
.topline {border-bottom:1px solid #203856;margin-bottom:16px;padding-bottom:12px;}

</style>
""", unsafe_allow_html=True)


def db_query(sql):
    with psycopg2.connect(**DB) as conn:
        return pd.read_sql_query(sql, conn)


@st.cache_data(ttl=90, show_spinner=False)
def load_kpis():
    return db_query("""SELECT machine_id, window_start, window_end, total_measurements,
        avg_temperature, avg_vibration, avg_energy, total_alerts, alert_rate
        FROM public.machine_kpi ORDER BY window_start DESC LIMIT 20000""")


def data():
    try:
        df = load_kpis()
        if df.empty:
            st.info("No KPI data found in TimescaleDB.")
            return None
        df["window_start"] = pd.to_datetime(df["window_start"], utc=True)
        df["window_end"] = pd.to_datetime(df["window_end"], utc=True)
        for col in ["total_measurements", "avg_temperature", "avg_vibration", "avg_energy", "total_alerts", "alert_rate"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        return df
    except (psycopg2.Error, pd.errors.DatabaseError, ValueError) as exc:
        st.warning(f"TimescaleDB is unavailable: {exc}")
        return None


def api_health():
    try:
        r = requests.get(f"{API}/health", timeout=3)
        r.raise_for_status()
        return r.json()
    except (requests.RequestException, ValueError):
        return None


def prom_value(query):
    try:
        r = requests.get(f"{PROM}/api/v1/query", params={"query": query}, timeout=3)
        r.raise_for_status()
        results = r.json().get("data", {}).get("result", [])
        if not results:
            return None
        val = float(results[0]["value"][1])
        return val if math.isfinite(val) else None
    except (requests.RequestException, ValueError, TypeError, IndexError, KeyError):
        return None


def chart(fig, height=290):
    fig.update_layout(
        height=height, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#9fb2cd", family="Inter", size=11),
        title_font=dict(color="#e9f2ff", size=13),
        margin=dict(l=12, r=12, t=47, b=12),
        legend=dict(orientation="h", y=1.13, x=0, title=None, font=dict(size=10)),
        xaxis=dict(gridcolor="#1e2b40", zeroline=False, title=None),
        yaxis=dict(gridcolor="#1e2b40", zeroline=False, title=None),
        colorway=PALETTE, hovermode="x unified"
    )
    return fig


def trend(df, col, period="1D", by_machine=False):
    """Daily average for a readable historical trend."""
    columns = ["window_start", col] + (["machine_id"] if by_machine else [])
    part = df[columns].dropna().copy()
    if part.empty:
        return part
    part["bucket"] = part["window_start"].dt.floor(period)
    group = (["machine_id"] if by_machine else []) + ["bucket"]
    return part.groupby(group, as_index=False)[col].mean().sort_values(group)


def area_chart(df, metric, title, color="#398bff", height=300):
    daily = trend(df, metric)
    if daily.empty:
        st.info("No data available for this chart.")
        return
    # Show actual daily averages with a readable y-axis.
    # Avoid an area chart starting at zero for nearly constant temperatures.
    fig = px.line(daily, x="bucket", y=metric, title=title)
    fig.update_traces(
        line=dict(color=color, width=3, shape="spline", smoothing=0.55),
        mode="lines+markers",
        marker=dict(size=5, color=color),
        hovertemplate="%{x|%d %b %Y}<br>%{y:.2f}<extra></extra>"
    )
    values = daily[metric].dropna()
    if not values.empty:
        low, high = float(values.min()), float(values.max())
        padding = max((high - low) * 0.6, abs(high) * 0.015, 0.1)
        fig.update_yaxes(range=[low - padding, high + padding])
    fig.update_layout(showlegend=False)
    st.plotly_chart(chart(fig, height), use_container_width=True)


def heading(title, subtitle="", eyebrow="INDUSTRIAL INTELLIGENCE"):
    st.markdown(f'<div class="pretitle">{eyebrow}</div><div class="page-title">{title}</div><div class="page-desc">{subtitle}</div>', unsafe_allow_html=True)


def section(title):
    st.markdown(f'<div class="section-title">{title}</div>', unsafe_allow_html=True)


def box(title, desc):
    st.markdown(f'<div class="panel"><div class="panel-title">{title}</div><div class="panel-sub">{desc}</div></div>', unsafe_allow_html=True)


# Sidebar
st.sidebar.markdown('<div class="brandline">▣ <span>Industry</span>Pulse</div><div class="subtle">Industrial Intelligence Platform</div>', unsafe_allow_html=True)
st.sidebar.divider()
page = st.sidebar.radio("Navigation", [
    "▦  Overview", "◉  IoT Monitoring", "▥  Industrial Analytics",
    "◇  Predictive Maintenance", "◈  MLOps & Monitoring",
    "⇄  Data Pipeline", "⬡  Architecture & About"
], label_visibility="collapsed")
st.sidebar.divider()
st.sidebar.markdown('<div class="sidebar-art"><img alt="Industrial factory illustration" style="width:100%;display:block" src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCAyODAgMTY1Ij48ZGVmcz48bGluZWFyR3JhZGllbnQgaWQ9ImciIHgxPSIwIiB4Mj0iMCIgeTE9IjAiIHkyPSIxIj48c3RvcCBzdG9wLWNvbG9yPSIjMTQzYzgzIi8+PHN0b3Agb2Zmc2V0PSIxIiBzdG9wLWNvbG9yPSIjMDYxNTJkIi8+PC9saW5lYXJHcmFkaWVudD48bGluZWFyR3JhZGllbnQgaWQ9ImIiIHgxPSIwIiB4Mj0iMSI+PHN0b3Agc3RvcC1jb2xvcj0iIzI0YzhmZiIvPjxzdG9wIG9mZnNldD0iMSIgc3RvcC1jb2xvcj0iIzI1NjNlYiIvPjwvbGluZWFyR3JhZGllbnQ+PC9kZWZzPjxyZWN0IHdpZHRoPSIyODAiIGhlaWdodD0iMTY1IiBmaWxsPSJ1cmwoI2cpIi8+PHBhdGggZD0iTTAgMTQyUTkwIDExOCAyODAgMTQ1VjE2NUgwWiIgZmlsbD0iIzBiMjg1MSIvPjxnIGZpbGw9IiMxMDM5NjgiIHN0cm9rZT0iIzIzN2JjNiIgc3Ryb2tlLXdpZHRoPSIxIj48cGF0aCBkPSJNMTQgMTM3Vjk0TDU2IDc4Vjk2TDg4IDgxVjEzN1oiLz48cGF0aCBkPSJNOTQgMTM3VjEwNUwxMzYgODhWMTM3WiIvPjxwYXRoIGQ9Ik0xNDIgMTM3Vjc2SDE5OFYxMzdaIi8+PHBhdGggZD0iTTIwNSAxMzdWOTlIMjY4VjEzN1oiLz48L2c+PGcgZmlsbD0iIzBjM2Q3MiIgc3Ryb2tlPSIjMzdhOGVkIiBzdHJva2Utd2lkdGg9IjEiPjxwYXRoIGQ9Ik0yOSA5M1Y0NUg0MVY5MFoiLz48cGF0aCBkPSJNMTYzIDc2VjI1SDE3N1Y3NloiLz48cGF0aCBkPSJNMjIxIDEwMFY1NEgyMzJWMTAwWiIvPjwvZz48ZyBmaWxsPSIjMzZjOWZmIj48cmVjdCB4PSIyNSIgeT0iMTA5IiB3aWR0aD0iNyIgaGVpZ2h0PSI2IiBvcGFjaXR5PSIuOCIvPjxyZWN0IHg9IjI1IiB5PSIxMjMiIHdpZHRoPSI3IiBoZWlnaHQ9IjYiIG9wYWNpdHk9Ii44Ii8+PHJlY3QgeD0iNDAiIHk9IjEwOSIgd2lkdGg9IjciIGhlaWdodD0iNiIgb3BhY2l0eT0iLjgiLz48cmVjdCB4PSI0MCIgeT0iMTIzIiB3aWR0aD0iNyIgaGVpZ2h0PSI2IiBvcGFjaXR5PSIuOCIvPjxyZWN0IHg9IjU1IiB5PSIxMDkiIHdpZHRoPSI3IiBoZWlnaHQ9IjYiIG9wYWNpdHk9Ii44Ii8+PHJlY3QgeD0iNTUiIHk9IjEyMyIgd2lkdGg9IjciIGhlaWdodD0iNiIgb3BhY2l0eT0iLjgiLz48cmVjdCB4PSI3MCIgeT0iMTA5IiB3aWR0aD0iNyIgaGVpZ2h0PSI2IiBvcGFjaXR5PSIuOCIvPjxyZWN0IHg9IjcwIiB5PSIxMjMiIHdpZHRoPSI3IiBoZWlnaHQ9IjYiIG9wYWNpdHk9Ii44Ii8+PHJlY3QgeD0iMTA0IiB5PSIxMDkiIHdpZHRoPSI3IiBoZWlnaHQ9IjYiIG9wYWNpdHk9Ii44Ii8+PHJlY3QgeD0iMTA0IiB5PSIxMjMiIHdpZHRoPSI3IiBoZWlnaHQ9IjYiIG9wYWNpdHk9Ii44Ii8+PHJlY3QgeD0iMTE5IiB5PSIxMDkiIHdpZHRoPSI3IiBoZWlnaHQ9IjYiIG9wYWNpdHk9Ii44Ii8+PHJlY3QgeD0iMTE5IiB5PSIxMjMiIHdpZHRoPSI3IiBoZWlnaHQ9IjYiIG9wYWNpdHk9Ii44Ii8+PHJlY3QgeD0iMTUxIiB5PSIxMDkiIHdpZHRoPSI3IiBoZWlnaHQ9IjYiIG9wYWNpdHk9Ii44Ii8+PHJlY3QgeD0iMTUxIiB5PSIxMjMiIHdpZHRoPSI3IiBoZWlnaHQ9IjYiIG9wYWNpdHk9Ii44Ii8+PHJlY3QgeD0iMTY4IiB5PSIxMDkiIHdpZHRoPSI3IiBoZWlnaHQ9IjYiIG9wYWNpdHk9Ii44Ii8+PHJlY3QgeD0iMTY4IiB5PSIxMjMiIHdpZHRoPSI3IiBoZWlnaHQ9IjYiIG9wYWNpdHk9Ii44Ii8+PHJlY3QgeD0iMTg0IiB5PSIxMDkiIHdpZHRoPSI3IiBoZWlnaHQ9IjYiIG9wYWNpdHk9Ii44Ii8+PHJlY3QgeD0iMTg0IiB5PSIxMjMiIHdpZHRoPSI3IiBoZWlnaHQ9IjYiIG9wYWNpdHk9Ii44Ii8+PHJlY3QgeD0iMjE0IiB5PSIxMDkiIHdpZHRoPSI3IiBoZWlnaHQ9IjYiIG9wYWNpdHk9Ii44Ii8+PHJlY3QgeD0iMjE0IiB5PSIxMjMiIHdpZHRoPSI3IiBoZWlnaHQ9IjYiIG9wYWNpdHk9Ii44Ii8+PHJlY3QgeD0iMjMwIiB5PSIxMDkiIHdpZHRoPSI3IiBoZWlnaHQ9IjYiIG9wYWNpdHk9Ii44Ii8+PHJlY3QgeD0iMjMwIiB5PSIxMjMiIHdpZHRoPSI3IiBoZWlnaHQ9IjYiIG9wYWNpdHk9Ii44Ii8+PHJlY3QgeD0iMjQ2IiB5PSIxMDkiIHdpZHRoPSI3IiBoZWlnaHQ9IjYiIG9wYWNpdHk9Ii44Ii8+PHJlY3QgeD0iMjQ2IiB5PSIxMjMiIHdpZHRoPSI3IiBoZWlnaHQ9IjYiIG9wYWNpdHk9Ii44Ii8+PC9nPjxwYXRoIGQ9Ik0wIDE1MVE3NSAxMjkgMTQyIDE0OVQyODAgMTQ1IiBmaWxsPSJub25lIiBzdHJva2U9InVybCgjYikiIHN0cm9rZS13aWR0aD0iMiIvPjxwYXRoIGQ9Ik0wIDE1OVExMTAgMTQxIDI4MCAxNTkiIGZpbGw9Im5vbmUiIHN0cm9rZT0iIzBlNmRiNyIgc3Ryb2tlLXdpZHRoPSIxIi8+PC9zdmc+"></div>', unsafe_allow_html=True)
health = api_health()
scrape = prom_value('up{job="industrypulse-ml-api"}')
st.sidebar.markdown('<div class="sidebar-foot"><b>SYSTEM STATUS</b><br>' +
    ('🟢 ML API connected' if health else '🔴 ML API unavailable') + '<br>' +
    ('🟢 Prometheus scraping' if scrape == 1 else '🔴 Prometheus unavailable') +
    '<br>◌ Historical IoT dataset<br>◌ Grafana :3001</div>', unsafe_allow_html=True)

st.markdown('<div class="topline"><div><span class="brandline">IndustryPulse</span> <span class="subtle"> / AI Monitoring</span></div><div class="subtle">● LOCAL DEMO &nbsp; · &nbsp; DATA ENGINEERING + MLOPS</div></div>', unsafe_allow_html=True)

if page == "▦  Overview":
    heading("Industrial Intelligence Dashboard", "Explore machine telemetry, historical KPIs, alerts and predictive maintenance in one place.", "WELCOME TO INDUSTRYPULSE")
    st.markdown('<div class="header-tags"><span>⚙ Industrial IoT</span><span>◈ Data Engineering</span><span>◇ Predictive AI</span><span>▣ MLOps</span></div>', unsafe_allow_html=True)
    df = data()
    if df is not None:
        latest = df["window_end"].max()
        st.caption(f"Historical snapshot · Last recorded: {latest:%d %b %Y, %H:%M UTC} · Not a live sensor feed")
        a, b, c, d = st.columns(4)
        a.metric("◉ Simulated Machines", f"{df['machine_id'].nunique():,}")
        b.metric("▦ KPI Records", f"{len(df):,}")
        c.metric("⚠ Recorded Alerts", f"{df['total_alerts'].fillna(0).sum():,.0f}")
        d.metric("↗ Measurements", f"{df['total_measurements'].fillna(0).sum():,.0f}")
        main, side = st.columns([2.1, 1], gap="medium")
        with main:
            section("System Performance")
            area_chart(df, "avg_temperature", "Daily average temperature · all machines", height=310)
            l, r = st.columns(2)
            with l:
                section("Machine Distribution")
                counts = df.groupby("machine_id", as_index=False)["total_measurements"].sum()
                fig = px.bar(counts, x="machine_id", y="total_measurements", color="machine_id", title="Measurements by machine", color_discrete_sequence=PALETTE)
                fig.update_layout(showlegend=False)
                st.plotly_chart(chart(fig, 265), use_container_width=True)
            with r:
                section("Energy Consumption")
                energy = df.groupby("machine_id", as_index=False)["avg_energy"].mean()
                fig = px.bar(energy, x="machine_id", y="avg_energy", color="machine_id", title="Average energy by machine", color_discrete_sequence=PALETTE)
                fig.update_traces(marker_line_width=0, width=0.55)
                fig.update_xaxes(type="category")
                fig.update_layout(showlegend=False)
                st.plotly_chart(chart(fig, 265), use_container_width=True)
        with side:
            section("Recent Alerts")
            recent = df[df["total_alerts"].fillna(0) > 0].sort_values("window_end", ascending=False).head(4)
            if recent.empty:
                box("No recorded alerts", "No alerts found in the stored KPI windows.")
            for _, item in recent.iterrows():
                alert_count = int(item["total_alerts"])
                alert_color = "#ff718a" if alert_count >= 5 else "#ffc45c"
                st.markdown(f'<div class="mini-card" style="border-left:3px solid {alert_color}"><div class="mini-name">⚠ {item["machine_id"]} · {alert_count} recorded alerts</div><div class="mini-info">Historical window · {item["window_end"]:%d %b %Y, %H:%M UTC}</div></div>', unsafe_allow_html=True)
            st.caption("Alert colors indicate count only, not severity.")
            section("Device Status")
            for machine in sorted(df["machine_id"].dropna().unique()):
                part = df[df["machine_id"] == machine]
                st.markdown(f'<div class="mini-card"><div class="mini-name">◉ {machine} <span style="float:right;color:#5bc7b4">Recorded</span></div><div class="mini-info">{len(part):,} KPI windows · historical data</div></div>', unsafe_allow_html=True)
    section("Platform Capabilities")
    a, b, c, d = st.columns(4)
    with a: box("◉ IoT Streaming", "MQTT · Kafka · Spark")
    with b: box("▦ Lakehouse", "Iceberg · MinIO · Medallion")
    with c: box("◇ Predictive AI", "XGBoost · FastAPI")
    with d: box("◈ Observability", "Prometheus · MLflow · CI")

elif page == "◉  IoT Monitoring":
    heading("IoT Monitoring", "Explore historical sensor-derived metrics for five simulated industrial machines.", "INDUSTRIAL TELEMETRY")
    df = data()
    if df is not None:
        st.caption(f"Last recorded KPI window: {df['window_end'].max():%d %b %Y %H:%M UTC} · Historical data")
        machines = sorted(df["machine_id"].dropna().unique().tolist())
        selected = st.multiselect("Select machines", machines, default=machines)
        filtered = df[df["machine_id"].isin(selected)]
        if filtered.empty:
            st.info("Select at least one machine.")
        else:
            a, b, c = st.columns(3)
            a.metric("Average Temperature", f"{filtered['avg_temperature'].mean():.1f}")
            b.metric("Average Vibration", f"{filtered['avg_vibration'].mean():.3f}")
            c.metric("Recorded Alerts", f"{filtered['total_alerts'].sum():,.0f}")
            for left_col, right_col in [("avg_temperature", "avg_vibration"), ("avg_energy", None)]:
                cols = st.columns(2)
                for i, col in enumerate((left_col, right_col)):
                    if col is None:
                        continue
                    with cols[i]:
                        color = "#a28bff" if col == "avg_vibration" else "#398bff"
                        area_chart(filtered, col, col.replace("avg_", "").replace("_", " ").title() + " · daily average", color=color)
                if right_col is None:
                    with cols[1]:
                        alerts = filtered.groupby("machine_id", as_index=False)["total_alerts"].sum()
                        fig = px.bar(alerts, x="machine_id", y="total_alerts", color="machine_id", title="Alerts by machine", color_discrete_sequence=PALETTE)
                        fig.update_layout(showlegend=False)
                        st.plotly_chart(chart(fig), use_container_width=True)

elif page == "▥  Industrial Analytics":
    heading("Industrial Analytics", "Compare machines and explore the Gold KPI layer in TimescaleDB.", "DATA ANALYTICS")
    df = data()
    if df is not None:
        machines = sorted(df["machine_id"].dropna().unique().tolist())
        selected = st.multiselect("Machines", machines, default=machines)
        filtered = df[df["machine_id"].isin(selected)]
        if not filtered.empty:
            a, b, c, d = st.columns(4)
            a.metric("Avg Temperature", f"{filtered['avg_temperature'].mean():.2f}")
            b.metric("Avg Vibration", f"{filtered['avg_vibration'].mean():.3f}")
            c.metric("Avg Energy", f"{filtered['avg_energy'].mean():.2f}")
            d.metric("Total Alerts", f"{filtered['total_alerts'].sum():,.0f}")
            grouped = filtered.groupby("machine_id", as_index=False).agg(avg_temperature=("avg_temperature", "mean"), total_alerts=("total_alerts", "sum"))
            l, r = st.columns(2)
            with l:
                fig = px.bar(grouped, x="machine_id", y="avg_temperature", color="machine_id", title="Temperature by machine", color_discrete_sequence=PALETTE)
                fig.update_layout(showlegend=False)
                st.plotly_chart(chart(fig), use_container_width=True)
            with r:
                fig = px.bar(grouped, x="machine_id", y="total_alerts", color="machine_id", title="Alerts by machine", color_discrete_sequence=PALETTE)
                fig.update_layout(showlegend=False)
                st.plotly_chart(chart(fig), use_container_width=True)
            section("Explore KPI Records")
            st.dataframe(filtered.sort_values("window_start", ascending=False), hide_index=True, use_container_width=True)
            st.download_button("Download CSV", filtered.to_csv(index=False).encode(), "industrypulse_kpis.csv", "text/csv")

elif page == "◇  Predictive Maintenance":
    heading("Predictive Maintenance", "Test the trained XGBoost model through the existing FastAPI prediction service.", "AI-POWERED MAINTENANCE")
    if health and health.get("model_loaded"):
        st.success(f"Model ready · Decision threshold: {float(health.get('threshold', 0)):.0%}")
    else:
        st.warning("The ML API or model is not available.")
    with st.form("prediction_form"):
        l, r = st.columns(2)
        with l:
            air = st.number_input("Air temperature (K)", value=300.0, step=0.1)
            process = st.number_input("Process temperature (K)", value=310.0, step=0.1)
            speed = st.number_input("Rotational speed (rpm)", value=1500, min_value=0)
        with r:
            torque = st.number_input("Torque (Nm)", value=40.0, min_value=0.0, step=0.1)
            wear = st.number_input("Tool wear (min)", value=100, min_value=0)
        submit = st.form_submit_button("Predict Failure Risk", type="primary", use_container_width=True)
    if submit:
        payload = dict(air_temperature=air, process_temperature=process, rotational_speed=speed, torque=torque, tool_wear=wear)
        try:
            response = requests.post(f"{API}/predict", json=payload, timeout=15)
            response.raise_for_status()
            result = response.json()
            prob = float(result["failure_probability"])
            threshold = float(result["threshold"])
            a, b, c = st.columns(3)
            a.metric("Failure Probability", f"{prob:.1%}")
            b.metric("Decision Threshold", f"{threshold:.0%}")
            c.metric("Prediction", result["status"])
            st.progress(max(0.0, min(1.0, prob)))
            if result["status"] == "FAILURE_RISK":
                st.error("Potential failure risk detected by the model.")
            else:
                st.success("The model predicts normal operation for these inputs.")
        except (requests.RequestException, KeyError, ValueError) as exc:
            st.error(f"Prediction request failed: {exc}")
    st.caption("Educational model trained using AI4I 2020. Not an industrial safety system.")

elif page == "◈  MLOps & Monitoring":
    heading("MLOps & Monitoring", "Model serving metrics, data drift and experiment tracking.", "RELIABILITY / OBSERVABILITY")
    queries = [
        ("Predictions", "sum(industrypulse_predictions_total)"),
        ("Validation Errors", "sum(industrypulse_validation_errors_total)"),
        ("Processing Errors", "sum(industrypulse_prediction_errors_total)"),
        ("Latency (seconds)", "sum(rate(industrypulse_prediction_duration_seconds_sum[5m])) / sum(rate(industrypulse_prediction_duration_seconds_count[5m]))"),
    ]
    for col, (name, query) in zip(st.columns(4), queries):
        val = prom_value(query)
        col.metric(name, "N/A" if val is None else (f"{val:.3f}" if "Latency" in name else f"{val:,.0f}"))
    st.caption("Prometheus counters reset when the API restarts; latency requires recent prediction requests.")
    section("Data Drift · Population Stability Index")
    if os.path.isfile(DRIFT):
        try:
            with open(DRIFT, encoding="utf-8") as f:
                report = json.load(f)
            rows = [{"Feature": k.replace("_", " ").title(), "PSI": v.get("psi"), "Status": v.get("status")} for k, v in report.get("features", {}).items()]
            if rows:
                drift = pd.DataFrame(rows)
                l, r = st.columns([1, 1.5])
                with l: st.dataframe(drift, hide_index=True, use_container_width=True)
                with r:
                    fig = px.bar(drift, x="PSI", y="Feature", color="Status", orientation="h", title="Feature drift (saved report)", color_discrete_map={"HIGH_DRIFT": "#f16b7a", "MODERATE_DRIFT": "#f3aa42", "LOW_DRIFT": "#28c8ad"})
                    st.plotly_chart(chart(fig), use_container_width=True)
            else:
                st.info("The drift report contains no feature results.")
            st.caption("Saved comparison between AI4I reference data and a sampled IoT stream. Not live drift monitoring.")
        except (OSError, ValueError, TypeError) as exc:
            st.warning(f"Could not read drift report: {exc}")
    else:
        st.info("Drift report not found. Mount ml/reports into /app/reports.")
    section("Tools & Experiment Tracking")
    a, b, c = st.columns(3)
    with a:
        box("MLflow", "Experiment tracking and model evaluation")
        st.link_button("Open MLflow", MLFLOW, use_container_width=True)
    with b:
        box("Grafana", "Prediction metrics and industrial dashboards")
        st.link_button("Open Grafana", GRAFANA, use_container_width=True)
    with c:
        box("GitHub Actions", "Automated syntax checks and FastAPI tests")
        st.link_button("View CI Workflow", REPO + "/actions", use_container_width=True)

elif page == "⇄  Data Pipeline":
    heading("Data Pipeline", "From sensor events to analytical tables and model predictions.", "STREAMING / BATCH / LAKEHOUSE")
    section("Streaming Architecture")
    st.markdown('<div class="panel">' + ''.join(f'<span class="pipeline-step">{name}</span><span class="pipeline-arrow">→</span>' for name in ["IoT Simulator", "MQTT", "Kafka", "Spark Streaming", "Iceberg / MinIO", "TimescaleDB"]) + '</div>', unsafe_allow_html=True)
    section("Medallion Lakehouse")
    a, b, c = st.columns(3)
    with a: box("Bronze · Raw", "Sensor events, source records and Kafka metadata")
    with b: box("Silver · Clean", "Validated, standardized and enriched measurements")
    with c: box("Gold · Analytics", "Aggregated machine KPIs and alert metrics")
    section("Predictive Maintenance Path")
    st.markdown('<div class="panel">' + ''.join(f'<span class="pipeline-step">{name}</span><span class="pipeline-arrow">→</span>' for name in ["Kafka", "ML Worker", "FastAPI / XGBoost", "TimescaleDB Predictions"]) + '</div>', unsafe_allow_html=True)
    st.caption("Architecture documentation: Kafka and Spark are not required to be running for this dashboard to display historical data.")

else:
    heading("Architecture & About", "The technologies and engineering decisions behind IndustryPulse.", "PROJECT / PORTFOLIO")
    items = [
        ("Data Ingestion", "Python · MQTT · Mosquitto · Kafka"),
        ("Data Processing", "Spark · PySpark · Structured Streaming"),
        ("Lakehouse", "Apache Iceberg · MinIO · Bronze / Silver / Gold"),
        ("Serving", "PostgreSQL · TimescaleDB"),
        ("Analytics", "Grafana · Power BI · SQL"),
        ("Machine Learning", "XGBoost · scikit-learn · FastAPI"),
        ("MLOps", "MLflow · Prometheus · Data Drift · GitHub Actions"),
        ("Orchestration", "Apache Airflow · tested on demand"),
        ("Analytics Engineering", "dbt · work in progress"),
    ]
    l, r = st.columns(2)
    for i, (name, desc) in enumerate(items):
        with (l if i % 2 == 0 else r):
            box(name, desc)
    section("About the Project")
    st.write("I built IndustryPulse to learn how industrial IoT data moves through ingestion, streaming, lakehouse processing, analytical storage and predictive maintenance. The platform is a personal engineering project and is still evolving.")
    st.markdown("**Oumayma Zerrik** · Master's Student in Big Data & IoT")
    st.link_button("Explore GitHub Repository", REPO)

st.markdown('<div class="subtle" style="text-align:center;padding:30px 0 10px">INDUSTRYPULSE · INDUSTRIAL IoT · BIG DATA · PREDICTIVE MAINTENANCE</div>', unsafe_allow_html=True)
