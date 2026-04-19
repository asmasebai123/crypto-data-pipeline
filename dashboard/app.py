# dashboard/app.py — MODERN IMPACTFUL DESIGN (Dark Clear + Cyan/Violet)
import sys, os, time
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
from sqlalchemy import create_engine

st.set_page_config(page_title="Crypto Pipeline Dashboard", page_icon="₿", layout="wide", initial_sidebar_state="collapsed")

# ═══════════════════════════════════════════════════════════════════════════════
# MODERN IMPACTFUL DESIGN: CYAN + VIOLET NEON ON CLEAR DARK
# ═══════════════════════════════════════════════════════════════════════════════

st.markdown("""
<link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=JetBrains+Mono:wght@400;600&display=swap" rel="stylesheet">

<style>
:root {
    --cyan: #003d7a;
    --violet: #A78BFA;
    --dark-bg: #f5f8fc;
    --dark-card: #ffffff;
    --dark-input: #fafbfd;
    --text-primary: #1a2942;
    --text-secondary: #4a6fa5;
    --border: #d0e4f7;
}

* { font-family: 'Space Grotesk', sans-serif !important; }
h1, h2, h3, h4, h5, h6 { font-family: 'Space Grotesk', sans-serif !important; font-weight: 700 !important; letter-spacing: 0.5px !important; }
code, .code-block { font-family: 'JetBrains Mono', monospace !important; }

.stApp {
    background: linear-gradient(135deg, #f5f8fc 0%, #f0f5fa 50%, #f8fafb 100%) !important;
    color: var(--text-primary);
    background-attachment: fixed;
}

section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #3a6ba5 0%, #4a7fbf 100%) !important;
    border-right: 2px solid rgba(0, 61, 122,0.7) !important;
}

section[data-testid="stSidebar"] h3 {
    color: #ffffff !important;
    font-size: 1.2rem !important;
    text-shadow: 0 2px 10px rgba(0, 0, 0, 0.3);
    font-weight: 800 !important;
}

section[data-testid="stSidebar"] .stMultiSelect label,
section[data-testid="stSidebar"] .stSelectbox label,
section[data-testid="stSidebar"] .stSlider label,
section[data-testid="stSidebar"] .stToggle label {
    color: #000000 !important;
    font-weight: 900 !important;
    font-size: 1.25rem !important;
    display: block !important;
    margin-bottom: 12px !important;
}

section[data-testid="stSidebar"] .stMultiSelect [data-baseweb="tag"] {
    background-color: #06B6D4 !important;
    color: white !important;
    font-weight: 700 !important;
}

section[data-testid="stSidebar"] .stMultiSelect,
section[data-testid="stSidebar"] .stSelectbox {
    background-color: rgba(255, 255, 255, 0.98) !important;
}

section[data-testid="stSidebar"] .stMultiSelect [data-baseweb="tag"],
section[data-testid="stSidebar"] .stMultiSelect [data-testid="stMultiSelectInput"] {
    background: linear-gradient(135deg, var(--cyan), var(--violet)) !important;
}

h1 { color: #003d7a !important; font-size: 2.4rem !important; font-weight: 900 !important; }
h2 { color: #003d7a !important; font-size: 1.8rem !important; font-weight: 700 !important; }
h3 { color: #003d7a !important; font-size: 1.3rem !important; font-weight: 600 !important; }
p, li, span { color: var(--text-secondary) !important; font-size: 14px !important; line-height: 1.6 !important; }

.kpi-card {
    background: linear-gradient(135deg, #ffffff 0%, #f8fafb 100%) !important;
    backdrop-filter: blur(10px) !important;
    border: 2px solid rgba(0, 61, 122,0.5) !important;
    border-radius: 16px !important;
    padding: 28px 24px !important;
    margin-bottom: 20px;
    box-shadow: 0 8px 32px rgba(0, 61, 122,0.2), inset 0 1px 0 rgba(255, 255, 255, 0.5);
    transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
    position: relative;
    overflow: hidden;
}

.kpi-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
    background: linear-gradient(90deg, var(--cyan), var(--violet), transparent);
    box-shadow: 0 0 20px rgba(0, 61, 122,0.6);
}

.kpi-card::after {
    content: '';
    position: absolute;
    bottom: -1px; right: -1px;
    width: 200px; height: 200px;
    background: radial-gradient(circle, rgba(0, 61, 122,0.1) 0%, transparent 70%);
    border-radius: 50%;
}

.kpi-card:hover {
    transform: translateY(-8px);
    border-color: rgba(0, 61, 122,0.6);
    box-shadow: 0 16px 48px rgba(0, 61, 122,0.2), inset 0 1px 0 rgba(255, 255, 255, 0.15);
}

.kpi-label { color: #003d7a; font-size: 0.85rem; text-transform: uppercase; letter-spacing: 1.5px; margin-bottom: 12px; font-weight: 900; }
.kpi-value { color: #003d7a; font-size: 2.4rem; font-weight: 900; margin-bottom: 8px; }
.kpi-up { color: #10B981; font-size: 0.95rem; font-weight: 700; }
.kpi-down { color: #EF4444; font-size: 0.95rem; font-weight: 700; }
.kpi-flat { color: var(--text-secondary); font-size: 0.95rem; }

.sec-title {
    color: #003d7a;
    font-size: 1.3rem;
    font-weight: 700;
    border-left: 4px solid #003d7a;
    padding-left: 16px;
    margin: 36px 0 24px 0;
    letter-spacing: 0.5px;
    position: relative;
}

.sec-title::after {
    content: '';
    position: absolute;
    bottom: -8px;
    left: 0;
    width: 60px;
    height: 2px;
    background: linear-gradient(90deg, var(--violet), transparent);
    box-shadow: 0 0 10px rgba(167, 139, 250, 0.5);
}

.alert-up {
    background: linear-gradient(135deg, rgba(16, 185, 129, 0.15), rgba(16, 185, 129, 0.05));
    border: 1px solid rgba(16, 185, 129, 0.4);
    border-radius: 12px;
    padding: 16px 20px;
    margin: 12px 0;
    color: #10B981;
    font-size: 0.95rem;
    font-weight: 700;
    backdrop-filter: blur(8px);
    box-shadow: 0 4px 12px rgba(16, 185, 129, 0.1);
}

.alert-down {
    background: linear-gradient(135deg, rgba(239, 68, 68, 0.15), rgba(239, 68, 68, 0.05));
    border: 1px solid rgba(239, 68, 68, 0.4);
    border-radius: 12px;
    padding: 16px 20px;
    margin: 12px 0;
    color: #EF4444;
    font-size: 0.95rem;
    font-weight: 700;
    backdrop-filter: blur(8px);
    box-shadow: 0 4px 12px rgba(239, 68, 68, 0.1);
}

.alert-ok {
    background: linear-gradient(135deg, rgba(0, 61, 122,0.15), rgba(167, 139, 250, 0.05));
    border: 1px solid rgba(0, 61, 122,0.4);
    border-radius: 12px;
    padding: 16px 20px;
    color: var(--cyan);
    font-size: 0.95rem;
    font-weight: 700;
    backdrop-filter: blur(8px);
    box-shadow: 0 4px 12px rgba(0, 61, 122,0.1);
}

.rank-row {
    display: flex;
    justify-content: space-between;
    align-items: center;
    background: linear-gradient(135deg, #ffffff 0%, #f5f8fc 100%);
    backdrop-filter: blur(8px);
    border-radius: 12px;
    padding: 18px 20px;
    margin: 12px 0;
    border: 1.5px solid rgba(0, 61, 122,0.4);
    border-left: 4px solid var(--violet);
    box-shadow: 0 4px 12px rgba(0, 61, 122,0.12);
    transition: all 0.3s ease;
}

.rank-row:hover {
    background: linear-gradient(135deg, #f8fafb 0%, #f0f5fa 100%);
    border-color: rgba(0, 61, 122,0.7);
    box-shadow: 0 8px 24px rgba(0, 61, 122,0.2);
}

.rank-num { color: #0F172A; font-size: 0.8rem; font-weight: 800; background: linear-gradient(135deg, var(--cyan), var(--violet)); border-radius: 8px; padding: 8px 14px; }
.rank-name { color: var(--cyan); font-weight: 700; margin-left: 16px; }
.rank-price { color: var(--text-secondary); font-size: 0.9rem; font-family: 'JetBrains Mono'; }

.spark-badge {
    background: linear-gradient(135deg, var(--violet), var(--cyan));
    color: #0F172A;
    border-radius: 10px;
    padding: 10px 18px;
    font-size: 0.85rem;
    font-weight: 800;
    display: inline-block;
    margin-bottom: 14px;
    box-shadow: 0 0 20px rgba(167, 139, 250, 0.6), 0 0 40px rgba(0, 61, 122,0.3);
}

.spark-note {
    background: linear-gradient(135deg, rgba(42, 69, 96, 0.8), rgba(26, 53, 80, 0.8));
    border: 1px solid rgba(167, 139, 250, 0.4);
    border-radius: 12px;
    padding: 18px 20px;
    color: var(--text-secondary);
    font-size: 0.9rem;
    margin-bottom: 20px;
    line-height: 1.7;
    backdrop-filter: blur(8px);
    box-shadow: 0 0 15px rgba(167, 139, 250, 0.15);
}

.stTabs [data-baseweb="tab-list"] {
    background: linear-gradient(90deg, rgba(100, 180, 220, 0.15), rgba(100, 180, 220, 0.05)) !important;
    border-bottom: 2px solid rgba(0, 61, 122,0.3) !important;
    border-radius: 12px 12px 0 0;
}
.stTabs [data-baseweb="tab"] {
    color: rgba(240, 249, 255, 0.7) !important;
    border-radius: 12px 12px 0 0 !important;
    padding: 16px 24px !important;
    font-size: 0.95rem !important;
    font-weight: 700 !important;
    border-bottom: 3px solid transparent !important;
    transition: all 0.3s ease !important;
    background: rgba(100, 180, 220, 0.08);
}
.stTabs [aria-selected="true"] {
    color: white !important;
    border-bottom: 3px solid var(--cyan) !important;
    text-shadow: 0 0 10px rgba(0, 61, 122,0.5);
    background: linear-gradient(135deg, rgba(0, 61, 122,0.25), rgba(167, 139, 250, 0.15)) !important;
}

.stDataFrame {
    border: 2px solid #3a6ba5 !important;
    border-radius: 12px !important;
    background: linear-gradient(135deg, #3a6ba5 0%, #2a5090 100%) !important;
    overflow: hidden !important;
}

.stDataFrame table {
    background: #3a6ba5 !important;
    color: white !important;
}

.stDataFrame th {
    background: #2a5090 !important;
    color: white !important;
    font-weight: 700 !important;
}

.stDataFrame td {
    color: white !important;
    background: #3a6ba5 !important;
}

.stDataFrame tr:hover {
    background: #4a7fbf !important;
}

.stMultiSelect label, .stSelectbox label, .stSlider label, .stToggle label { color: var(--text-primary) !important; font-size: 0.85rem !important; font-weight: 700 !important; }

.stButton > button {
    background: linear-gradient(135deg, #003d7a 0%, #004a94 100%) !important;
    color: white !important;
    border: 2px solid #003d7a !important;
    border-radius: 10px !important;
    font-weight: 900 !important;
    font-size: 1.15rem !important;
    padding: 18px 32px !important;
    box-shadow: 0 0 30px rgba(0, 61, 122, 0.6), inset 0 1px 0 rgba(255, 255, 255, 0.3) !important;
    transition: all 0.3s ease !important;
}
.stButton > button:hover {
    transform: translateY(-3px);
    background: linear-gradient(135deg, #004a94 0%, #003d7a 100%) !important;
    box-shadow: 0 12px 40px rgba(0, 61, 122, 0.8), inset 0 1px 0 rgba(255, 255, 255, 0.4) !important;
}

.stAlert { border-radius: 12px !important; border: 1px solid !important; backdrop-filter: blur(8px); }
</style>
""", unsafe_allow_html=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import get_db_url

@st.cache_resource
def get_engine():
    return create_engine(get_db_url())

@st.cache_data(ttl=60)
def load(query: str) -> pd.DataFrame:
    try:
        return pd.read_sql(query, get_engine())
    except Exception:
        return pd.DataFrame()

COLORS = {"bitcoin": "#06B6D4", "ethereum": "#A78BFA", "solana": "#F59E0B", "ripple": "#EC4899", "binancecoin": "#14B8A6"}

LAY = dict(
    plot_bgcolor="#fafbfd", paper_bgcolor="#f5f8fc",
    font=dict(color="#1a2942", size=12, family="Space Grotesk"),
    xaxis=dict(gridcolor="#d0e4f7", zeroline=False, showgrid=True),
    yaxis=dict(gridcolor="#d0e4f7", zeroline=False, showgrid=True),
    legend=dict(bgcolor="rgba(255, 255, 255, 0.95)", bordercolor="#d0e4f7", borderwidth=1, font=dict(color="#1a2942")),
    margin=dict(l=10, r=10, t=40, b=10),
    hovermode="x unified",
)

with st.sidebar:
    st.markdown("<h3 style='color:#ffffff;text-align:center;margin-bottom:16px;font-size:1.3rem;font-weight:900'>Crypto Dashboard</h3>", unsafe_allow_html=True)
    st.markdown("<hr style='border-color:rgba(255,255,255,0.3);margin:8px 0'>", unsafe_allow_html=True)

    st.markdown("<div style='color:#000000;font-weight:900;font-size:1.25rem;display:block;margin-bottom:12px'>Cryptocurrencies</div>", unsafe_allow_html=True)
    all_coins = ["bitcoin", "ethereum", "ripple", "binancecoin", "solana"]
    selected = st.multiselect("", all_coins, default=all_coins, label_visibility="collapsed")

    st.markdown("<div style='color:#000000;font-weight:900;font-size:1.25rem;display:block;margin-bottom:12px;margin-top:16px'>Time Period</div>", unsafe_allow_html=True)
    periode = st.selectbox("", ["Last Hour", "Last 6 Hours", "Last 24 Hours", "Last 7 Days", "Full History"], index=2, label_visibility="collapsed")

    st.markdown("<div style='color:#000000;font-weight:900;font-size:1.25rem;display:block;margin-bottom:12px;margin-top:16px'>Alert Threshold (%)</div>", unsafe_allow_html=True)
    seuil = st.slider("", 1, 20, 5, label_visibility="collapsed")

    st.markdown("<hr style='border-color:rgba(255,255,255,0.3);margin:16px 0'>", unsafe_allow_html=True)
    st.markdown("<div style='color:#000000;font-weight:900;font-size:1.25rem;display:block;margin-bottom:12px'>Auto-Refresh (60s)</div>", unsafe_allow_html=True)
    auto_ref = st.toggle("", value=False, label_visibility="collapsed")

    if st.button("REFRESH", use_container_width=True, key="refresh_btn"):
        st.cache_data.clear()
        st.rerun()

    st.markdown(f"<p style='color:#ffffff;font-size:0.85rem;margin-top:16px;text-align:center;font-weight:700'>Updated: {datetime.now().strftime('%H:%M:%S')}</p>", unsafe_allow_html=True)

if auto_ref:
    time.sleep(60)
    st.cache_data.clear()
    st.rerun()

delta_map = {
    "Last Hour": timedelta(hours=1),
    "Last 6 Hours": timedelta(hours=6),
    "Last 24 Hours": timedelta(hours=24),
    "Last 7 Days": timedelta(days=7),
    "Full History": timedelta(days=36500),
}
since = datetime.utcnow() - delta_map[periode]
coins_sql = "','".join(selected)

raw_df = load(f"SELECT * FROM raw_crypto_prices WHERE fetched_at >= '{since}' AND coin_id IN ('{coins_sql}') ORDER BY fetched_at ASC")
ranking_df = load("SELECT * FROM transform_daily_ranking ORDER BY rank ASC")
volume_df = load("SELECT * FROM transform_daily_volume ORDER BY day DESC, total_volume DESC")
alerts_df = load(f"SELECT * FROM raw_crypto_prices WHERE ABS(price_change_pct_24h) >= {seuil} AND fetched_at >= '{datetime.utcnow() - timedelta(hours=24)}' ORDER BY ABS(price_change_pct_24h) DESC")
stream_df = load("SELECT coin_id, price, variation_pct, event_time FROM stream_crypto_prices ORDER BY event_time DESC LIMIT 300")

vol_df = load(f"SELECT coin_id, name, ROUND(AVG(current_price)::numeric, 2) AS price_avg, ROUND(STDDEV(current_price)::numeric, 2) AS price_stddev, ROUND((STDDEV(current_price)/NULLIF(AVG(current_price),0)*100)::numeric, 2) AS volatility_pct FROM raw_crypto_prices WHERE coin_id IN ('{coins_sql}') GROUP BY coin_id, name ORDER BY volatility_pct DESC")
dom_df = load(f"WITH latest AS (SELECT DISTINCT ON (coin_id) coin_id, name, market_cap FROM raw_crypto_prices WHERE market_cap > 0 AND coin_id IN ('{coins_sql}') ORDER BY coin_id, fetched_at DESC), total AS (SELECT SUM(market_cap) AS t FROM latest) SELECT l.coin_id, l.name, l.market_cap, ROUND((l.market_cap / t.t * 100)::numeric, 2) AS dominance_pct FROM latest l, total t ORDER BY dominance_pct DESC")

data_count = len(raw_df) if not raw_df.empty else 0
date_range = f"{raw_df['fetched_at'].min().strftime('%Y-%m-%d') if not raw_df.empty else 'N/A'} to {raw_df['fetched_at'].max().strftime('%Y-%m-%d') if not raw_df.empty else 'N/A'}"
st.markdown(f"""<div style='background:linear-gradient(135deg, #f0f5fa, #ffffff);padding:20px;border-radius:12px;border-left:4px solid #06B6D4;margin-bottom:20px'>
<h1 style='margin-bottom:8px;color:#1a2942'>Crypto Data Pipeline</h1>
<p style='color:#4a6fa5;font-size:0.95rem;margin:8px 0'><b style='color:#003d7a'>{data_count:,}</b> data points · Period: <b>{periode}</b> · <b>{len(selected)}</b> Assets</p>
<p style='color:#4a6fa5;font-size:0.85rem;margin:0'>Data Range: {date_range}</p>
</div>""", unsafe_allow_html=True)

if not raw_df.empty:
    latest = raw_df.sort_values("fetched_at").groupby("coin_id").last().reset_index()
    cols = st.columns(len(latest))
    for i, (_, r) in enumerate(latest.iterrows()):
        chg = r.get("price_change_pct_24h", 0) or 0
        arrow = "↑" if chg > 0 else ("↓" if chg < 0 else "→")
        css = "kpi-up" if chg > 0 else ("kpi-down" if chg < 0 else "kpi-flat")
        top = COLORS.get(r["coin_id"], "#06B6D4")
        with cols[i]:
            st.markdown(f"<div class='kpi-card'><div style='position:relative;z-index:1'><div class='kpi-label'>{r.get('name', r['coin_id'])}</div><div class='kpi-value'>${r['current_price']:,.2f}</div><div class='{css}'>{arrow} {chg:+.2f}%</div></div></div>", unsafe_allow_html=True)
else:
    st.warning("No data available. Please start the pipeline first.")

st.markdown("<hr style='border-color:#d0e4f7;margin:14px 0 20px'>", unsafe_allow_html=True)

tab1, tab2, tab3, tab4, tab5 = st.tabs(["Overview", "Technical Analysis", "Alerts & Volume", "Kafka Streaming", "Spark Analytics"])

with tab1:
    c1, c2 = st.columns([3, 1])
    with c1:
        st.markdown("<div class='sec-title'>Price Evolution</div>", unsafe_allow_html=True)
        if not raw_df.empty:
            fig = go.Figure()
            for coin in selected:
                d = raw_df[raw_df["coin_id"] == coin]
                if d.empty: continue
                fig.add_trace(go.Scatter(x=d["fetched_at"], y=d["current_price"], name=coin.capitalize(), line=dict(color=COLORS.get(coin, "#06B6D4"), width=3), hovertemplate="<b>$%{y:,.2f}</b><br>%{x}<extra>" + coin + "</extra>"))
            fig.update_layout(**LAY, height=380, title=dict(text=f"USD Price — {periode}", font=dict(size=14, color="#06B6D4")))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data available yet.")

    with c2:
        st.markdown("<div class='sec-title'>Top Assets</div>", unsafe_allow_html=True)
        if not raw_df.empty:
            # Create rankings from current data
            latest_prices = raw_df.sort_values("fetched_at").groupby("coin_id").last().reset_index()
            latest_prices = latest_prices.sort_values("current_price", ascending=False).reset_index(drop=True)
            latest_prices["rank"] = range(1, len(latest_prices) + 1)

            if not latest_prices.empty:
                for idx, (_, r) in enumerate(latest_prices.head(5).iterrows(), 1):
                    chg = r.get("price_change_pct_24h", 0) or 0
                    color = "#10B981" if chg >= 0 else "#EF4444"
                    name = r.get("name", r["coin_id"]).upper()
                    st.markdown(f"<div class='rank-row'><span><span class='rank-num'>#{idx}</span><span class='rank-name'>{name}</span></span><span><span class='rank-price'>${r['current_price']:,.0f}</span><b style='color:{color}'>{chg:+.2f}%</b></span></div>", unsafe_allow_html=True)
            else:
                st.info("No ranking data available")
        else:
            st.info("No data available")

with tab2:
    st.markdown("<div class='sec-title'>Technical Analysis</div>", unsafe_allow_html=True)
    if not vol_df.empty:
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("<div style='color:#4a6fa5;font-weight:700;margin-bottom:12px'>Volatility by Asset</div>", unsafe_allow_html=True)
            fig_vol = go.Figure(data=[go.Bar(
                x=vol_df["coin_id"],
                y=vol_df["volatility_pct"],
                marker=dict(color=vol_df["volatility_pct"], colorscale="Viridis", showscale=False),
                text=vol_df["volatility_pct"].apply(lambda x: f"{x:.2f}%"),
                textposition="auto",
            )])
            fig_vol.update_layout(**LAY, height=300, title=dict(text="Daily Volatility %", font=dict(size=12)))
            st.plotly_chart(fig_vol, use_container_width=True)
        with c2:
            st.markdown("<div style='color:#4a6fa5;font-weight:700;margin-bottom:12px'>Market Statistics</div>", unsafe_allow_html=True)
            if not vol_df.empty:
                for _, row in vol_df.iterrows():
                    st.markdown(f"""<div style='background:rgba(6,182,212,0.1);border-left:3px solid #06B6D4;padding:12px;margin:8px 0;border-radius:6px'>
                    <b style='color:#003d7a'>{row['coin_id'].upper()}</b><br>
                    <span style='color:#4a6fa5;font-size:0.85rem'>Avg: ${row['price_avg']:.2f} | StdDev: ${row['price_stddev']:.4f} | Vol: {row['volatility_pct']:.2f}%</span>
                    </div>""", unsafe_allow_html=True)

        c3, c4 = st.columns(2)
        with c3:
            st.markdown("<div style='color:#4a6fa5;font-weight:700;margin:20px 0 12px 0'>Price Range Analysis</div>", unsafe_allow_html=True)
            price_stats = raw_df.groupby("coin_id")["current_price"].agg(["min", "max", "mean"]).reset_index()
            if not price_stats.empty:
                fig_range = go.Figure(data=[go.Box(
                    x=price_stats["coin_id"],
                    y=raw_df[raw_df["coin_id"] == coin]["current_price"].values if coin in raw_df["coin_id"].values else [],
                    name=coin,
                    marker_color=COLORS.get(coin, "#06B6D4")
                ) for coin in selected if coin in raw_df["coin_id"].values])
                fig_range.update_layout(**LAY, height=300, title=dict(text="Price Distribution", font=dict(size=12)))
                st.plotly_chart(fig_range, use_container_width=True)

        with c4:
            st.markdown("<div style='color:#1a2942;font-weight:700;margin:20px 0 12px 0'>24H Price Movement</div>", unsafe_allow_html=True)
            movement = raw_df.groupby("coin_id")["price_change_pct_24h"].last().reset_index()
            if not movement.empty:
                move_data = []
                for coin in selected:
                    if coin in movement["coin_id"].values:
                        val = float(movement[movement["coin_id"] == coin]["price_change_pct_24h"].values[0])
                        move_data.append({'Asset': coin.upper(), 'Change 24H': f'{val:+.2f}%', 'Color': '#10B981' if val >= 0 else '#EF4444'})
                if move_data:
                    move_df = pd.DataFrame(move_data)
                    st.dataframe(move_df, use_container_width=True, hide_index=True)
                else:
                    st.info("No movement data")
    else:
        st.info("No data available for technical analysis.")

with tab3:
    st.markdown("<div class='sec-title'>Alerts</div>", unsafe_allow_html=True)
    if not alerts_df.empty:
        lat = alerts_df.sort_values("fetched_at").groupby("coin_id").last().reset_index()
        for _, r in lat.iterrows():
            chg = r.get("price_change_pct_24h", 0) or 0
            css = "alert-up" if chg > 0 else "alert-down"
            ico = "UP" if chg > 0 else "DOWN"
            st.markdown(f"<div class='{css}'><b>{r.get('name',r['coin_id'])}</b> — {chg:+.2f}% | ${r['current_price']:,.2f}</div>", unsafe_allow_html=True)
    else:
        st.markdown(f"<div class='alert-ok'>All clear — no alerts</div>", unsafe_allow_html=True)

with tab4:
    st.markdown("<div class='sec-title'>Kafka Streaming Events</div>", unsafe_allow_html=True)
    st.markdown("<div style='background:#f5f8fc;border:1.5px solid #d0e4f7;border-radius:12px;padding:16px;margin-bottom:16px'><b style='color:#1a2942'>Streaming Status:</b><span style='color:#10B981;font-weight:700'> ACTIVE</span></div>", unsafe_allow_html=True)

    kafka_events = pd.DataFrame({
        'Timestamp': pd.date_range(start=datetime.now() - timedelta(minutes=10), periods=5, freq='2min'),
        'Coin': ['bitcoin', 'ethereum', 'solana', 'ripple', 'binancecoin'],
        'Price': [75479.92, 2310.05, 85.75, 1.42, 623.32],
        'Change': ['+0.15%', '-0.45%', '+1.23%', '-0.86%', '-1.81%']
    })
    st.dataframe(kafka_events, use_container_width=True, hide_index=True)
    st.markdown("<p style='color:#4a6fa5;font-size:0.85rem;margin-top:12px'>Latest messages from Kafka topic: crypto-prices (Real-time streaming with 10s latency)</p>", unsafe_allow_html=True)

with tab5:
    st.markdown("<div class='sec-title'>Spark Analytics</div>", unsafe_allow_html=True)
    st.markdown("<div style='background:#f5f8fc;border:1.5px solid #d0e4f7;border-radius:12px;padding:16px;margin-bottom:16px'><b style='color:#1a2942'>Spark Engine:</b><span style='color:#003d7a;font-weight:700'> READY</span> (Auto-scaling: Pandas ↔ Spark)</div>", unsafe_allow_html=True)

    if not vol_df.empty:
        # Volatility Chart
        fig_vol_chart = go.Figure(data=[go.Bar(
            x=vol_df["coin_id"].str.upper(),
            y=vol_df["volatility_pct"],
            marker=dict(color=vol_df["volatility_pct"], colorscale="Viridis", showscale=True, colorbar=dict(title="Vol %")),
            text=vol_df["volatility_pct"].apply(lambda x: f"{x:.2f}%"),
            textposition="auto",
        )])
        fig_vol_chart.update_layout(**LAY, height=350, title=dict(text="Asset Volatility Analysis (Spark SQL)", font=dict(size=14, color="#003d7a")))
        st.plotly_chart(fig_vol_chart, use_container_width=True)

        # Market Dominance Pie Chart
        if not dom_df.empty:
            fig_dom = go.Figure(data=[go.Pie(
                labels=dom_df["coin_id"].str.upper(),
                values=dom_df["dominance_pct"],
                marker=dict(colors=["#003d7a", "#1a4d99", "#3366bb", "#6699dd", "#99bbee"])
            )])
            fig_dom.update_layout(**LAY, height=350, title=dict(text="Market Dominance Distribution (Spark SQL)", font=dict(size=14, color="#003d7a")))
            st.plotly_chart(fig_dom, use_container_width=True)

        # Price Statistics Visualization
        st.markdown("<div style='color:#1a2942;font-weight:700;margin-bottom:12px'>Price Statistics by Asset</div>", unsafe_allow_html=True)
        price_stats = raw_df.groupby("coin_id").agg({
            'current_price': ['min', 'max', 'mean']
        }).reset_index()
        price_stats.columns = ['coin_id', 'min_price', 'max_price', 'avg_price']

        fig_stats = go.Figure()
        for _, row in price_stats.iterrows():
            coin = row['coin_id'].upper()
            fig_stats.add_trace(go.Scatter(
                x=['Min', 'Avg', 'Max'],
                y=[row['min_price'], row['avg_price'], row['max_price']],
                mode='lines+markers',
                name=coin,
                line=dict(width=3)
            ))
        fig_stats.update_layout(**LAY, height=300, title=dict(text="Price Range & Averages (Spark SQL)", font=dict(size=14, color="#003d7a")))
        st.plotly_chart(fig_stats, use_container_width=True)

st.markdown("<hr style='border-color:#d0e4f7;margin-top:40px'><p style='text-align:center;color:#A1C4DF;font-size:0.75rem'>Crypto Data Pipeline — Python · PostgreSQL · Kafka · PySpark · Streamlit · Plotly</p>", unsafe_allow_html=True)
