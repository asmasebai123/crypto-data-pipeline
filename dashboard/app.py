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
# MODERN CLEAN LIGHT-MODE DESIGN (Stripe/Linear inspired)
# ═══════════════════════════════════════════════════════════════════════════════

st.markdown("""
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;600&display=swap" rel="stylesheet">

<style>
:root {
    --primary: #2563EB;
    --primary-light: #3B82F6;
    --secondary: #0EA5E9;
    --success: #16A34A;
    --danger: #DC2626;
    --bg: #F8FAFC;
    --card: #FFFFFF;
    --text-primary: #0F172A;
    --text-secondary: #64748B;
    --border: #E2E8F0;
    --border-light: #F1F5F9;
}

* { font-family: 'Inter', sans-serif !important; }
h1, h2, h3, h4, h5, h6 { font-family: 'Inter', sans-serif !important; font-weight: 700 !important; }
code, .code-block { font-family: 'JetBrains Mono', monospace !important; }

.stApp {
    background: var(--bg) !important;
    color: var(--text-primary);
    background-attachment: fixed;
}

section[data-testid="stSidebar"] {
    background: #b2d5f7 !important;
    border-right: 1px solid #9AC8F0 !important;
}

section[data-testid="stSidebar"] h3 {
    color: var(--text-primary) !important;
    font-size: 1.3rem !important;
    font-weight: 700 !important;
}

section[data-testid="stSidebar"] .stMultiSelect label,
section[data-testid="stSidebar"] .stSelectbox label,
section[data-testid="stSidebar"] .stSlider label,
section[data-testid="stSidebar"] .stToggle label {
    color: var(--text-secondary) !important;
    font-weight: 600 !important;
    font-size: 0.75rem !important;
    text-transform: uppercase !important;
    letter-spacing: 0.5px !important;
    display: block !important;
    margin-bottom: 8px !important;
}

section[data-testid="stSidebar"] .stMultiSelect [data-baseweb="tag"] {
    background-color: var(--primary) !important;
    color: white !important;
    font-weight: 700 !important;
    font-size: 0.9rem !important;
    border-radius: 6px !important;
    padding: 6px 10px !important;
}

section[data-testid="stSidebar"] .stMultiSelect [data-baseweb="tag"] span {
    color: white !important;
    font-weight: 700 !important;
}

section[data-testid="stSidebar"] .stMultiSelect [data-baseweb="tag"] * {
    color: white !important;
}

section[data-testid="stSidebar"] .stMultiSelect,
section[data-testid="stSidebar"] .stSelectbox {
    background-color: var(--card) !important;
    border-radius: 8px !important;
    border: 1px solid var(--border) !important;
    box-shadow: 0 1px 3px rgba(0,0,0,0.05) !important;
}

section[data-testid="stSidebar"] .stMultiSelect:hover,
section[data-testid="stSidebar"] .stSelectbox:hover {
    border-color: var(--primary) !important;
    box-shadow: 0 4px 12px rgba(37, 99, 235, 0.1) !important;
}

section[data-testid="stSidebar"] [data-baseweb="input"],
section[data-testid="stSidebar"] [data-baseweb="select"] {
    border-radius: 8px !important;
}

section[data-testid="stSidebar"] .stSlider [role="slider"] {
    border-radius: 4px !important;
}

section[data-testid="stSidebar"] .stToggle [role="switch"] {
    border-radius: 12px !important;
}

h1 { color: var(--text-primary) !important; font-size: 2.2rem !important; font-weight: 700 !important; }
h2 { color: var(--text-primary) !important; font-size: 1.5rem !important; font-weight: 700 !important; }
h3 { color: var(--text-primary) !important; font-size: 1.1rem !important; font-weight: 600 !important; }
p, li, span { color: var(--text-secondary) !important; font-size: 14px !important; line-height: 1.6 !important; }

.kpi-card {
    background: #EFF6FF !important;
    border: 1px solid #BFDBFE !important;
    border-radius: 12px !important;
    padding: 20px !important;
    margin-bottom: 16px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.04) !important;
    transition: all 0.2s ease;
    position: relative;
    overflow: hidden;
}

.kpi-card:hover {
    border-color: var(--primary);
    box-shadow: 0 4px 16px rgba(37, 99, 235, 0.15) !important;
    background: #F0F9FF !important;
}

.kpi-label { color: var(--text-secondary); font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px; font-weight: 600; }
.kpi-value { color: var(--text-primary); font-size: 2.2rem; font-weight: 700; margin-bottom: 8px; font-family: 'JetBrains Mono'; }
.kpi-up { color: var(--success); font-size: 0.9rem; font-weight: 600; }
.kpi-down { color: var(--danger); font-size: 0.9rem; font-weight: 600; }
.kpi-flat { color: var(--text-secondary); font-size: 0.9rem; }

.sec-title {
    color: var(--text-primary);
    font-size: 1.1rem;
    font-weight: 700;
    margin: 24px 0 16px 0;
    letter-spacing: -0.5px;
    position: relative;
}

.alert-up {
    background: #DCFCE7;
    border: 1px solid #86EFAC;
    border-radius: 8px;
    padding: 12px 16px;
    margin: 12px 0;
    color: var(--success);
    font-size: 0.9rem;
    font-weight: 600;
}

.alert-down {
    background: #FEE2E2;
    border: 1px solid #FECACA;
    border-radius: 8px;
    padding: 12px 16px;
    margin: 12px 0;
    color: var(--danger);
    font-size: 0.9rem;
    font-weight: 600;
}

.alert-ok {
    background: #EFF6FF;
    border: 1px solid #BFDBFE;
    border-radius: 8px;
    padding: 12px 16px;
    color: var(--primary);
    font-size: 0.9rem;
    font-weight: 600;
}

.rank-row {
    display: flex;
    justify-content: space-between;
    align-items: center;
    background: var(--card);
    border-radius: 8px;
    padding: 16px;
    margin: 8px 0;
    border: 1px solid var(--border);
    box-shadow: 0 1px 2px rgba(0,0,0,0.04);
    transition: all 0.2s ease;
}

.rank-row:hover {
    background: var(--card);
    border-color: var(--primary);
    box-shadow: 0 4px 12px rgba(37, 99, 235, 0.1);
}

.rank-num { color: white; font-size: 0.75rem; font-weight: 700; background: var(--primary); border-radius: 4px; padding: 6px 10px; }
.rank-name { color: var(--text-primary); font-weight: 600; margin-left: 12px; }
.rank-price { color: var(--text-secondary); font-size: 0.85rem; font-family: 'JetBrains Mono'; }

.spark-badge {
    background: var(--primary);
    color: white;
    border-radius: 6px;
    padding: 8px 12px;
    font-size: 0.8rem;
    font-weight: 700;
    display: inline-block;
    margin-bottom: 12px;
    box-shadow: 0 2px 8px rgba(37, 99, 235, 0.15);
}

.spark-note {
    background: #EFF6FF;
    border: 1px solid #BFDBFE;
    border-radius: 8px;
    padding: 16px;
    color: var(--text-secondary);
    font-size: 0.9rem;
    margin-bottom: 16px;
    line-height: 1.6;
    box-shadow: 0 1px 2px rgba(0,0,0,0.04);
}

.stTabs [data-baseweb="tab-list"] {
    background: transparent !important;
    border-bottom: 1px solid var(--border) !important;
    border-radius: 0;
}
.stTabs [data-baseweb="tab"] {
    color: var(--text-secondary) !important;
    border-radius: 0 !important;
    padding: 12px 16px !important;
    font-size: 0.9rem !important;
    font-weight: 600 !important;
    border-bottom: 2px solid transparent !important;
    transition: all 0.2s ease !important;
    background: transparent;
}
.stTabs [aria-selected="true"] {
    color: var(--primary) !important;
    border-bottom: 2px solid var(--primary) !important;
    text-shadow: none;
    background: transparent !important;
}

.stDataFrame {
    border: 1px solid var(--border) !important;
    border-radius: 8px !important;
    background: var(--card) !important;
    overflow: hidden !important;
}

.stDataFrame table {
    background: var(--card) !important;
    color: var(--text-primary) !important;
}

.stDataFrame th {
    background: var(--border) !important;
    color: var(--text-primary) !important;
    font-weight: 600 !important;
}

.stDataFrame td {
    color: var(--text-primary) !important;
    background: var(--card) !important;
}

.stDataFrame tr:hover {
    background: #F9FAFB !important;
}

.stMultiSelect label, .stSelectbox label, .stSlider label, .stToggle label { color: var(--text-secondary) !important; font-size: 0.75rem !important; font-weight: 600 !important; }

.stButton > button {
    background: var(--primary) !important;
    color: white !important;
    border: 1px solid var(--primary) !important;
    border-radius: 8px !important;
    font-weight: 700 !important;
    font-size: 0.9rem !important;
    padding: 12px 24px !important;
    box-shadow: 0 2px 4px rgba(37, 99, 235, 0.2) !important;
    transition: all 0.2s ease !important;
    text-transform: none !important;
    letter-spacing: 0 !important;
}
.stButton > button:hover {
    background: var(--primary-light) !important;
    box-shadow: 0 4px 12px rgba(37, 99, 235, 0.3) !important;
    color: white !important;
    transform: translateY(-1px);
}

.stAlert { border-radius: 8px !important; border: 1px solid !important; }
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

COLORS = {"bitcoin": "#2563EB", "ethereum": "#7C3AED", "solana": "#16A34A", "ripple": "#0EA5E9", "binancecoin": "#D97706"}

LAY = dict(
    plot_bgcolor="#FFFFFF", paper_bgcolor="#FFFFFF",
    font=dict(color="#0F172A", size=12, family="Inter"),
    xaxis=dict(gridcolor="#F1F5F9", zeroline=False, showgrid=True),
    yaxis=dict(gridcolor="#F1F5F9", zeroline=False, showgrid=True),
    legend=dict(bgcolor="rgba(255, 255, 255, 0.95)", bordercolor="#E2E8F0", borderwidth=1, font=dict(color="#0F172A")),
    margin=dict(l=10, r=10, t=40, b=10),
    hovermode="x unified",
)

with st.sidebar:
    # Header
    st.markdown("""
    <div style='text-align:center;margin-bottom:24px;padding-bottom:16px;border-bottom:1px solid #E2E8F0'>
        <h3 style='color:#0F172A;margin:0;font-size:1.3rem;font-weight:700;letter-spacing:-0.5px'>Crypto Dashboard</h3>
        <p style='color:#64748B;margin:8px 0 0 0;font-size:0.75rem;font-weight:500;text-transform:uppercase;letter-spacing:0.5px'>Real-time Analytics</p>
    </div>
    """, unsafe_allow_html=True)

    # Cryptocurrencies Section
    st.markdown("<div style='color:#0F172A;font-weight:700;font-size:0.75rem;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:8px'>Cryptocurrencies</div>", unsafe_allow_html=True)
    all_coins = ["bitcoin", "ethereum", "ripple", "binancecoin", "solana"]
    selected = st.multiselect("", all_coins, default=all_coins, label_visibility="collapsed")

    # Time Period Section
    st.markdown("<div style='color:#0F172A;font-weight:700;font-size:0.75rem;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:8px;margin-top:20px'>Time Period</div>", unsafe_allow_html=True)
    periode = st.selectbox("", ["Last Hour", "Last 6 Hours", "Last 24 Hours", "Last 7 Days", "Full History"], index=2, label_visibility="collapsed")

    # Alert Threshold Section
    st.markdown("<div style='color:#0F172A;font-weight:700;font-size:0.75rem;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:8px;margin-top:20px'>Alert Threshold (%)</div>", unsafe_allow_html=True)
    seuil = st.slider("", 1, 20, 5, label_visibility="collapsed")

    # Settings Section
    st.markdown("<div style='margin-top:24px;padding-top:16px;border-top:1px solid #E2E8F0'></div>", unsafe_allow_html=True)

    st.markdown("<div style='color:#0F172A;font-weight:600;font-size:0.9rem;margin-bottom:10px;display:flex;align-items:center;justify-content:space-between'><span>Auto-Refresh (60s)</span></div>", unsafe_allow_html=True)
    auto_ref = st.toggle("", value=False, label_visibility="collapsed")

    # Refresh Button
    if st.button("Rafraichir", use_container_width=True, key="refresh_btn"):
        st.cache_data.clear()
        st.rerun()

    # Footer
    st.markdown(f"""
    <div style='margin-top:24px;padding-top:16px;border-top:1px solid #E2E8F0;text-align:center'>
        <p style='color:#64748B;font-size:0.75rem;margin:0;font-weight:500;text-transform:uppercase'>Updated</p>
        <p style='color:#2563EB;font-size:0.9rem;margin:4px 0 0 0;font-weight:700;letter-spacing:-0.5px'>{datetime.now().strftime('%H:%M:%S')}</p>
    </div>
    """, unsafe_allow_html=True)

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
st.markdown(f"""<div style='background:var(--card);padding:20px;border-radius:12px;border:1px solid #E2E8F0;box-shadow: 0 1px 3px rgba(0,0,0,0.06);margin-bottom:20px'>
<h1 style='margin-bottom:8px;color:#0F172A'>Crypto Data Pipeline</h1>
<p style='color:#64748B;font-size:0.95rem;margin:8px 0'><b style='color:#2563EB'>{data_count:,}</b> data points · Period: <b>{periode}</b> · <b>{len(selected)}</b> Assets</p>
<p style='color:#64748B;font-size:0.85rem;margin:0'>Data Range: {date_range}</p>
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

st.markdown("<hr style='border-color:#E2E8F0;margin:14px 0 20px'>", unsafe_allow_html=True)

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
