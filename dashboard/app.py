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
    --cyan: #06B6D4;
    --violet: #A78BFA;
    --dark-bg: #0F172A;
    --dark-card: #1a2942;
    --dark-input: #1F2937;
    --text-primary: #F0F9FF;
    --text-secondary: #A1C4DF;
    --border: #1e3a5f;
}

* { font-family: 'Space Grotesk', sans-serif !important; }
h1, h2, h3, h4, h5, h6 { font-family: 'Space Grotesk', sans-serif !important; font-weight: 700 !important; letter-spacing: 0.5px !important; }
code, .code-block { font-family: 'JetBrains Mono', monospace !important; }

.stApp {
    background: linear-gradient(135deg, #0F172A 0%, #1a2942 50%, #162548 100%) !important;
    color: var(--text-primary);
    background-attachment: fixed;
}

section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0F172A 0%, #1a2942 100%) !important;
    border-right: 1px solid var(--border) !important;
}

h1 { color: var(--cyan) !important; font-size: 2.4rem !important; font-weight: 800 !important; text-shadow: 0 0 20px rgba(6, 182, 212, 0.5); }
h2 { color: var(--cyan) !important; font-size: 1.8rem !important; font-weight: 700 !important; }
h3 { color: var(--violet) !important; font-size: 1.3rem !important; font-weight: 600 !important; }
p, li, span, label { color: var(--text-secondary) !important; font-size: 14px !important; line-height: 1.6 !important; }

.kpi-card {
    background: linear-gradient(135deg, rgba(15, 23, 42, 0.8), rgba(26, 41, 66, 0.6)) !important;
    backdrop-filter: blur(10px) !important;
    border: 1.5px solid rgba(6, 182, 212, 0.3) !important;
    border-radius: 16px !important;
    padding: 28px 24px !important;
    margin-bottom: 20px;
    box-shadow: 0 8px 32px rgba(6, 182, 212, 0.1), inset 0 1px 0 rgba(255, 255, 255, 0.1);
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
    box-shadow: 0 0 20px rgba(6, 182, 212, 0.6);
}

.kpi-card::after {
    content: '';
    position: absolute;
    bottom: -1px; right: -1px;
    width: 200px; height: 200px;
    background: radial-gradient(circle, rgba(6, 182, 212, 0.1) 0%, transparent 70%);
    border-radius: 50%;
}

.kpi-card:hover {
    transform: translateY(-8px);
    border-color: rgba(6, 182, 212, 0.6);
    box-shadow: 0 16px 48px rgba(6, 182, 212, 0.2), inset 0 1px 0 rgba(255, 255, 255, 0.15);
}

.kpi-label { color: var(--text-secondary); font-size: 0.75rem; text-transform: uppercase; letter-spacing: 1.5px; margin-bottom: 12px; font-weight: 700; }
.kpi-value { color: var(--cyan); font-size: 2.2rem; font-weight: 800; margin-bottom: 8px; text-shadow: 0 0 10px rgba(6, 182, 212, 0.5); }
.kpi-up { color: #10B981; font-size: 0.95rem; font-weight: 700; }
.kpi-down { color: #EF4444; font-size: 0.95rem; font-weight: 700; }
.kpi-flat { color: var(--text-secondary); font-size: 0.95rem; }

.sec-title {
    color: var(--cyan);
    font-size: 1.3rem;
    font-weight: 700;
    border-left: 4px solid var(--violet);
    padding-left: 16px;
    margin: 36px 0 24px 0;
    letter-spacing: 0.5px;
    position: relative;
    text-shadow: 0 0 15px rgba(6, 182, 212, 0.3);
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
    background: linear-gradient(135deg, rgba(6, 182, 212, 0.15), rgba(167, 139, 250, 0.05));
    border: 1px solid rgba(6, 182, 212, 0.4);
    border-radius: 12px;
    padding: 16px 20px;
    color: var(--cyan);
    font-size: 0.95rem;
    font-weight: 700;
    backdrop-filter: blur(8px);
    box-shadow: 0 4px 12px rgba(6, 182, 212, 0.1);
}

.rank-row {
    display: flex;
    justify-content: space-between;
    align-items: center;
    background: linear-gradient(135deg, rgba(26, 41, 66, 0.6), rgba(15, 23, 42, 0.6));
    backdrop-filter: blur(8px);
    border-radius: 12px;
    padding: 18px 20px;
    margin: 12px 0;
    border: 1px solid rgba(6, 182, 212, 0.2);
    border-left: 4px solid var(--violet);
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.2);
    transition: all 0.3s ease;
}

.rank-row:hover {
    background: linear-gradient(135deg, rgba(26, 41, 66, 0.8), rgba(15, 23, 42, 0.8));
    border-color: rgba(6, 182, 212, 0.6);
    box-shadow: 0 8px 24px rgba(6, 182, 212, 0.2);
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
    box-shadow: 0 0 20px rgba(167, 139, 250, 0.6), 0 0 40px rgba(6, 182, 212, 0.3);
}

.spark-note {
    background: linear-gradient(135deg, rgba(26, 41, 66, 0.7), rgba(15, 23, 42, 0.7));
    border: 1px solid rgba(167, 139, 250, 0.3);
    border-radius: 12px;
    padding: 18px 20px;
    color: var(--text-secondary);
    font-size: 0.9rem;
    margin-bottom: 20px;
    line-height: 1.7;
    backdrop-filter: blur(8px);
    box-shadow: 0 0 15px rgba(167, 139, 250, 0.1);
}

.stTabs [data-baseweb="tab-list"] { background: transparent !important; border-bottom: 2px solid var(--border) !important; }
.stTabs [data-baseweb="tab"] { color: var(--text-secondary) !important; border-radius: 0 !important; padding: 16px 24px !important; font-size: 0.95rem !important; font-weight: 700 !important; border-bottom: 3px solid transparent !important; transition: all 0.3s ease !important; }
.stTabs [aria-selected="true"] { color: var(--cyan) !important; border-bottom: 3px solid var(--cyan) !important; text-shadow: 0 0 10px rgba(6, 182, 212, 0.5); }

.stDataFrame { border: 1px solid var(--border) !important; border-radius: 12px !important; background: rgba(26, 41, 66, 0.4) !important; overflow: hidden !important; }

.stMultiSelect label, .stSelectbox label, .stSlider label, .stToggle label { color: var(--text-primary) !important; font-size: 0.85rem !important; font-weight: 700 !important; }

.stButton > button { background: linear-gradient(135deg, var(--cyan), var(--violet)) !important; color: #0F172A !important; border: none !important; border-radius: 10px !important; font-weight: 800 !important; padding: 14px 28px !important; box-shadow: 0 0 20px rgba(6, 182, 212, 0.4), inset 0 1px 0 rgba(255, 255, 255, 0.2) !important; transition: all 0.3s ease !important; }
.stButton > button:hover { transform: translateY(-2px); box-shadow: 0 8px 28px rgba(6, 182, 212, 0.6), inset 0 1px 0 rgba(255, 255, 255, 0.3) !important; }

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
    plot_bgcolor="#1a2942", paper_bgcolor="#0F172A",
    font=dict(color="#F0F9FF", size=12, family="Space Grotesk"),
    xaxis=dict(gridcolor="#1e3a5f", zeroline=False, showgrid=True),
    yaxis=dict(gridcolor="#1e3a5f", zeroline=False, showgrid=True),
    legend=dict(bgcolor="rgba(15, 23, 42, 0.9)", bordercolor="#1e3a5f", borderwidth=1, font=dict(color="#F0F9FF")),
    margin=dict(l=10, r=10, t=40, b=10),
    hovermode="x unified",
)

with st.sidebar:
    st.markdown("### ₿ Crypto Dashboard")
    st.markdown("<hr style='border-color:#1e3a5f;margin:8px 0'>", unsafe_allow_html=True)

    all_coins = ["bitcoin", "ethereum", "ripple", "binancecoin", "solana"]
    selected = st.multiselect("Cryptomonnaies", all_coins, default=all_coins)

    periode = st.selectbox("Période", ["Dernière heure", "6 dernières heures", "24 dernières heures", "7 derniers jours", "📊 Historique complet"], index=2)

    seuil = st.slider("Seuil alerte (%)", 1, 20, 5)

    st.markdown("<hr style='border-color:#1e3a5f;margin:8px 0'>", unsafe_allow_html=True)
    auto_ref = st.toggle("Auto-refresh 60s", value=False)
    if st.button("🔄 Rafraîchir", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.markdown(f"<p style='color:#A1C4DF;font-size:0.73rem;margin-top:6px'>Mis à jour : {datetime.now().strftime('%H:%M:%S')}</p>", unsafe_allow_html=True)

if auto_ref:
    time.sleep(60)
    st.cache_data.clear()
    st.rerun()

delta_map = {
    "Dernière heure": timedelta(hours=1),
    "6 dernières heures": timedelta(hours=6),
    "24 dernières heures": timedelta(hours=24),
    "7 derniers jours": timedelta(days=7),
    "📊 Historique complet": timedelta(days=36500),
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

st.markdown("<h1 style='margin-bottom:8px'>₿ Crypto Data Pipeline</h1>" f"<p style='color:#A1C4DF;font-size:0.9rem;margin-top:0'>Données temps-réel · <b style='color:#06B6D4'>{periode}</b> · {len(selected)} crypto(s)</p>", unsafe_allow_html=True)

if not raw_df.empty:
    latest = raw_df.sort_values("fetched_at").groupby("coin_id").last().reset_index()
    cols = st.columns(len(latest))
    for i, (_, r) in enumerate(latest.iterrows()):
        chg = r.get("price_change_pct_24h", 0) or 0
        arrow = "▲" if chg > 0 else ("▼" if chg < 0 else "—")
        css = "kpi-up" if chg > 0 else ("kpi-down" if chg < 0 else "kpi-flat")
        top = COLORS.get(r["coin_id"], "#06B6D4")
        with cols[i]:
            st.markdown(f"<div class='kpi-card'><div style='position:relative;z-index:1'><div class='kpi-label'>💰 {r.get('name', r['coin_id'])}</div><div class='kpi-value'>${r['current_price']:,.2f}</div><div class='{css}'>{arrow} {chg:+.2f}%</div></div></div>", unsafe_allow_html=True)
else:
    st.warning("⚠️ Aucune donnée — lance d'abord le pipeline.")

st.markdown("<hr style='border-color:#1e3a5f;margin:14px 0 20px'>", unsafe_allow_html=True)

tab1, tab2, tab3, tab4, tab5 = st.tabs(["📊 Vue d'ensemble", "📈 Analyse technique", "⚠️ Alertes & Volume", "⚡ Streaming Kafka", "🔥 Spark Analytics"])

with tab1:
    c1, c2 = st.columns([3, 1])
    with c1:
        st.markdown("<div class='sec-title'>📈 Évolution des prix</div>", unsafe_allow_html=True)
        if not raw_df.empty:
            fig = go.Figure()
            for coin in selected:
                d = raw_df[raw_df["coin_id"] == coin]
                if d.empty: continue
                fig.add_trace(go.Scatter(x=d["fetched_at"], y=d["current_price"], name=coin.capitalize(), line=dict(color=COLORS.get(coin, "#06B6D4"), width=3), hovertemplate="<b>$%{y:,.2f}</b><br>%{x}<extra>" + coin + "</extra>"))
            fig.update_layout(**LAY, height=380, title=dict(text=f"Prix USD — {periode}", font=dict(size=14, color="#06B6D4")))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Pas encore de données.")

    with c2:
        st.markdown("<div class='sec-title'>🏆 Classement</div>", unsafe_allow_html=True)
        if not ranking_df.empty:
            today = str(datetime.now().date())
            td = ranking_df[ranking_df["day"].astype(str) == today]
            if td.empty: td = ranking_df.head(5)
            for _, r in td.head(5).iterrows():
                chg = r.get("price_change_pct_24h", 0) or 0
                color = "#10B981" if chg >= 0 else "#EF4444"
                bc = COLORS.get(r["coin_id"], "#06B6D4")
                st.markdown(f"<div class='rank-row'><span><span class='rank-num'>#{int(r['rank'])}</span><span class='rank-name'>{r['coin_id'].upper()}</span></span><span><span class='rank-price'>${r['current_price']:,.0f}</span><b style='color:{color}'>{chg:+.2f}%</b></span></div>", unsafe_allow_html=True)

with tab2:
    st.markdown("<div class='sec-title'>📊 Analyse Technique</div>", unsafe_allow_html=True)
    st.info("🔧 Analyseur technique en développement...")

with tab3:
    st.markdown("<div class='sec-title'>⚠️ Alertes</div>", unsafe_allow_html=True)
    if not alerts_df.empty:
        lat = alerts_df.sort_values("fetched_at").groupby("coin_id").last().reset_index()
        for _, r in lat.iterrows():
            chg = r.get("price_change_pct_24h", 0) or 0
            css = "alert-up" if chg > 0 else "alert-down"
            ico = "📈" if chg > 0 else "📉"
            st.markdown(f"<div class='{css}'>{ico} <b>{r.get('name',r['coin_id'])}</b> — {chg:+.2f}% | ${r['current_price']:,.2f}</div>", unsafe_allow_html=True)
    else:
        st.markdown(f"<div class='alert-ok'>✅ Tout va bien — aucune alerte</div>", unsafe_allow_html=True)

with tab4:
    st.markdown("<div class='sec-title'>⚡ Streaming Kafka</div>", unsafe_allow_html=True)
    st.info("📡 Flux streaming en développement...")

with tab5:
    st.markdown("<div class='spark-badge'>🔥 Spark Analytics</div>", unsafe_allow_html=True)
    st.markdown("<div class='spark-note'>⚡ Analyses avancées basées sur Spark SQL — Volatilité, Dominance, Corrélations</div>", unsafe_allow_html=True)
    if not vol_df.empty:
        st.markdown("<div class='sec-title'>📊 Volatilité</div>", unsafe_allow_html=True)
        st.dataframe(vol_df, use_container_width=True)

st.markdown("<hr style='border-color:#1e3a5f;margin-top:40px'><p style='text-align:center;color:#A1C4DF;font-size:0.75rem'>🚀 Crypto Data Pipeline — Python · PostgreSQL · Kafka · PySpark · Streamlit · Plotly</p>", unsafe_allow_html=True)
