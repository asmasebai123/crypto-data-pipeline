import sys, os, time
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
from sqlalchemy import create_engine
 
st.set_page_config(
    page_title="Crypto Pipeline Dashboard",
    page_icon="₿",
    layout="wide",
    initial_sidebar_state="collapsed",
)
 
# ═══════════════════════════════════════════════════════════════════════════════
# FIGMA-STYLE PREMIUM DESIGN: VIOLET + BLEU + BEIGE
# ═══════════════════════════════════════════════════════════════════════════════
 
st.markdown("""
<link href="https://fonts.googleapis.com/css2?family=Outfit:wght@400;500;600;700;800&family=Inter:wght@300;400;500;600;700&family=Space+Mono:wght@400;700&display=swap" rel="stylesheet">
 
<style>
/* PALETTE PREMIUM */
:root {
    --primary-violet: #7C3AED;
    --primary-blue: #3B82F6;
    --accent-beige: #F5F3FF;
    --accent-warm: #FEF3E2;
    --white: #FFFFFF;
    --text-primary: #1F2937;
    --text-secondary: #6B7280;
    --border-light: #E5E7EB;
    --success: #10B981;
    --danger: #EF4444;
}
 
* {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif !important;
}
 
h1, h2, h3, h4, h5, h6 {
    font-family: 'Outfit', sans-serif !important;
    font-weight: 700 !important;
    letter-spacing: -0.5px !important;
}
 
code, .code-block { font-family: 'Space Mono', monospace !important; }
 
.stApp {
    background: linear-gradient(135deg, #FFFFFF 0%, #F5F3FF 50%, #FEF3E2 100%) !important;
    color: var(--text-primary);
    background-attachment: fixed;
}
 
section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #F5F3FF 0%, #FFFFFF 100%) !important;
    border-right: 1px solid var(--border-light) !important;
}
 
h1 { color: var(--primary-violet) !important; font-size: 2.2rem !important; font-weight: 800 !important; margin-bottom: 8px !important; }
h2 { color: var(--text-primary) !important; font-size: 1.6rem !important; font-weight: 700 !important; margin: 32px 0 16px 0 !important; }
h3 { color: var(--primary-blue) !important; font-size: 1.2rem !important; font-weight: 600 !important; }
p, li, span, label { color: var(--text-secondary) !important; font-size: 14px !important; line-height: 1.7 !important; }
 
.kpi-card {
    background: rgba(255, 255, 255, 0.7) !important;
    backdrop-filter: blur(10px) !important;
    border: 1px solid rgba(123, 58, 237, 0.15) !important;
    border-radius: 12px !important;
    padding: 24px 20px !important;
    margin-bottom: 16px;
    box-shadow: 0 4px 6px rgba(0, 0, 0, 0.07), inset 0 1px 0 rgba(255, 255, 255, 0.6);
    transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
    position: relative;
    overflow: hidden;
}
 
.kpi-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 3px;
    background: linear-gradient(90deg, var(--primary-violet), var(--primary-blue));
}
 
.kpi-card:hover {
    transform: translateY(-4px);
    box-shadow: 0 12px 24px rgba(123, 58, 237, 0.15), inset 0 1px 0 rgba(255, 255, 255, 0.8);
    border-color: rgba(123, 58, 237, 0.3);
}
 
.kpi-label { color: var(--text-secondary); font-size: 0.7rem; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 10px; font-weight: 600; font-family: 'Space Mono', monospace; }
.kpi-value { color: var(--primary-violet); font-size: 2rem; font-weight: 800; margin-bottom: 8px; font-family: 'Outfit', sans-serif; letter-spacing: -0.5px; }
.kpi-up { color: var(--success); font-size: 0.9rem; font-weight: 600; }
.kpi-down { color: var(--danger); font-size: 0.9rem; font-weight: 600; }
.kpi-flat { color: var(--text-secondary); font-size: 0.9rem; }
 
.sec-title {
    color: var(--text-primary);
    font-size: 1.15rem;
    font-weight: 700;
    border-left: 4px solid var(--primary-violet);
    padding-left: 14px;
    margin: 32px 0 20px 0;
    letter-spacing: -0.3px;
    font-family: 'Outfit', sans-serif;
    position: relative;
}
 
.alert-up {
    background: linear-gradient(135deg, rgba(16, 185, 129, 0.1), rgba(16, 185, 129, 0.05));
    border: 1px solid rgba(16, 185, 129, 0.3);
    border-radius: 10px;
    padding: 14px 18px;
    margin: 12px 0;
    color: #059669;
    font-size: 0.9rem;
    font-weight: 600;
}
 
.alert-down {
    background: linear-gradient(135deg, rgba(239, 68, 68, 0.1), rgba(239, 68, 68, 0.05));
    border: 1px solid rgba(239, 68, 68, 0.3);
    border-radius: 10px;
    padding: 14px 18px;
    margin: 12px 0;
    color: #dc2626;
    font-size: 0.9rem;
    font-weight: 600;
}
 
.alert-ok {
    background: linear-gradient(135deg, rgba(16, 185, 129, 0.1), rgba(16, 185, 129, 0.05));
    border: 1px solid rgba(16, 185, 129, 0.3);
    border-radius: 10px;
    padding: 14px 18px;
    color: #059669;
    font-size: 0.9rem;
    font-weight: 600;
}
 
.rank-row {
    display: flex;
    justify-content: space-between;
    align-items: center;
    background: rgba(255, 255, 255, 0.5);
    backdrop-filter: blur(8px);
    border-radius: 10px;
    padding: 16px 18px;
    margin: 10px 0;
    border: 1px solid rgba(123, 58, 237, 0.15);
    border-left: 4px solid var(--primary-violet);
    box-shadow: 0 2px 4px rgba(0, 0, 0, 0.04);
    transition: all 0.2s ease;
}
 
.rank-row:hover {
    background: rgba(255, 255, 255, 0.7);
    box-shadow: 0 4px 12px rgba(123, 58, 237, 0.1);
}
 
.rank-num { color: white; font-size: 0.8rem; font-weight: 700; background: linear-gradient(135deg, var(--primary-violet), var(--primary-blue)); border-radius: 6px; padding: 6px 12px; font-family: 'Outfit', sans-serif; }
.rank-name { color: var(--text-primary); font-weight: 700; margin-left: 14px; font-family: 'Outfit', sans-serif; }
.rank-price { color: var(--text-secondary); font-size: 0.85rem; font-family: 'Space Mono', monospace; }
 
.spark-badge {
    background: linear-gradient(135deg, var(--primary-violet), var(--primary-blue));
    color: white;
    border-radius: 8px;
    padding: 8px 16px;
    font-size: 0.8rem;
    font-weight: 700;
    display: inline-block;
    margin-bottom: 12px;
    font-family: 'Outfit', sans-serif;
    box-shadow: 0 4px 12px rgba(123, 58, 237, 0.3);
}
 
.spark-note {
    background: linear-gradient(135deg, rgba(245, 243, 255, 0.8), rgba(254, 243, 226, 0.5));
    border: 1px solid rgba(123, 58, 237, 0.2);
    border-radius: 10px;
    padding: 16px 18px;
    color: var(--text-secondary);
    font-size: 0.85rem;
    margin-bottom: 18px;
    line-height: 1.7;
}
 
.stTabs [data-baseweb="tab-list"] { background: transparent !important; border-bottom: 2px solid var(--border-light) !important; }
.stTabs [data-baseweb="tab"] { color: var(--text-secondary) !important; border-radius: 0 !important; padding: 14px 22px !important; font-size: 0.95rem !important; font-weight: 600 !important; border-bottom: 3px solid transparent !important; font-family: 'Outfit', sans-serif !important; }
.stTabs [aria-selected="true"] { color: var(--primary-violet) !important; border-bottom: 3px solid var(--primary-violet) !important; font-weight: 700 !important; }
 
.stDataFrame { border: 1px solid var(--border-light) !important; border-radius: 10px !important; background: rgba(255, 255, 255, 0.6) !important; }
 
.stMultiSelect label, .stSelectbox label, .stSlider label, .stToggle label { color: var(--text-primary) !important; font-size: 0.85rem !important; font-weight: 700 !important; font-family: 'Outfit', sans-serif !important; }
 
.stButton > button { background: linear-gradient(135deg, var(--primary-violet), var(--primary-blue)) !important; color: white !important; border: none !important; border-radius: 8px !important; font-weight: 700 !important; padding: 12px 24px !important; font-family: 'Outfit', sans-serif !important; box-shadow: 0 4px 12px rgba(123, 58, 237, 0.2) !important; transition: all 0.3s ease !important; }
.stButton > button:hover { transform: translateY(-2px); box-shadow: 0 8px 20px rgba(123, 58, 237, 0.3) !important; }
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
 
COLORS = {
    "bitcoin": "#7C3AED", "ethereum": "#3B82F6",
    "solana": "#F59E0B",  "ripple": "#8B5CF6", "binancecoin": "#EC4899",
}
 
LAY = dict(
    plot_bgcolor="#FFFFFF", paper_bgcolor="#FFFFFF",
    font=dict(color="#1F2937", size=12, family="Inter"),
    xaxis=dict(gridcolor="#E5E7EB", zeroline=False, showgrid=True),
    yaxis=dict(gridcolor="#E5E7EB", zeroline=False, showgrid=True),
    legend=dict(bgcolor="rgba(255,255,255,0.95)", bordercolor="#E5E7EB", borderwidth=1, font=dict(color="#1F2937")),
    margin=dict(l=10, r=10, t=40, b=10),
    hovermode="x unified",
)
 
# SIDEBAR
with st.sidebar:
    st.markdown("### ₿ Crypto Dashboard")
    st.markdown("<hr style='border-color:#E5E7EB;margin:8px 0'>", unsafe_allow_html=True)
 
    all_coins = ["bitcoin", "ethereum", "ripple", "binancecoin", "solana"]
    selected = st.multiselect("Cryptomonnaies", all_coins, default=all_coins)
 
    periode = st.selectbox("Période", ["Dernière heure", "6 dernières heures", "24 dernières heures", "7 derniers jours", "📊 Historique complet"], index=2)
 
    seuil = st.slider("Seuil alerte (%)", 1, 20, 5)
 
    st.markdown("<hr style='border-color:#E5E7EB;margin:8px 0'>", unsafe_allow_html=True)
    auto_ref = st.toggle("Auto-refresh 60s", value=False)
    if st.button("🔄 Rafraîchir", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
 
    st.markdown(f"<p style='color:#6B7280;font-size:0.73rem;margin-top:6px'>Mis à jour : {datetime.now().strftime('%H:%M:%S')}</p>", unsafe_allow_html=True)
 
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
 
# DONNÉES
raw_df = load(f"SELECT * FROM raw_crypto_prices WHERE fetched_at >= '{since}' AND coin_id IN ('{coins_sql}') ORDER BY fetched_at ASC")
ranking_df = load("SELECT * FROM transform_daily_ranking ORDER BY rank ASC")
volume_df = load("SELECT * FROM transform_daily_volume ORDER BY day DESC, total_volume DESC")
alerts_df = load(f"SELECT * FROM raw_crypto_prices WHERE ABS(price_change_pct_24h) >= {seuil} AND fetched_at >= '{datetime.utcnow() - timedelta(hours=24)}' ORDER BY ABS(price_change_pct_24h) DESC")
stream_df = load("SELECT coin_id, price, variation_pct, event_time FROM stream_crypto_prices ORDER BY event_time DESC LIMIT 300")
 
vol_df = load(f"SELECT coin_id, name, ROUND(AVG(current_price)::numeric, 2) AS price_avg, ROUND(STDDEV(current_price)::numeric, 2) AS price_stddev, ROUND((STDDEV(current_price)/NULLIF(AVG(current_price),0)*100)::numeric, 2) AS volatility_pct FROM raw_crypto_prices WHERE coin_id IN ('{coins_sql}') GROUP BY coin_id, name ORDER BY volatility_pct DESC")
dom_df = load(f"WITH latest AS (SELECT DISTINCT ON (coin_id) coin_id, name, market_cap FROM raw_crypto_prices WHERE market_cap > 0 AND coin_id IN ('{coins_sql}') ORDER BY coin_id, fetched_at DESC), total AS (SELECT SUM(market_cap) AS t FROM latest) SELECT l.coin_id, l.name, l.market_cap, ROUND((l.market_cap / t.t * 100)::numeric, 2) AS dominance_pct FROM latest l, total t ORDER BY dominance_pct DESC")
 
# HEADER
st.markdown("<h2 style='margin-bottom:2px'>₿ Crypto Data Pipeline</h2>" f"<p style='color:#6B7280;font-size:0.85rem;margin-top:0'>Données temps-réel · <b style='color:#7C3AED'>{periode}</b> · {len(selected)} crypto(s)</p>", unsafe_allow_html=True)
 
# KPI
if not raw_df.empty:
    latest = raw_df.sort_values("fetched_at").groupby("coin_id").last().reset_index()
    cols = st.columns(len(latest))
    for i, (_, r) in enumerate(latest.iterrows()):
        chg = r.get("price_change_pct_24h", 0) or 0
        arrow = "▲" if chg > 0 else ("▼" if chg < 0 else "—")
        css = "kpi-up" if chg > 0 else ("kpi-down" if chg < 0 else "kpi-flat")
        top = COLORS.get(r["coin_id"], "#7C3AED")
        with cols[i]:
            st.markdown(f"<div class='kpi-card' style='border-top-color:{top}'><div class='kpi-label'>{r.get('name', r['coin_id'])}</div><div class='kpi-value'>${r['current_price']:,.2f}</div><div class='{css}'>{arrow} {chg:+.2f}% / 24h</div></div>", unsafe_allow_html=True)
else:
    st.warning("Aucune donnée — lance d'abord le pipeline.")
 
st.markdown("<hr style='border-color:#E5E7EB;margin:10px 0 16px'>", unsafe_allow_html=True)
 
# TABS
tab1, tab2, tab3, tab4, tab5 = st.tabs(["📊 Vue d'ensemble", "📈 Analyse technique", "⚠️ Alertes & Volume", "⚡ Streaming Kafka", "🔥 Spark Analytics"])
 
with tab1:
    c1, c2 = st.columns([3, 1])
    with c1:
        st.markdown("<div class='sec-title'>Évolution des prix</div>", unsafe_allow_html=True)
        if not raw_df.empty:
            fig = go.Figure()
            for coin in selected:
                d = raw_df[raw_df["coin_id"] == coin]
                if d.empty: continue
                fig.add_trace(go.Scatter(x=d["fetched_at"], y=d["current_price"], name=coin.capitalize(), line=dict(color=COLORS.get(coin, "#aaa"), width=2), hovertemplate="<b>$%{y:,.2f}</b><br>%{x}<extra>" + coin + "</extra>"))
            fig.update_layout(**LAY, height=350, title=dict(text=f"Prix USD — {periode}", font=dict(size=13, color="#888888")))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Pas encore de données.")
 
    with c2:
        st.markdown("<div class='sec-title'>Classement du jour</div>", unsafe_allow_html=True)
        if not ranking_df.empty:
            today = str(datetime.now().date())
            td = ranking_df[ranking_df["day"].astype(str) == today]
            if td.empty: td = ranking_df.head(5)
            for _, r in td.head(5).iterrows():
                chg = r.get("price_change_pct_24h", 0) or 0
                color = "#10b981" if chg >= 0 else "#ef4444"
                bc = COLORS.get(r["coin_id"], "#7C3AED")
                st.markdown(f"<div class='rank-row' style='border-left-color:{bc}'><span><span class='rank-num'>#{int(r['rank'])}</span><span class='rank-name'>{r['coin_id'].capitalize()}</span></span><span><span class='rank-price'>${r['current_price']:,.0f}</span><b style='color:{color};margin-left:8px'>{chg:+.2f}%</b></span></div>", unsafe_allow_html=True)
        else:
            st.info("Classement non disponible.")
 
    st.markdown("<div class='sec-title' style='margin-top:18px'>Résumé des prix actuels</div>", unsafe_allow_html=True)
    if not raw_df.empty:
        summ = raw_df.sort_values("fetched_at").groupby("coin_id").last().reset_index()
        summ = summ[["coin_id","name","current_price","price_change_pct_24h","total_volume","high_24h","low_24h"]].copy()
        summ.columns = ["ID","Nom","Prix ($)","Var. 24h (%)","Volume","Haut 24h","Bas 24h"]
        st.dataframe(summ.style.format({"Prix ($)":"${:,.2f}","Var. 24h (%)":"{:+.2f}%","Volume":"${:,.0f}","Haut 24h":"${:,.2f}","Bas 24h":"${:,.2f}"}).map(lambda v: "color:#10b981;font-weight:600" if isinstance(v,(int,float)) and v>0 else ("color:#ef4444;font-weight:600" if isinstance(v,(int,float)) and v<0 else ""), subset=["Var. 24h (%)"]), use_container_width=True, height=210)
 
with tab2:
    st.markdown("<div class='sec-title'>Graphique OHLC (par heure)</div>", unsafe_allow_html=True)
    coin_ch = st.selectbox("Crypto à analyser", selected if selected else ["bitcoin"], key="ohlc")
    if not raw_df.empty:
        ohlc = (raw_df[raw_df["coin_id"] == coin_ch].copy().assign(hour=lambda d: pd.to_datetime(d["fetched_at"]).dt.floor("h")).groupby("hour").agg(open=("current_price","first"), high=("current_price","max"), low=("current_price","min"), close=("current_price","last"), vol=("total_volume","mean")).reset_index())
        if len(ohlc) >= 2:
            fig2 = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.75,0.25], vertical_spacing=0.04)
            fig2.add_trace(go.Candlestick(x=ohlc["hour"], open=ohlc["open"], high=ohlc["high"], low=ohlc["low"], close=ohlc["close"], increasing_line_color="#10b981", decreasing_line_color="#ef4444", name=coin_ch.capitalize()), row=1, col=1)
            fig2.add_trace(go.Bar(x=ohlc["hour"], y=ohlc["vol"], marker_color=COLORS.get(coin_ch,"#7C3AED"), opacity=0.6, name="Volume"), row=2, col=1)
            fig2.update_layout(**LAY, height=450, xaxis_rangeslider_visible=False, title=dict(text=f"OHLC — {coin_ch.capitalize()}", font=dict(size=13,color="#888888")))
            st.plotly_chart(fig2, use_container_width=True)
 
with tab3:
    ca, cb = st.columns(2)
    with ca:
        st.markdown(f"<div class='sec-title'>Alertes volatilité (seuil ±{seuil}%)</div>", unsafe_allow_html=True)
        if not alerts_df.empty:
            lat = alerts_df.sort_values("fetched_at").groupby("coin_id").last().reset_index()
            for _, r in lat.iterrows():
                chg = r.get("price_change_pct_24h", 0) or 0
                css = "alert-up" if chg > 0 else "alert-down"
                ico = "▲" if chg > 0 else "▼"
                lbl = "HAUSSE FORTE" if chg > 0 else "BAISSE FORTE"
                st.markdown(f"<div class='{css}'>{ico} <b>{r.get('name',r['coin_id'])}</b> — {lbl}<br><span style='font-size:0.85rem'>{chg:+.2f}% &nbsp;|&nbsp; ${r['current_price']:,.2f}</span></div>", unsafe_allow_html=True)
        else:
            st.markdown(f"<div class='alert-ok'>✅ Aucune alerte — variations sous ±{seuil}%</div>", unsafe_allow_html=True)
 
with tab4:
    st.markdown("<div class='sec-title'>Flux Kafka — derniers événements</div>", unsafe_allow_html=True)
    if not stream_df.empty:
        st.dataframe(stream_df.head(20), use_container_width=True)
    else:
        st.info("⚡ Aucun événement Kafka disponible.")
 
with tab5:
    st.markdown("<div class='spark-badge'>⚡ PySpark — Analyses avancées</div>", unsafe_allow_html=True)
    if not vol_df.empty:
        st.markdown("<div class='sec-title'>Volatilité des prix</div>", unsafe_allow_html=True)
        st.dataframe(vol_df, use_container_width=True)
    if not dom_df.empty:
        st.markdown("<div class='sec-title'>Dominance du marché</div>", unsafe_allow_html=True)
        st.dataframe(dom_df, use_container_width=True)
 
# Footer
st.markdown("<hr style='border-color:#E5E7EB;margin-top:30px'><p style='text-align:center;color:#9CA3AF;font-size:0.75rem'>Crypto Data Pipeline — Python · PostgreSQL (Neon) · Kafka · PySpark · Streamlit · Plotly</p>", unsafe_allow_html=True)