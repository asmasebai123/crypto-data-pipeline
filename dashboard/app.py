# dashboard/app.py — Version 2.1
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
    initial_sidebar_state="expanded",
)

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
/* fond global */
.stApp { background-color: #0f1117; color: #e0e6f0; }
section[data-testid="stSidebar"] {
    background-color: #161b27;
    border-right: 1px solid #2a3548;
}

/* titres Streamlit */
h1, h2, h3, h4 { color: #e8f0fe !important; }
p, li, span, label { color: #c8d8f0; }

/* ── KPI cards ── */
.kpi-card {
    background: #1a2236;
    border: 1px solid #2a3f5f;
    border-radius: 10px;
    padding: 16px;
    text-align: center;
    margin-bottom: 6px;
}
.kpi-label {
    color: #90a4c0;
    font-size: 0.75rem;
    text-transform: uppercase;
    letter-spacing: 0.07em;
    margin-bottom: 6px;
}
.kpi-value {
    color: #ffffff;
    font-size: 1.5rem;
    font-weight: 700;
    margin-bottom: 4px;
}
.kpi-up   { color: #00e0a0; font-size: 0.88rem; font-weight: 600; }
.kpi-down { color: #ff5c6c; font-size: 0.88rem; font-weight: 600; }
.kpi-flat { color: #90a4c0; font-size: 0.88rem; }

/* ── Section header ── */
.sec-title {
    color: #c8d8f0;
    font-size: 1rem;
    font-weight: 700;
    border-left: 3px solid #4a7fd4;
    padding-left: 10px;
    margin: 14px 0 10px 0;
}

/* ── Alert boxes ── */
.alert-up {
    background: #0d2e22;
    border: 1px solid #00c880;
    border-radius: 8px;
    padding: 10px 14px;
    margin: 5px 0;
    color: #00e0a0;
    font-size: 0.9rem;
}
.alert-down {
    background: #2e0d14;
    border: 1px solid #ff5c6c;
    border-radius: 8px;
    padding: 10px 14px;
    margin: 5px 0;
    color: #ff7b87;
    font-size: 0.9rem;
}
.alert-ok {
    background: #0d2218;
    border: 1px solid #00c880;
    border-radius: 8px;
    padding: 10px 14px;
    color: #00e0a0;
    font-size: 0.9rem;
}

/* ── Rank row ── */
.rank-row {
    display: flex;
    justify-content: space-between;
    align-items: center;
    background: #1a2236;
    border-radius: 8px;
    padding: 9px 14px;
    margin: 4px 0;
    border-left: 3px solid #4a7fd4;
}
.rank-num { color: #90a4c0; font-size: 0.8rem; font-weight: 700; background: #2a3f5f; border-radius: 4px; padding: 2px 7px; }
.rank-name { color: #dce8ff; font-weight: 600; margin-left: 8px; }
.rank-price { color: #90a4c0; font-size: 0.82rem; }

/* ── Spark badge ── */
.spark-badge {
    background: linear-gradient(90deg, #c8440a, #e8901a);
    color: #fff;
    border-radius: 6px;
    padding: 4px 12px;
    font-size: 0.78rem;
    font-weight: 700;
    display: inline-block;
    margin-bottom: 8px;
}
.spark-note {
    background: #1a2236;
    border: 1px solid #2a3f5f;
    border-radius: 8px;
    padding: 10px 14px;
    color: #90a4c0;
    font-size: 0.82rem;
    margin-bottom: 14px;
}

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"] {
    background: #161b27;
    border-bottom: 1px solid #2a3548;
    gap: 4px;
}
.stTabs [data-baseweb="tab"] {
    color: #90a4c0;
    border-radius: 6px 6px 0 0;
    padding: 7px 16px;
    font-size: 0.88rem;
}
.stTabs [aria-selected="true"] {
    background: #1a2236 !important;
    color: #ffffff !important;
    font-weight: 600 !important;
}

/* ── Dataframes ── */
.stDataFrame { border: 1px solid #2a3f5f; border-radius: 8px; }

/* ── Sidebar labels ── */
.stMultiSelect label, .stSelectbox label, .stSlider label, .stToggle label {
    color: #c8d8f0 !important;
    font-size: 0.85rem !important;
}
</style>
""", unsafe_allow_html=True)

# ── DB ────────────────────────────────────────────────────────────────────────
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
    "bitcoin": "#F7931A", "ethereum": "#627EEA",
    "solana": "#9945FF",  "ripple": "#00AAE4", "binancecoin": "#F3BA2F",
}
LAY = dict(
    plot_bgcolor="#0f1117", paper_bgcolor="#0f1117",
    font=dict(color="#c8d8f0", size=12),
    xaxis=dict(gridcolor="#1e2a3a", zeroline=False, color="#90a4c0"),
    yaxis=dict(gridcolor="#1e2a3a", zeroline=False, color="#90a4c0"),
    legend=dict(bgcolor="#1a2236", bordercolor="#2a3f5f", borderwidth=1,
                font=dict(color="#c8d8f0")),
    margin=dict(l=10, r=10, t=40, b=10),
)

# ── SIDEBAR ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### ₿ Crypto Dashboard")
    st.markdown("<hr style='border-color:#2a3548;margin:8px 0'>", unsafe_allow_html=True)

    all_coins = ["bitcoin", "ethereum", "ripple", "binancecoin", "solana"]
    selected = st.multiselect("Cryptomonnaies", all_coins, default=all_coins)

    periode = st.selectbox(
        "Période", ["Dernière heure", "6 dernières heures",
                    "24 dernières heures", "7 derniers jours"], index=2)

    seuil = st.slider("Seuil alerte (%)", 1, 20, 5)

    st.markdown("<hr style='border-color:#2a3548;margin:8px 0'>", unsafe_allow_html=True)
    auto_ref = st.toggle("Auto-refresh 60s", value=False)
    if st.button("🔄 Rafraîchir", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.markdown(
        f"<p style='color:#607080;font-size:0.73rem;margin-top:6px'>"
        f"Mis à jour : {datetime.now().strftime('%H:%M:%S')}</p>",
        unsafe_allow_html=True)

if auto_ref:
    time.sleep(60)
    st.cache_data.clear()
    st.rerun()

# ── Filtre temps ──────────────────────────────────────────────────────────────
delta_map = {
    "Dernière heure": timedelta(hours=1),
    "6 dernières heures": timedelta(hours=6),
    "24 dernières heures": timedelta(hours=24),
    "7 derniers jours": timedelta(days=7),
}
since     = datetime.utcnow() - delta_map[periode]
coins_sql = "','".join(selected)

# ── Données ───────────────────────────────────────────────────────────────────
raw_df = load(f"""
    SELECT * FROM raw_crypto_prices
    WHERE fetched_at >= '{since}' AND coin_id IN ('{coins_sql}')
    ORDER BY fetched_at ASC
""")
ranking_df = load("SELECT * FROM transform_daily_ranking ORDER BY rank ASC")
volume_df  = load("SELECT * FROM transform_daily_volume ORDER BY day DESC, total_volume DESC")
alerts_df  = load(f"""
    SELECT * FROM raw_crypto_prices
    WHERE ABS(price_change_pct_24h) >= {seuil}
      AND fetched_at >= '{datetime.utcnow() - timedelta(hours=24)}'
    ORDER BY ABS(price_change_pct_24h) DESC
""")
stream_df = load("""
    SELECT coin_id, price, variation_pct, event_time
    FROM stream_crypto_prices ORDER BY event_time DESC LIMIT 300
""")

# Analyses "Spark SQL" calculées via PostgreSQL (disponibles en ligne)
vol_df = load(f"""
    SELECT coin_id, name,
           ROUND(AVG(current_price)::numeric, 2)    AS price_avg,
           ROUND(STDDEV(current_price)::numeric, 2) AS price_stddev,
           ROUND((STDDEV(current_price)/NULLIF(AVG(current_price),0)*100)::numeric, 2) AS volatility_pct
    FROM raw_crypto_prices
    WHERE coin_id IN ('{coins_sql}')
    GROUP BY coin_id, name
    ORDER BY volatility_pct DESC
""")
dom_df = load(f"""
    WITH latest AS (
        SELECT DISTINCT ON (coin_id) coin_id, name, market_cap
        FROM raw_crypto_prices
        WHERE market_cap > 0 AND coin_id IN ('{coins_sql}')
        ORDER BY coin_id, fetched_at DESC
    ),
    total AS (SELECT SUM(market_cap) AS t FROM latest)
    SELECT l.coin_id, l.name, l.market_cap,
           ROUND((l.market_cap / t.t * 100)::numeric, 2) AS dominance_pct
    FROM latest l, total t
    ORDER BY dominance_pct DESC
""")

# ── HEADER ────────────────────────────────────────────────────────────────────
st.markdown(
    "<h2 style='margin-bottom:2px'>₿ Crypto Data Pipeline — Dashboard</h2>"
    f"<p style='color:#90a4c0;font-size:0.85rem;margin-top:0'>"
    f"Données quasi temps-réel · <b style='color:#c8d8f0'>{periode}</b> · "
    f"{len(selected)} crypto(s)</p>",
    unsafe_allow_html=True)

# ── KPI ───────────────────────────────────────────────────────────────────────
if not raw_df.empty:
    latest = raw_df.sort_values("fetched_at").groupby("coin_id").last().reset_index()
    cols = st.columns(len(latest))
    for i, (_, r) in enumerate(latest.iterrows()):
        chg = r.get("price_change_pct_24h", 0) or 0
        arrow = "▲" if chg > 0 else ("▼" if chg < 0 else "—")
        css   = "kpi-up" if chg > 0 else ("kpi-down" if chg < 0 else "kpi-flat")
        top   = COLORS.get(r["coin_id"], "#4a7fd4")
        with cols[i]:
            st.markdown(f"""
            <div class="kpi-card" style="border-top:3px solid {top}">
                <div class="kpi-label">{r.get('name', r['coin_id'])}</div>
                <div class="kpi-value">${r['current_price']:,.2f}</div>
                <div class="{css}">{arrow} {chg:+.2f}% / 24h</div>
            </div>""", unsafe_allow_html=True)
else:
    st.warning("Aucune donnée — lance d'abord le pipeline.")

st.markdown("<hr style='border-color:#2a3548;margin:10px 0 16px'>", unsafe_allow_html=True)

# ══ ONGLETS ═══════════════════════════════════════════════════════════════════
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Vue d'ensemble",
    "📈 Analyse technique",
    "⚠️ Alertes & Volume",
    "⚡ Streaming Kafka",
    "🔥 Spark Analytics",
])

# ─── TAB 1 : Vue d'ensemble ───────────────────────────────────────────────────
with tab1:
    c1, c2 = st.columns([3, 1])
    with c1:
        st.markdown("<div class='sec-title'>Évolution des prix</div>", unsafe_allow_html=True)
        if not raw_df.empty:
            fig = go.Figure()
            for coin in selected:
                d = raw_df[raw_df["coin_id"] == coin]
                if d.empty: continue
                fig.add_trace(go.Scatter(
                    x=d["fetched_at"], y=d["current_price"],
                    name=coin.capitalize(),
                    line=dict(color=COLORS.get(coin, "#aaa"), width=2),
                    hovertemplate="<b>$%{y:,.2f}</b><br>%{x}<extra>" + coin + "</extra>"
                ))
            fig.update_layout(**LAY, height=350, hovermode="x unified",
                title=dict(text=f"Prix USD — {periode}", font=dict(size=13, color="#90a4c0")))
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
                chg   = r.get("price_change_pct_24h", 0) or 0
                color = "#00e0a0" if chg >= 0 else "#ff5c6c"
                bc    = COLORS.get(r["coin_id"], "#4a7fd4")
                st.markdown(f"""
                <div class="rank-row" style="border-left-color:{bc}">
                    <span>
                        <span class="rank-num">#{int(r['rank'])}</span>
                        <span class="rank-name">{r['coin_id'].capitalize()}</span>
                    </span>
                    <span>
                        <span class="rank-price">${r['current_price']:,.0f}</span>
                        <b style="color:{color};margin-left:8px">{chg:+.2f}%</b>
                    </span>
                </div>""", unsafe_allow_html=True)
        else:
            st.info("Classement non disponible.")

    st.markdown("<div class='sec-title' style='margin-top:18px'>Résumé des prix actuels</div>", unsafe_allow_html=True)
    if not raw_df.empty:
        summ = raw_df.sort_values("fetched_at").groupby("coin_id").last().reset_index()
        summ = summ[["coin_id","name","current_price","price_change_pct_24h",
                     "total_volume","high_24h","low_24h"]].copy()
        summ.columns = ["ID","Nom","Prix ($)","Var. 24h (%)","Volume","Haut 24h","Bas 24h"]
        st.dataframe(
            summ.style
                .format({"Prix ($)":"${:,.2f}","Var. 24h (%)":"{:+.2f}%",
                         "Volume":"${:,.0f}","Haut 24h":"${:,.2f}","Bas 24h":"${:,.2f}"})
                .map(lambda v: "color:#00e0a0;font-weight:600" if isinstance(v,(int,float)) and v>0
                     else ("color:#ff5c6c;font-weight:600" if isinstance(v,(int,float)) and v<0 else ""),
                     subset=["Var. 24h (%)"]),
            use_container_width=True, height=210)

# ─── TAB 2 : Analyse technique ────────────────────────────────────────────────
with tab2:
    st.markdown("<div class='sec-title'>Graphique Chandelier OHLC (par heure)</div>", unsafe_allow_html=True)
    coin_ch = st.selectbox("Crypto à analyser", selected if selected else all_coins, key="ohlc")

    if not raw_df.empty:
        ohlc = (
            raw_df[raw_df["coin_id"] == coin_ch].copy()
            .assign(hour=lambda d: pd.to_datetime(d["fetched_at"]).dt.floor("h"))
            .groupby("hour")
            .agg(open=("current_price","first"), high=("current_price","max"),
                 low=("current_price","min"), close=("current_price","last"),
                 vol=("total_volume","mean"))
            .reset_index()
        )
        if len(ohlc) >= 2:
            fig2 = make_subplots(rows=2, cols=1, shared_xaxes=True,
                                 row_heights=[0.75,0.25], vertical_spacing=0.04)
            fig2.add_trace(go.Candlestick(
                x=ohlc["hour"], open=ohlc["open"], high=ohlc["high"],
                low=ohlc["low"], close=ohlc["close"],
                increasing_line_color="#00e0a0", decreasing_line_color="#ff5c6c",
                increasing_fillcolor="#00e0a0", decreasing_fillcolor="#ff5c6c",
                name=coin_ch.capitalize()), row=1, col=1)
            fig2.add_trace(go.Bar(
                x=ohlc["hour"], y=ohlc["vol"],
                marker_color=COLORS.get(coin_ch,"#4a7fd4"), opacity=0.6, name="Volume"),
                row=2, col=1)
            fig2.update_layout(**LAY, height=450, xaxis_rangeslider_visible=False,
                title=dict(text=f"OHLC — {coin_ch.capitalize()}", font=dict(size=13,color="#90a4c0")))
            fig2.update_yaxes(gridcolor="#1e2a3a", color="#90a4c0")
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("Pas assez de points — augmente la période d'analyse.")

    st.markdown("<hr style='border-color:#2a3548;margin:14px 0'>", unsafe_allow_html=True)
    st.markdown("<div class='sec-title'>Heatmap de corrélation</div>", unsafe_allow_html=True)
    st.caption("1.0 = évoluent identiquement · -1.0 = évoluent en sens inverse · 0 = indépendants")

    if not raw_df.empty and len(selected) >= 2:
        piv = (raw_df[raw_df["coin_id"].isin(selected)]
               .pivot_table(index="fetched_at", columns="coin_id", values="current_price")
               .ffill().dropna())
        if piv.shape[1] >= 2:
            corr = piv.pct_change().dropna().corr().round(2)
            labs = [c.capitalize() for c in corr.columns]
            fhm = go.Figure(go.Heatmap(
                z=corr.values, x=labs, y=labs,
                colorscale=[[0,"#ff5c6c"],[0.5,"#2a3548"],[1,"#00e0a0"]],
                zmin=-1, zmax=1,
                text=corr.values.round(2), texttemplate="%{text}",
                textfont=dict(size=13, color="#ffffff")))
            fhm.update_layout(**LAY, height=340,
                title=dict(text="Corrélation des rendements", font=dict(size=13,color="#90a4c0")))
            st.plotly_chart(fhm, use_container_width=True)

    st.markdown("<div class='sec-title' style='margin-top:14px'>Performance relative (base 100)</div>", unsafe_allow_html=True)
    st.caption("Toutes les cryptos ramenées à 100 au départ — compare les gains/pertes")
    if not raw_df.empty:
        frel = go.Figure()
        for coin in selected:
            d = raw_df[raw_df["coin_id"] == coin].sort_values("fetched_at")
            if d.empty or d["current_price"].iloc[0] == 0: continue
            base = d["current_price"].iloc[0]
            frel.add_trace(go.Scatter(
                x=d["fetched_at"], y=(d["current_price"]/base*100).round(2),
                name=coin.capitalize(),
                line=dict(color=COLORS.get(coin,"#aaa"), width=2),
                hovertemplate="<b>%{y:.1f}</b><extra>" + coin + "</extra>"))
        frel.add_hline(y=100, line_dash="dot", line_color="#607080", opacity=0.6)
        frel.update_layout(**LAY, height=290, hovermode="x unified",
            title=dict(text="Indice de performance (base 100)", font=dict(size=13,color="#90a4c0")),
            yaxis_title="Indice")
        st.plotly_chart(frel, use_container_width=True)

# ─── TAB 3 : Alertes & Volume ─────────────────────────────────────────────────
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
                st.markdown(
                    f"<div class='{css}'>{ico} <b>{r.get('name',r['coin_id'])}</b> — {lbl}<br>"
                    f"<span style='font-size:0.85rem'>{chg:+.2f}% &nbsp;|&nbsp; ${r['current_price']:,.2f}</span></div>",
                    unsafe_allow_html=True)
        else:
            st.markdown(f"<div class='alert-ok'>✅ Aucune alerte — variations sous ±{seuil}%</div>", unsafe_allow_html=True)

        if not raw_df.empty:
            st.markdown("<div class='sec-title' style='margin-top:16px'>Variation 24h (%)</div>", unsafe_allow_html=True)
            lat2 = raw_df.sort_values("fetched_at").groupby("coin_id").last().reset_index()
            lat2 = lat2[lat2["coin_id"].isin(selected)]
            fbar = go.Figure(go.Bar(
                x=lat2["coin_id"].str.capitalize(),
                y=lat2["price_change_pct_24h"],
                marker_color=lat2["price_change_pct_24h"].apply(lambda x: "#00e0a0" if x>=0 else "#ff5c6c"),
                text=lat2["price_change_pct_24h"].apply(lambda x: f"{x:+.2f}%"),
                textposition="outside", textfont=dict(color="#c8d8f0")))
            fbar.add_hline(y=0, line_color="#607080", line_width=1)
            fbar.update_layout(**LAY, height=280, showlegend=False,
                title=dict(text="Variation 24h (%)", font=dict(size=13,color="#90a4c0")))
            st.plotly_chart(fbar, use_container_width=True)

    with cb:
        st.markdown("<div class='sec-title'>Volume journalier échangé</div>", unsafe_allow_html=True)
        if not volume_df.empty:
            last_day = volume_df["day"].max() if "day" in volume_df.columns else None
            lv = volume_df[volume_df["day"] == last_day] if last_day else volume_df.head(5)
            lv = lv[lv["coin_id"].isin(selected)]
            fvol = go.Figure(go.Bar(
                x=lv["coin_id"].str.capitalize(), y=lv["total_volume"],
                marker_color=[COLORS.get(c,"#aaa") for c in lv["coin_id"]],
                text=lv["total_volume"].apply(lambda x: f"${x/1e9:.2f}B" if x>1e9 else f"${x/1e6:.1f}M"),
                textposition="outside", textfont=dict(color="#c8d8f0")))
            fvol.update_layout(**LAY, height=290, showlegend=False,
                title=dict(text=f"Volume — {last_day}", font=dict(size=13,color="#90a4c0")),
                yaxis_title="Volume USD")
            st.plotly_chart(fvol, use_container_width=True)

            vf = volume_df[volume_df["coin_id"].isin(selected)]
            if not vf.empty:
                fa = px.area(vf, x="day", y="total_volume", color="coin_id",
                             color_discrete_map=COLORS,
                             labels={"day":"Date","total_volume":"Volume","coin_id":"Crypto"})
                fa.update_layout(**LAY, height=250)
                st.plotly_chart(fa, use_container_width=True)
        else:
            st.info("Volumes non disponibles.")

# ─── TAB 4 : Streaming Kafka ──────────────────────────────────────────────────
with tab4:
    st.markdown("<div class='sec-title'>Flux temps réel Kafka — derniers événements</div>", unsafe_allow_html=True)
    if not stream_df.empty:
        sf = stream_df[stream_df["coin_id"].isin(selected)].copy()
        sf["event_time"] = pd.to_datetime(sf["event_time"])

        fst = go.Figure()
        for coin in selected:
            d = sf[sf["coin_id"] == coin].sort_values("event_time")
            if d.empty: continue
            fst.add_trace(go.Scatter(
                x=d["event_time"], y=d["price"],
                name=coin.capitalize(), mode="lines+markers",
                line=dict(color=COLORS.get(coin,"#aaa"), width=1.5),
                marker=dict(size=4),
                hovertemplate="<b>$%{y:,.2f}</b><br>%{x}<extra>" + coin + "</extra>"))
        fst.update_layout(**LAY, height=330, hovermode="x unified",
            title=dict(text="Prix streaming (Kafka)", font=dict(size=13,color="#90a4c0")))
        st.plotly_chart(fst, use_container_width=True)

        cs1, cs2 = st.columns(2)
        with cs1:
            st.markdown("<div class='sec-title'>Distribution des variations (%)</div>", unsafe_allow_html=True)
            fhi = go.Figure()
            for coin in selected:
                d = sf[sf["coin_id"] == coin]
                if d.empty: continue
                fhi.add_trace(go.Histogram(
                    x=d["variation_pct"], name=coin.capitalize(),
                    marker_color=COLORS.get(coin,"#aaa"), opacity=0.7, nbinsx=20))
            fhi.update_layout(**LAY, height=270, barmode="overlay",
                title=dict(text="Distribution variation_pct", font=dict(size=13,color="#90a4c0")),
                xaxis_title="Variation (%)", yaxis_title="Fréquence")
            st.plotly_chart(fhi, use_container_width=True)
        with cs2:
            st.markdown("<div class='sec-title'>Derniers événements reçus</div>", unsafe_allow_html=True)
            disp = sf.head(20)[["coin_id","price","variation_pct","event_time"]].copy()
            disp.columns = ["Crypto","Prix ($)","Var. (%)","Horodatage"]
            st.dataframe(
                disp.style
                    .format({"Prix ($)":"${:,.2f}","Var. (%)":"{:+.2f}%"})
                    .map(lambda v: "color:#00e0a0" if isinstance(v,(int,float)) and v>0
                         else ("color:#ff5c6c" if isinstance(v,(int,float)) and v<0 else ""),
                         subset=["Var. (%)"]),
                use_container_width=True, height=270)
    else:
        st.markdown(
            "<div style='background:#1a2236;border:1px solid #2a3f5f;border-radius:8px;"
            "padding:16px;color:#90a4c0'>"
            "⚡ Aucun événement Kafka disponible.<br>"
            "<code style='color:#c8d8f0'>python ingestion/kafka_producer.py</code>"
            "</div>", unsafe_allow_html=True)

# ─── TAB 5 : Spark Analytics ──────────────────────────────────────────────────
with tab5:
    st.markdown(
        "<div class='spark-badge'>⚡ PySpark — Analyses avancées</div>",
        unsafe_allow_html=True)
    st.markdown(
        "<div class='spark-note'>"
        "Ces analyses reproduisent les calculs du moteur Spark (volatilité, dominance marché). "
        "En ligne, elles sont calculées directement via PostgreSQL. "
        "En local, lance <code>python transformations/run_transforms.py --engine spark</code> "
        "pour utiliser le vrai moteur PySpark."
        "</div>", unsafe_allow_html=True)

    sp1, sp2 = st.columns(2)

    with sp1:
        st.markdown("<div class='sec-title'>Volatilité des prix (écart-type)</div>", unsafe_allow_html=True)
        st.caption("Écart-type élevé = crypto plus risquée mais potentiellement plus rentable")
        if not vol_df.empty:
            fvs = go.Figure(go.Bar(
                x=vol_df["coin_id"].str.capitalize(),
                y=vol_df["volatility_pct"],
                marker_color=[COLORS.get(c,"#4a7fd4") for c in vol_df["coin_id"]],
                text=vol_df["volatility_pct"].apply(lambda x: f"{x:.2f}%"),
                textposition="outside", textfont=dict(color="#c8d8f0")))
            fvs.update_layout(**LAY, height=290, showlegend=False,
                title=dict(text="Volatilité % (écart-type / moyenne)", font=dict(size=13,color="#90a4c0")),
                yaxis_title="Volatilité (%)")
            st.plotly_chart(fvs, use_container_width=True)

            disp_vol = vol_df[["coin_id","name","price_avg","price_stddev","volatility_pct"]].copy()
            disp_vol.columns = ["ID","Nom","Prix moyen","Écart-type","Volatilité (%)"]
            st.dataframe(
                disp_vol.style
                    .format({"Prix moyen":"${:,.2f}","Écart-type":"${:,.2f}","Volatilité (%)":"{:.2f}%"})
                    .map(lambda v: "color:#ff5c6c;font-weight:600" if isinstance(v,(int,float)) and v > 5
                         else ("color:#00e0a0;font-weight:600" if isinstance(v,(int,float)) and v <= 5 else ""),
                         subset=["Volatilité (%)"]),
                use_container_width=True)
        else:
            st.info("Pas encore de données de prix.")

    with sp2:
        st.markdown("<div class='sec-title'>Dominance du marché (market cap)</div>", unsafe_allow_html=True)
        st.caption("Part de capitalisation boursière de chaque crypto")
        if not dom_df.empty:
            fdom = go.Figure(go.Pie(
                labels=dom_df["coin_id"].str.capitalize(),
                values=dom_df["dominance_pct"],
                marker_colors=[COLORS.get(c,"#aaa") for c in dom_df["coin_id"]],
                hole=0.42,
                textinfo="label+percent",
                textfont=dict(size=12, color="#ffffff"),
                hovertemplate="<b>%{label}</b><br>%{percent}<extra></extra>"))
            fdom.update_layout(**LAY, height=320,
                title=dict(text="Dominance marché (%)", font=dict(size=13,color="#90a4c0")))
            st.plotly_chart(fdom, use_container_width=True)

            disp_dom = dom_df[["coin_id","name","market_cap","dominance_pct"]].copy()
            disp_dom.columns = ["ID","Nom","Market Cap ($)","Dominance (%)"]
            st.dataframe(
                disp_dom.style.format({"Market Cap ($)":"${:,.0f}","Dominance (%)":"{:.2f}%"}),
                use_container_width=True)
        else:
            st.info("Pas encore de données market cap.")

    with st.expander("📋 Données brutes complètes (raw_crypto_prices)"):
        if not raw_df.empty:
            cols_s = ["coin_id","name","current_price","price_change_pct_24h",
                      "total_volume","market_cap","high_24h","low_24h","fetched_at"]
            cols_s = [c for c in cols_s if c in raw_df.columns]
            st.dataframe(raw_df[cols_s].sort_values("fetched_at",ascending=False).head(100),
                         use_container_width=True)

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown(
    "<hr style='border-color:#2a3548;margin-top:30px'>"
    "<p style='text-align:center;color:#4a5a6a;font-size:0.75rem'>"
    "Crypto Data Pipeline v2.1 · Python · PostgreSQL (Neon) · Kafka · PySpark · Streamlit · Plotly"
    "</p>", unsafe_allow_html=True)
