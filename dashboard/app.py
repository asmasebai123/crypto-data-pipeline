# dashboard/app.py  — Version 2.0 (Dark Pro)
"""
Dashboard Streamlit avancé pour le pipeline Crypto Data.
5 onglets : Vue d'ensemble · Technique · Alertes & Volume · Streaming · Spark Analytics
"""

import sys, os
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
from sqlalchemy import create_engine

# ─── Config page ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Crypto Pipeline Dashboard",
    page_icon="₿",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── CSS Dark Pro ──────────────────────────────────────────────────────────────
st.markdown("""
<style>
/* ── Fond global ── */
.stApp { background-color: #0a0e1a; }
section[data-testid="stSidebar"] { background-color: #0d1117; border-right: 1px solid #1e2a3a; }

/* ── KPI Cards ── */
.kpi-card {
    background: linear-gradient(135deg, #0d1b2a 0%, #1a2744 100%);
    border: 1px solid #1e3a5f;
    border-radius: 12px;
    padding: 18px 20px;
    text-align: center;
    margin-bottom: 8px;
    transition: border-color 0.2s;
}
.kpi-card:hover { border-color: #3a7bd5; }
.kpi-label  { color: #8899aa; font-size: 0.78rem; text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 4px; }
.kpi-value  { color: #e8f0fe; font-size: 1.55rem; font-weight: 700; margin-bottom: 2px; }
.kpi-up     { color: #00d4aa; font-size: 0.92rem; font-weight: 600; }
.kpi-down   { color: #ff4757; font-size: 0.92rem; font-weight: 600; }
.kpi-flat   { color: #8899aa; font-size: 0.92rem; }

/* ── Alert badges ── */
.alert-up {
    background: rgba(0,212,170,0.12); border: 1px solid #00d4aa;
    border-radius: 8px; padding: 10px 14px; margin: 5px 0;
    color: #00d4aa; font-weight: 500;
}
.alert-down {
    background: rgba(255,71,87,0.12); border: 1px solid #ff4757;
    border-radius: 8px; padding: 10px 14px; margin: 5px 0;
    color: #ff4757; font-weight: 500;
}

/* ── Section titles ── */
.section-title {
    font-size: 1.1rem; font-weight: 700; color: #c8d8f0;
    border-left: 3px solid #3a7bd5; padding-left: 10px; margin-bottom: 14px;
}

/* ── Rank items ── */
.rank-item {
    display: flex; justify-content: space-between; align-items: center;
    background: #0d1b2a; border-radius: 8px; padding: 10px 14px; margin: 4px 0;
    border-left: 3px solid #1e3a5f;
}
.rank-badge {
    background: #1e3a5f; color: #8899aa; border-radius: 4px;
    padding: 2px 8px; font-size: 0.75rem; font-weight: 700;
}

/* ── Spark badge ── */
.spark-badge {
    background: linear-gradient(90deg, #e25822, #f5a623);
    color: white; border-radius: 6px; padding: 3px 10px;
    font-size: 0.75rem; font-weight: 700; display: inline-block; margin-bottom: 10px;
}

/* ── Tab overrides ── */
.stTabs [data-baseweb="tab-list"] { background-color: #0d1117; border-bottom: 1px solid #1e2a3a; gap: 2px; }
.stTabs [data-baseweb="tab"] { background-color: transparent; color: #8899aa; border-radius: 8px 8px 0 0; padding: 8px 18px; }
.stTabs [aria-selected="true"] { background-color: #1a2744 !important; color: #e8f0fe !important; }
</style>
""", unsafe_allow_html=True)

# ─── Connexion DB ──────────────────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import get_db_url

@st.cache_resource
def get_engine():
    return create_engine(get_db_url())

@st.cache_data(ttl=60)
def load_data(query: str) -> pd.DataFrame:
    try:
        return pd.read_sql(query, get_engine())
    except Exception as e:
        return pd.DataFrame()

# Couleurs par crypto
COLORS = {
    "bitcoin":     "#F7931A",
    "ethereum":    "#627EEA",
    "solana":      "#9945FF",
    "ripple":      "#00AAE4",
    "binancecoin": "#F3BA2F",
}
PLOTLY_LAYOUT = dict(
    plot_bgcolor="#0a0e1a",
    paper_bgcolor="#0a0e1a",
    font_color="#c8d8f0",
    xaxis=dict(gridcolor="#1e2a3a", zeroline=False),
    yaxis=dict(gridcolor="#1e2a3a", zeroline=False),
    legend=dict(bgcolor="#0d1b2a", bordercolor="#1e3a5f", borderwidth=1),
    margin=dict(l=10, r=10, t=40, b=10),
)


# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("## ₿ Crypto Dashboard")
    st.markdown("<hr style='border-color:#1e2a3a'>", unsafe_allow_html=True)

    all_coins = ["bitcoin", "ethereum", "ripple", "binancecoin", "solana"]
    selected_coins = st.multiselect("Cryptomonnaies", options=all_coins, default=all_coins)

    periode = st.selectbox(
        "Période d'analyse",
        ["Dernière heure", "6 dernières heures", "24 dernières heures", "7 derniers jours"],
        index=2
    )
    seuil = st.slider("Seuil alerte (%)", min_value=1, max_value=20, value=5)

    st.markdown("<hr style='border-color:#1e2a3a'>", unsafe_allow_html=True)

    auto_refresh = st.toggle("Auto-refresh (60s)", value=False)
    if st.button("🔄 Rafraîchir maintenant", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.markdown(
        f"<div style='color:#8899aa;font-size:0.75rem;margin-top:8px'>"
        f"Mis à jour : {datetime.now().strftime('%H:%M:%S')}</div>",
        unsafe_allow_html=True
    )

# Auto-refresh
if auto_refresh:
    import time
    time.sleep(60)
    st.cache_data.clear()
    st.rerun()


# ─── Filtre temporel ──────────────────────────────────────────────────────────
delta_map = {
    "Dernière heure":      timedelta(hours=1),
    "6 dernières heures":  timedelta(hours=6),
    "24 dernières heures": timedelta(hours=24),
    "7 derniers jours":    timedelta(days=7),
}
since     = datetime.utcnow() - delta_map[periode]
coins_sql = "','".join(selected_coins)


# ─── Chargement données ───────────────────────────────────────────────────────
raw_df = load_data(f"""
    SELECT * FROM raw_crypto_prices
    WHERE fetched_at >= '{since}' AND coin_id IN ('{coins_sql}')
    ORDER BY fetched_at ASC
""")
ranking_df = load_data("SELECT * FROM transform_daily_ranking ORDER BY rank ASC")
volume_df  = load_data("SELECT * FROM transform_daily_volume ORDER BY day DESC, total_volume DESC")
alerts_df  = load_data(f"""
    SELECT * FROM raw_crypto_prices
    WHERE ABS(price_change_pct_24h) >= {seuil}
    AND fetched_at >= '{datetime.utcnow() - timedelta(hours=24)}'
    ORDER BY ABS(price_change_pct_24h) DESC
""")
stream_df  = load_data("""
    SELECT coin_id, price, variation_pct, event_time
    FROM stream_crypto_prices ORDER BY event_time DESC LIMIT 200
""")
spark_vol_df  = load_data("SELECT * FROM spark_volatility ORDER BY volatility_pct DESC")
spark_dom_df  = load_data("SELECT * FROM spark_market_dominance ORDER BY dominance_pct DESC")


# ══════════════════════════════════════════════════════════════════════════════
# HEADER
# ══════════════════════════════════════════════════════════════════════════════
st.markdown(
    "<h1 style='color:#e8f0fe;font-size:1.9rem;margin-bottom:2px'>"
    "₿ Crypto Data Pipeline — Dashboard Pro</h1>"
    f"<p style='color:#8899aa;font-size:0.85rem;margin-top:0'>"
    f"Données quasi temps-réel · Période : <b style='color:#c8d8f0'>{periode}</b> · "
    f"{len(selected_coins)} crypto(s) sélectionnée(s)</p>",
    unsafe_allow_html=True
)

# ── KPI Cards ─────────────────────────────────────────────────────────────────
if not raw_df.empty:
    latest = raw_df.sort_values("fetched_at").groupby("coin_id").last().reset_index()
    kpi_cols = st.columns(len(latest))
    for i, (_, row) in enumerate(latest.iterrows()):
        chg = row.get("price_change_pct_24h", 0) or 0
        arrow = "▲" if chg > 0 else ("▼" if chg < 0 else "—")
        css_class = "kpi-up" if chg > 0 else ("kpi-down" if chg < 0 else "kpi-flat")
        coin_color = COLORS.get(row["coin_id"], "#8899aa")
        with kpi_cols[i]:
            st.markdown(f"""
            <div class="kpi-card" style="border-top: 3px solid {coin_color}">
                <div class="kpi-label">{row.get('name', row['coin_id'])}</div>
                <div class="kpi-value">${row['current_price']:,.2f}</div>
                <div class="{css_class}">{arrow} {chg:+.2f}% / 24h</div>
            </div>""", unsafe_allow_html=True)
else:
    st.warning("Aucune donnée disponible — lance d'abord le pipeline.")

st.markdown("<hr style='border-color:#1e2a3a;margin:8px 0 16px'>", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# ONGLETS
# ══════════════════════════════════════════════════════════════════════════════
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Vue d'ensemble",
    "📈 Analyse technique",
    "⚠️ Alertes & Volume",
    "⚡ Streaming Kafka",
    "🔥 Spark Analytics",
])


# ─────────────────────────────────────────────────────────────────────────────
# ONGLET 1 — Vue d'ensemble
# ─────────────────────────────────────────────────────────────────────────────
with tab1:
    col_left, col_right = st.columns([3, 1])

    with col_left:
        st.markdown("<div class='section-title'>Évolution des prix</div>", unsafe_allow_html=True)
        if not raw_df.empty:
            fig = go.Figure()
            for coin in selected_coins:
                d = raw_df[raw_df["coin_id"] == coin]
                if d.empty:
                    continue
                fig.add_trace(go.Scatter(
                    x=d["fetched_at"], y=d["current_price"],
                    name=coin.capitalize(),
                    line=dict(color=COLORS.get(coin, "#aaa"), width=2),
                    mode="lines",
                    hovertemplate="<b>%{y:$,.2f}</b><br>%{x}<extra>" + coin + "</extra>"
                ))
            fig.update_layout(
                **PLOTLY_LAYOUT,
                hovermode="x unified",
                height=360,
                title=dict(text=f"Prix — {periode}", font=dict(size=14, color="#8899aa")),
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Pas encore de données — relance le pipeline.")

    with col_right:
        st.markdown("<div class='section-title'>Classement du jour</div>", unsafe_allow_html=True)
        if not ranking_df.empty:
            today = str(datetime.now().date())
            today_rank = ranking_df[ranking_df["day"].astype(str) == today]
            if today_rank.empty:
                today_rank = ranking_df.head(5)
            for _, row in today_rank.head(5).iterrows():
                chg = row.get("price_change_pct_24h", 0) or 0
                color = "#00d4aa" if chg >= 0 else "#ff4757"
                coin_color = COLORS.get(row["coin_id"], "#8899aa")
                st.markdown(f"""
                <div class="rank-item" style="border-left-color:{coin_color}">
                    <span>
                        <span class="rank-badge">#{int(row['rank'])}</span>
                        <b style="color:#c8d8f0;margin-left:8px">{row['coin_id'].capitalize()}</b>
                    </span>
                    <span>
                        <span style="color:#8899aa;font-size:0.8rem">${row['current_price']:,.0f}</span>
                        <span style="color:{color};font-weight:700;margin-left:8px">{chg:+.2f}%</span>
                    </span>
                </div>""", unsafe_allow_html=True)
        else:
            st.info("Classement non disponible.")

    # Table résumé compacte
    st.markdown("<div class='section-title' style='margin-top:20px'>Résumé des dernières valeurs</div>", unsafe_allow_html=True)
    if not raw_df.empty:
        summary = raw_df.sort_values("fetched_at").groupby("coin_id").last().reset_index()
        summary = summary[["coin_id","name","current_price","price_change_pct_24h",
                            "total_volume","market_cap","high_24h","low_24h"]].copy()
        summary.columns = ["ID","Nom","Prix ($)","Var. 24h (%)","Volume","Mkt Cap","Haut 24h","Bas 24h"]
        st.dataframe(
            summary.style
                .format({"Prix ($)": "${:,.2f}", "Var. 24h (%)": "{:+.2f}%",
                         "Volume": "${:,.0f}", "Mkt Cap": "${:,.0f}",
                         "Haut 24h": "${:,.2f}", "Bas 24h": "${:,.2f}"})
                .applymap(lambda v: "color:#00d4aa" if isinstance(v, (int, float)) and v > 0
                          else ("color:#ff4757" if isinstance(v, (int, float)) and v < 0 else ""),
                          subset=["Var. 24h (%)"]),
            use_container_width=True, height=220
        )


# ─────────────────────────────────────────────────────────────────────────────
# ONGLET 2 — Analyse technique
# ─────────────────────────────────────────────────────────────────────────────
with tab2:
    st.markdown("<div class='section-title'>Graphique Chandelier (OHLC horaire)</div>", unsafe_allow_html=True)

    coin_choice = st.selectbox(
        "Crypto à analyser",
        options=selected_coins if selected_coins else all_coins,
        key="ohlc_coin"
    )

    if not raw_df.empty:
        # Calculer OHLC par heure depuis les données brutes
        ohlc_df = (
            raw_df[raw_df["coin_id"] == coin_choice]
            .copy()
            .assign(hour=lambda d: pd.to_datetime(d["fetched_at"]).dt.floor("h"))
            .groupby("hour")
            .agg(
                open =("current_price", "first"),
                high =("current_price", "max"),
                low  =("current_price", "min"),
                close=("current_price", "last"),
                volume=("total_volume", "mean"),
            )
            .reset_index()
        )

        if not ohlc_df.empty and len(ohlc_df) >= 2:
            fig_ohlc = make_subplots(
                rows=2, cols=1, shared_xaxes=True,
                row_heights=[0.75, 0.25], vertical_spacing=0.04
            )
            color = COLORS.get(coin_choice, "#aaa")
            fig_ohlc.add_trace(
                go.Candlestick(
                    x=ohlc_df["hour"], open=ohlc_df["open"],
                    high=ohlc_df["high"], low=ohlc_df["low"], close=ohlc_df["close"],
                    name=coin_choice.capitalize(),
                    increasing_line_color="#00d4aa", decreasing_line_color="#ff4757",
                    increasing_fillcolor="#00d4aa", decreasing_fillcolor="#ff4757",
                ),
                row=1, col=1
            )
            fig_ohlc.add_trace(
                go.Bar(
                    x=ohlc_df["hour"], y=ohlc_df["volume"],
                    name="Volume", marker_color=color, opacity=0.6
                ),
                row=2, col=1
            )
            fig_ohlc.update_layout(
                **PLOTLY_LAYOUT,
                height=460,
                xaxis_rangeslider_visible=False,
                title=dict(
                    text=f"Chandelier OHLC — {coin_choice.capitalize()} ({periode})",
                    font=dict(size=14, color="#8899aa")
                ),
            )
            fig_ohlc.update_yaxes(gridcolor="#1e2a3a", row=1, col=1)
            fig_ohlc.update_yaxes(gridcolor="#1e2a3a", row=2, col=1)
            st.plotly_chart(fig_ohlc, use_container_width=True)
        else:
            st.info("Pas assez de points de données pour construire un chandelier. Augmente la période.")

    st.markdown("<hr style='border-color:#1e2a3a;margin:16px 0'>", unsafe_allow_html=True)

    # Heatmap corrélation
    st.markdown("<div class='section-title'>Heatmap de corrélation entre cryptos</div>", unsafe_allow_html=True)
    st.caption("Mesure si les cryptos évoluent dans le même sens (1.0 = parfaitement corrélées, -1.0 = inversées)")

    if not raw_df.empty and len(selected_coins) >= 2:
        pivot = (
            raw_df[raw_df["coin_id"].isin(selected_coins)]
            .pivot_table(index="fetched_at", columns="coin_id", values="current_price")
            .ffill()
            .dropna()
        )
        if not pivot.empty and pivot.shape[1] >= 2:
            corr = pivot.pct_change().dropna().corr().round(2)
            labels = [c.capitalize() for c in corr.columns]
            fig_corr = go.Figure(go.Heatmap(
                z=corr.values, x=labels, y=labels,
                colorscale=[
                    [0,   "#ff4757"],
                    [0.5, "#1e2a3a"],
                    [1,   "#00d4aa"],
                ],
                zmin=-1, zmax=1,
                text=corr.values.round(2),
                texttemplate="%{text}",
                textfont=dict(size=13, color="white"),
                hoverongaps=False,
            ))
            fig_corr.update_layout(
                **PLOTLY_LAYOUT,
                height=350,
                title=dict(text="Corrélation des rendements", font=dict(size=14, color="#8899aa")),
            )
            st.plotly_chart(fig_corr, use_container_width=True)
        else:
            st.info("Pas assez de données communes pour calculer la corrélation.")
    else:
        st.info("Sélectionne au moins 2 cryptos dans la sidebar.")

    # Graphique performance relative (base 100)
    st.markdown("<div class='section-title'>Performance relative (base 100)</div>", unsafe_allow_html=True)
    st.caption("Toutes les cryptos ramenées à 100 au départ — permet de comparer les gains/pertes")

    if not raw_df.empty:
        fig_rel = go.Figure()
        for coin in selected_coins:
            d = raw_df[raw_df["coin_id"] == coin].sort_values("fetched_at")
            if d.empty or d["current_price"].iloc[0] == 0:
                continue
            base = d["current_price"].iloc[0]
            fig_rel.add_trace(go.Scatter(
                x=d["fetched_at"],
                y=(d["current_price"] / base * 100).round(2),
                name=coin.capitalize(),
                line=dict(color=COLORS.get(coin, "#aaa"), width=2),
                hovertemplate="<b>%{y:.1f}</b><extra>" + coin + "</extra>"
            ))
        fig_rel.add_hline(y=100, line_dash="dot", line_color="#8899aa", opacity=0.5)
        fig_rel.update_layout(
            **PLOTLY_LAYOUT, height=300, hovermode="x unified",
            title=dict(text="Indice de performance relative", font=dict(size=14, color="#8899aa")),
            yaxis_title="Indice (base 100)"
        )
        st.plotly_chart(fig_rel, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# ONGLET 3 — Alertes & Volume
# ─────────────────────────────────────────────────────────────────────────────
with tab3:
    col_a, col_b = st.columns([1, 1])

    with col_a:
        st.markdown(f"<div class='section-title'>Alertes de volatilité (seuil : ±{seuil}%)</div>", unsafe_allow_html=True)
        if not alerts_df.empty:
            latest_alerts = (
                alerts_df.sort_values("fetched_at")
                         .groupby("coin_id").last().reset_index()
            )
            for _, row in latest_alerts.iterrows():
                chg = row.get("price_change_pct_24h", 0) or 0
                css = "alert-up" if chg > 0 else "alert-down"
                icon = "▲" if chg > 0 else "▼"
                label = "HAUSSE FORTE" if chg > 0 else "BAISSE FORTE"
                st.markdown(
                    f"<div class='{css}'>"
                    f"{icon} <b>{row.get('name', row['coin_id'])}</b> — {label}<br>"
                    f"<span style='font-size:0.85rem'>{chg:+.2f}% &nbsp;|&nbsp; Prix : ${row['current_price']:,.2f}</span>"
                    f"</div>",
                    unsafe_allow_html=True
                )
        else:
            st.markdown(
                f"<div style='background:rgba(0,212,170,0.1);border:1px solid #00d4aa;"
                f"border-radius:8px;padding:12px;color:#00d4aa'>✅ Aucune alerte — "
                f"variations sous ±{seuil}%</div>",
                unsafe_allow_html=True
            )

        # Graphique variation 24h barres
        st.markdown("<br>", unsafe_allow_html=True)
        if not raw_df.empty:
            latest = raw_df.sort_values("fetched_at").groupby("coin_id").last().reset_index()
            latest = latest[latest["coin_id"].isin(selected_coins)]
            latest["color"] = latest["price_change_pct_24h"].apply(
                lambda x: "#00d4aa" if x >= 0 else "#ff4757"
            )
            fig_bar = go.Figure(go.Bar(
                x=latest["coin_id"].str.capitalize(),
                y=latest["price_change_pct_24h"],
                marker_color=latest["color"],
                text=latest["price_change_pct_24h"].apply(lambda x: f"{x:+.2f}%"),
                textposition="outside",
            ))
            fig_bar.add_hline(y=0, line_color="#8899aa", line_width=1)
            fig_bar.update_layout(
                **PLOTLY_LAYOUT, height=280,
                title=dict(text="Variation 24h (%)", font=dict(size=13, color="#8899aa")),
                yaxis_title="Variation (%)", showlegend=False
            )
            st.plotly_chart(fig_bar, use_container_width=True)

    with col_b:
        st.markdown("<div class='section-title'>Volume journalier échangé</div>", unsafe_allow_html=True)
        if not volume_df.empty:
            last_day = volume_df["day"].max() if "day" in volume_df.columns else None
            last_vol = volume_df[volume_df["day"] == last_day] if last_day else volume_df.head(5)
            last_vol = last_vol[last_vol["coin_id"].isin(selected_coins)]

            fig_vol = go.Figure(go.Bar(
                x=last_vol["coin_id"].str.capitalize(),
                y=last_vol["total_volume"],
                marker_color=[COLORS.get(c, "#aaa") for c in last_vol["coin_id"]],
                text=last_vol["total_volume"].apply(lambda x: f"${x/1e9:.2f}B" if x > 1e9 else f"${x/1e6:.1f}M"),
                textposition="outside",
            ))
            fig_vol.update_layout(
                **PLOTLY_LAYOUT, height=300,
                title=dict(text=f"Volume total — {last_day}", font=dict(size=13, color="#8899aa")),
                showlegend=False, yaxis_title="Volume (USD)"
            )
            st.plotly_chart(fig_vol, use_container_width=True)

            # Évolution volume dans le temps
            st.markdown("<div class='section-title'>Évolution du volume dans le temps</div>", unsafe_allow_html=True)
            vol_filtered = volume_df[volume_df["coin_id"].isin(selected_coins)]
            if not vol_filtered.empty:
                fig_vol2 = px.area(
                    vol_filtered, x="day", y="total_volume", color="coin_id",
                    color_discrete_map=COLORS,
                    labels={"day": "Date", "total_volume": "Volume (USD)", "coin_id": "Crypto"},
                )
                fig_vol2.update_layout(**PLOTLY_LAYOUT, height=260, showlegend=True)
                st.plotly_chart(fig_vol2, use_container_width=True)
        else:
            st.info("Volumes non disponibles.")


# ─────────────────────────────────────────────────────────────────────────────
# ONGLET 4 — Streaming Kafka
# ─────────────────────────────────────────────────────────────────────────────
with tab4:
    st.markdown("<div class='section-title'>Flux temps réel Kafka — derniers 200 événements</div>", unsafe_allow_html=True)

    if not stream_df.empty:
        stream_filtered = stream_df[stream_df["coin_id"].isin(selected_coins)]

        # Scatter prix en temps réel
        fig_stream = go.Figure()
        for coin in selected_coins:
            d = stream_filtered[stream_filtered["coin_id"] == coin].sort_values("event_time")
            if d.empty:
                continue
            fig_stream.add_trace(go.Scatter(
                x=d["event_time"], y=d["price"],
                name=coin.capitalize(), mode="lines+markers",
                line=dict(color=COLORS.get(coin, "#aaa"), width=1.5),
                marker=dict(size=4),
                hovertemplate="<b>$%{y:,.2f}</b><br>%{x}<extra>" + coin + "</extra>"
            ))
        fig_stream.update_layout(
            **PLOTLY_LAYOUT, height=340, hovermode="x unified",
            title=dict(text="Prix streaming (Kafka)", font=dict(size=14, color="#8899aa")),
        )
        st.plotly_chart(fig_stream, use_container_width=True)

        col_s1, col_s2 = st.columns(2)
        with col_s1:
            # Distribution des variations
            st.markdown("<div class='section-title'>Distribution des variations streaming</div>", unsafe_allow_html=True)
            fig_hist = go.Figure()
            for coin in selected_coins:
                d = stream_filtered[stream_filtered["coin_id"] == coin]
                if d.empty:
                    continue
                fig_hist.add_trace(go.Histogram(
                    x=d["variation_pct"], name=coin.capitalize(),
                    marker_color=COLORS.get(coin, "#aaa"), opacity=0.7,
                    nbinsx=20,
                ))
            fig_hist.update_layout(
                **PLOTLY_LAYOUT, height=280, barmode="overlay",
                title=dict(text="Distribution variation_pct (%)", font=dict(size=13, color="#8899aa")),
                xaxis_title="Variation (%)", yaxis_title="Fréquence"
            )
            st.plotly_chart(fig_hist, use_container_width=True)

        with col_s2:
            # Derniers événements
            st.markdown("<div class='section-title'>Derniers événements reçus</div>", unsafe_allow_html=True)
            st.dataframe(
                stream_filtered.head(20)[["coin_id","price","variation_pct","event_time"]]
                .rename(columns={"coin_id":"Crypto","price":"Prix ($)",
                                  "variation_pct":"Var. (%)","event_time":"Horodatage"})
                .style.format({"Prix ($)": "${:,.2f}", "Var. (%)": "{:+.2f}%"})
                .applymap(lambda v: "color:#00d4aa" if isinstance(v,(int,float)) and v > 0
                          else ("color:#ff4757" if isinstance(v,(int,float)) and v < 0 else ""),
                          subset=["Var. (%)"]),
                use_container_width=True, height=280
            )
    else:
        st.info("Aucun événement Kafka disponible — démarre le producteur Kafka.")
        st.code("python ingestion/kafka_producer.py", language="bash")


# ─────────────────────────────────────────────────────────────────────────────
# ONGLET 5 — Spark Analytics
# ─────────────────────────────────────────────────────────────────────────────
with tab5:
    st.markdown(
        "<div class='spark-badge'>⚡ PySpark — Analyses avancées</div>"
        "<p style='color:#8899aa;font-size:0.82rem'>"
        "Ces analyses sont calculées par le moteur Spark. "
        "Lance <code>python transformations/run_transforms.py --engine spark</code> pour les mettre à jour."
        "</p>",
        unsafe_allow_html=True
    )

    col_sp1, col_sp2 = st.columns(2)

    with col_sp1:
        # Volatilité
        st.markdown("<div class='section-title'>Volatilité des prix (écart-type)</div>", unsafe_allow_html=True)
        st.caption("Un écart-type élevé = crypto plus risquée mais potentiellement plus rentable")

        if not spark_vol_df.empty:
            spark_vol_filtered = spark_vol_df[spark_vol_df["coin_id"].isin(selected_coins)]
            fig_vol_spark = go.Figure(go.Bar(
                x=spark_vol_filtered["coin_id"].str.capitalize(),
                y=spark_vol_filtered["volatility_pct"],
                marker_color=[COLORS.get(c,"#aaa") for c in spark_vol_filtered["coin_id"]],
                text=spark_vol_filtered["volatility_pct"].apply(lambda x: f"{x:.2f}%"),
                textposition="outside",
            ))
            fig_vol_spark.update_layout(
                **PLOTLY_LAYOUT, height=300, showlegend=False,
                title=dict(text="Volatilité (%) calculée par Spark", font=dict(size=13, color="#8899aa")),
                yaxis_title="Volatilité (%)"
            )
            st.plotly_chart(fig_vol_spark, use_container_width=True)

            st.dataframe(
                spark_vol_df[["coin_id","name","price_avg","price_stddev","volatility_pct"]]
                .rename(columns={"coin_id":"ID","name":"Nom","price_avg":"Prix moyen",
                                  "price_stddev":"Écart-type","volatility_pct":"Volatilité (%)"})
                .style.format({"Prix moyen":"${:,.2f}", "Écart-type":"${:,.2f}", "Volatilité (%)":"{:.2f}%"})
                .background_gradient(subset=["Volatilité (%)"], cmap="RdYlGn_r"),
                use_container_width=True
            )
        else:
            st.info("Données de volatilité non disponibles — lance le pipeline Spark d'abord.")

    with col_sp2:
        # Dominance marché
        st.markdown("<div class='section-title'>Dominance du marché (market cap)</div>", unsafe_allow_html=True)
        st.caption("Part de capitalisation boursière de chaque crypto dans le portefeuille suivi")

        if not spark_dom_df.empty:
            spark_dom_filtered = spark_dom_df[spark_dom_df["coin_id"].isin(selected_coins)]
            fig_pie = go.Figure(go.Pie(
                labels=spark_dom_filtered["coin_id"].str.capitalize(),
                values=spark_dom_filtered["dominance_pct"],
                marker_colors=[COLORS.get(c,"#aaa") for c in spark_dom_filtered["coin_id"]],
                hole=0.45,
                textinfo="label+percent",
                hovertemplate="<b>%{label}</b><br>%{percent}<br>Part : %{value:.2f}%<extra></extra>",
            ))
            fig_pie.update_layout(
                **PLOTLY_LAYOUT, height=320,
                title=dict(text="Dominance marché (%)", font=dict(size=13, color="#8899aa")),
                legend=dict(orientation="v", x=1.05),
            )
            st.plotly_chart(fig_pie, use_container_width=True)
        else:
            st.info("Données de dominance non disponibles — lance le pipeline Spark d'abord.")

    # Données brutes (expander)
    with st.expander("📋 Données brutes complètes (raw_crypto_prices)"):
        if not raw_df.empty:
            cols_show = ["coin_id","name","current_price","price_change_pct_24h",
                         "total_volume","market_cap","high_24h","low_24h","fetched_at"]
            cols_show = [c for c in cols_show if c in raw_df.columns]
            st.dataframe(
                raw_df[cols_show].sort_values("fetched_at", ascending=False).head(100),
                use_container_width=True
            )
        else:
            st.info("Aucune donnée brute disponible.")


# ─── Footer ───────────────────────────────────────────────────────────────────
st.markdown(
    "<hr style='border-color:#1e2a3a;margin-top:30px'>"
    "<p style='text-align:center;color:#4a5568;font-size:0.75rem'>"
    "Crypto Data Pipeline v2.0 · Stack : Python · PostgreSQL · Kafka · PySpark · Streamlit · Plotly"
    "</p>",
    unsafe_allow_html=True
)
