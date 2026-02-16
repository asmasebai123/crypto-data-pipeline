# dashboard/app.py
import sys
import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from sqlalchemy import create_engine

# ─── Configuration page ───────────────────────────────────────────
st.set_page_config(
    page_title="Crypto Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ─── Connexion DB ─────────────────────────────────────────────────
DB_URL = "postgresql://admin:password123@localhost:5432/crypto_db"

@st.cache_resource
def get_engine():
    return create_engine(DB_URL)

@st.cache_data(ttl=60)  # Rafraichit toutes les 60 secondes
def load_data(query: str) -> pd.DataFrame:
    try:
        return pd.read_sql(query, get_engine())
    except Exception as e:
        st.error(f"Erreur base de donnees : {e}")
        return pd.DataFrame()


# ══════════════════════════════════════════════════════════════════
#  SIDEBAR
# ══════════════════════════════════════════════════════════════════

with st.sidebar:
    st.title("Crypto Dashboard")
    st.markdown("---")

    # Sélection des cryptos
    all_coins = ["bitcoin", "ethereum", "ripple", "binancecoin", "solana"]
    selected_coins = st.multiselect(
        "Cryptomonnaies",
        options=all_coins,
        default=all_coins
    )

    # Période d'analyse
    periode = st.selectbox(
        "Periode",
        options=["Derniere heure", "6 dernieres heures",
                 "24 dernieres heures", "7 derniers jours"],
        index=2
    )

    # Seuil d'alerte
    seuil = st.slider(
        "Seuil alerte (%)",
        min_value=1, max_value=20, value=5
    )

    st.markdown("---")
    st.caption(f"Mis a jour : {datetime.now().strftime('%H:%M:%S')}")

    # Bouton refresh
    if st.button("Rafraichir", use_container_width=True):
        st.cache_data.clear()
        st.rerun()


# ─── Calcul filtre temps ──────────────────────────────────────────
periode_map = {
    "Derniere heure":      timedelta(hours=1),
    "6 dernieres heures":  timedelta(hours=6),
    "24 dernieres heures": timedelta(hours=24),
    "7 derniers jours":    timedelta(days=7)
}
delta     = periode_map[periode]
since     = datetime.utcnow() - delta
coins_sql = "','".join(selected_coins)


# ══════════════════════════════════════════════════════════════════
#  CHARGEMENT DES DONNEES
# ══════════════════════════════════════════════════════════════════

raw_df = load_data(f"""
    SELECT * FROM raw_crypto_prices
    WHERE fetched_at >= '{since}'
    AND coin_id IN ('{coins_sql}')
    ORDER BY fetched_at DESC
""")

ranking_df = load_data("""
    SELECT * FROM transform_daily_ranking
    ORDER BY rank ASC
""")

volume_df = load_data("""
    SELECT * FROM transform_daily_volume
    ORDER BY day DESC, total_volume DESC
""")

alerts_df = load_data(f"""
    SELECT * FROM raw_crypto_prices
    WHERE ABS(price_change_pct_24h) >= {seuil}
    AND fetched_at >= '{datetime.utcnow() - timedelta(hours=24)}'
    ORDER BY ABS(price_change_pct_24h) DESC
""")


# ══════════════════════════════════════════════════════════════════
#  TITRE PRINCIPAL
# ══════════════════════════════════════════════════════════════════

st.title("Crypto Data Pipeline — Dashboard")
st.markdown(f"Donnees en temps quasi-reel · Periode : **{periode}**")
st.markdown("---")


# ══════════════════════════════════════════════════════════════════
#  SECTION 1 — KPIs en haut
# ══════════════════════════════════════════════════════════════════

st.subheader("Vue d'ensemble")

if not raw_df.empty:
    latest = (
        raw_df.sort_values("fetched_at")
              .groupby("coin_id")
              .last()
              .reset_index()
    )

    cols = st.columns(len(latest))
    for i, (_, row) in enumerate(latest.iterrows()):
        change = row.get("price_change_pct_24h", 0) or 0
        delta_color = "normal"
        with cols[i]:
            st.metric(
                label=row["name"],
                value=f"${row['current_price']:,.2f}",
                delta=f"{change:+.2f}%",
                delta_color=delta_color
            )
else:
    st.warning("Aucune donnee disponible — lance d'abord le pipeline.")

st.markdown("---")


# ══════════════════════════════════════════════════════════════════
#  SECTION 2 — Graphique évolution des prix
# ══════════════════════════════════════════════════════════════════

col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("Evolution des prix")

    if not raw_df.empty:
        fig = px.line(
            raw_df,
            x="fetched_at",
            y="current_price",
            color="coin_id",
            title=f"Prix par crypto — {periode}",
            labels={
                "fetched_at":    "Heure",
                "current_price": "Prix (USD)",
                "coin_id":       "Crypto"
            },
            color_discrete_map={
                "bitcoin":     "#F7931A",
                "ethereum":    "#627EEA",
                "solana":      "#9945FF",
                "ripple":      "#00AAE4",
                "binancecoin": "#F3BA2F"
            }
        )
        fig.update_layout(
            plot_bgcolor="#0E1117",
            paper_bgcolor="#0E1117",
            font_color="white",
            legend_title="Crypto",
            hovermode="x unified"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Pas encore de donnees — relance le pipeline.")


with col2:
    st.subheader("Top cryptos du jour")

    if not ranking_df.empty:
        today     = str(datetime.now().date())
        today_rank = ranking_df[
            ranking_df["day"].astype(str) == today
        ] if "day" in ranking_df.columns else ranking_df

        if today_rank.empty:
            today_rank = ranking_df.head(5)

        for _, row in today_rank.head(5).iterrows():
            change = row.get("price_change_pct_24h", 0) or 0
            emoji  = "🟢" if change >= 0 else "🔴"
            st.markdown(
                f"**#{int(row['rank'])} {row['coin_id'].capitalize()}**  "
                f"{emoji} `{change:+.2f}%`  "
                f"— ${row['current_price']:,.2f}"
            )
    else:
        st.info("Classement non disponible.")

st.markdown("---")


# ══════════════════════════════════════════════════════════════════
#  SECTION 3 — Alertes de volatilité
# ══════════════════════════════════════════════════════════════════

col3, col4 = st.columns([1, 1])

with col3:
    st.subheader(f"Alertes de volatilite (seuil : {seuil}%)")

    if not alerts_df.empty:
        latest_alerts = (
            alerts_df.sort_values("fetched_at")
                     .groupby("coin_id")
                     .last()
                     .reset_index()
        )
        for _, row in latest_alerts.iterrows():
            change = row.get("price_change_pct_24h", 0) or 0
            if change >= seuil:
                st.error(
                    f"HAUSSE FORTE — **{row['name']}** : "
                    f"`{change:+.2f}%` | Prix : ${row['current_price']:,.2f}"
                )
            elif change <= -seuil:
                st.warning(
                    f"BAISSE FORTE — **{row['name']}** : "
                    f"`{change:+.2f}%` | Prix : ${row['current_price']:,.2f}"
                )
    else:
        st.success(f"Aucune alerte — variations sous {seuil}%")


with col4:
    st.subheader("Volume journalier")

    if not volume_df.empty:
        last_day_vol = volume_df.copy()
        if "day" in last_day_vol.columns:
            last_day     = last_day_vol["day"].max()
            last_day_vol = last_day_vol[last_day_vol["day"] == last_day]

        fig_vol = px.bar(
            last_day_vol,
            x="coin_id",
            y="total_volume",
            title="Volume total echange (dernier jour)",
            labels={"coin_id": "Crypto", "total_volume": "Volume (USD)"},
            color="coin_id",
            color_discrete_map={
                "bitcoin":     "#F7931A",
                "ethereum":    "#627EEA",
                "solana":      "#9945FF",
                "ripple":      "#00AAE4",
                "binancecoin": "#F3BA2F"
            }
        )
        fig_vol.update_layout(
            plot_bgcolor="#0E1117",
            paper_bgcolor="#0E1117",
            font_color="white",
            showlegend=False
        )
        st.plotly_chart(fig_vol, use_container_width=True)
    else:
        st.info("Volumes non disponibles.")

st.markdown("---")


# ══════════════════════════════════════════════════════════════════
#  SECTION 4 — Tableau de données brutes
# ══════════════════════════════════════════════════════════════════

with st.expander("Donnees brutes (raw_crypto_prices)"):
    if not raw_df.empty:
        cols_show = ["coin_id", "name", "current_price",
                     "price_change_pct_24h", "total_volume", "fetched_at"]
        cols_show = [c for c in cols_show if c in raw_df.columns]
        st.dataframe(
            raw_df[cols_show].head(50),
            use_container_width=True
        )
    else:
        st.info("Aucune donnee brute disponible.")
        
# Données streaming temps réel
stream_df = load_data("""
    SELECT coin_id, price, variation_pct, event_time
    FROM stream_crypto_prices
    ORDER BY event_time DESC
    LIMIT 100
""")

st.subheader("Flux temps réel (Kafka)")
if not stream_df.empty:
    fig_stream = px.scatter(
        stream_df,
        x="event_time", y="price",
        color="coin_id",
        title="Prix en streaming (derniers 100 evenements)"
    )
    st.plotly_chart(fig_stream, use_container_width=True)