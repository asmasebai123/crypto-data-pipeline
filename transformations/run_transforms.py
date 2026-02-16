# transformations/run_transforms.py
import sys
import os
import logging
import pandas as pd

# Accès au dossier ingestion pour database.py
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'ingestion'))
import psycopg2
from sqlalchemy import create_engine

from cleaning     import clean_raw_prices, clean_stream_prices
from aggregations import compute_hourly_average, compute_daily_volume
from rankings     import compute_daily_ranking, get_top_performers
from alerts       import detect_price_alerts, detect_stream_alerts

# ─── Logs ─────────────────────────────────────────────────────────
os.makedirs("../logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("../logs/transformations.log", encoding="utf-8")
    ]
)
logger = logging.getLogger("run_transforms")

# ─── Connexion DB ─────────────────────────────────────────────────
DB_URL = "postgresql://admin:password123@localhost:5432/crypto_db"

def get_engine():
    return create_engine(DB_URL)

def load_raw_prices() -> pd.DataFrame:
    engine = get_engine()
    query  = "SELECT * FROM raw_crypto_prices ORDER BY fetched_at DESC"
    df     = pd.read_sql(query, engine)
    logger.info(f"raw_crypto_prices charge : {len(df)} lignes")
    return df

def load_stream_prices() -> pd.DataFrame:
    engine = get_engine()
    query  = "SELECT * FROM stream_crypto_prices ORDER BY event_time DESC"
    df     = pd.read_sql(query, engine)
    logger.info(f"stream_crypto_prices charge : {len(df)} lignes")
    return df

def save_to_db(df: pd.DataFrame, table: str):
    if df.empty:
        logger.warning(f"Rien a sauvegarder dans {table}")
        return
    engine = get_engine()
    df.to_sql(table, engine, if_exists="replace",
              index=False, method="multi")
    logger.info(f"Sauvegarde OK : {len(df)} lignes dans {table}")


# ─── Pipeline principal ───────────────────────────────────────────
def run_all_transforms():
    logger.info("=== Debut pipeline transformations ===")

    # 1. Charger et nettoyer les données batch
    raw_df    = load_raw_prices()
    clean_df  = clean_raw_prices(raw_df)

    # 2. Charger et nettoyer le stream
    stream_df       = load_stream_prices()
    clean_stream_df = clean_stream_prices(stream_df)

    # 3. Moyenne horaire
    hourly_df = compute_hourly_average(clean_df)
    print("\n--- Moyenne horaire ---")
    print(hourly_df.to_string(index=False))
    save_to_db(hourly_df, "transform_hourly_avg")

    # 4. Volume journalier
    volume_df = compute_daily_volume(clean_df)
    print("\n--- Volume journalier ---")
    print(volume_df.to_string(index=False))
    save_to_db(volume_df, "transform_daily_volume")

    # 5. Classement journalier
    ranking_df = compute_daily_ranking(clean_df)
    print("\n--- Classement du jour ---")
    print(ranking_df.to_string(index=False))
    save_to_db(ranking_df, "transform_daily_ranking")

    # 6. Top performers
    top_df = get_top_performers(clean_df, top_n=3)
    print("\n--- Top 3 cryptos ---")
    print(top_df.to_string(index=False))

    # 7. Alertes batch
    alerts_df = detect_price_alerts(clean_df)
    print("\n--- Alertes de variation (batch) ---")
    if alerts_df.empty:
        print("  Aucune alerte detectee")
    else:
        print(alerts_df[["coin_id","price_change_pct_24h",
                          "alert_level","alert_message"]].to_string(index=False))
    save_to_db(alerts_df, "transform_alerts")

    # 8. Alertes stream
    stream_alerts = detect_stream_alerts(clean_stream_df)
    print("\n--- Alertes stream ---")
    if stream_alerts.empty:
        print("  Aucune alerte stream detectee")
    else:
        print(stream_alerts[["coin_id","price","variation_pct"]].to_string(index=False))

    logger.info("=== Pipeline transformations termine ===")


if __name__ == "__main__":
    run_all_transforms()