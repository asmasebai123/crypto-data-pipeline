# orchestration/flows.py
import sys
import os
import pandas as pd
from datetime import datetime, timedelta

# Chemins vers les autres modules
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(ROOT, "ingestion"))
sys.path.append(os.path.join(ROOT, "transformations"))

from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash

# Imports ingestion
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from main import fetch_crypto_prices
from database import insert_raw_prices, create_tables, get_connection

# Imports transformations
from cleaning import clean_raw_prices
from aggregations import compute_hourly_average, compute_daily_volume
from rankings import compute_daily_ranking
from alerts import detect_price_alerts

from sqlalchemy import create_engine

DB_URL = "postgresql://admin:password123@localhost:5432/crypto_db"

def get_engine():
    return create_engine(DB_URL)

def save_to_db(df: pd.DataFrame, table: str):
    if df is None or df.empty:
        return
    engine = get_engine()
    df.to_sql(table, engine, if_exists="replace",
              index=False, method="multi")


# ══════════════════════════════════════════════════════════════════
#  TACHES INDIVIDUELLES
# ══════════════════════════════════════════════════════════════════

@task(
    name="Creer tables PostgreSQL",
    retries=3,
    retry_delay_seconds=10
)
def task_create_tables():
    logger = get_run_logger()
    logger.info("Creation des tables PostgreSQL...")
    create_tables()
    logger.info("Tables prets")


@task(
    name="Recuperer prix CoinGecko",
    retries=3,
    retry_delay_seconds=60,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(minutes=9)
)
def task_fetch_prices():
    logger = get_run_logger()
    logger.info("Appel API CoinGecko...")
    data = fetch_crypto_prices(max_retries=3, wait_seconds=60)
    if not data:
        raise Exception("API CoinGecko n'a retourne aucune donnee")
    logger.info(f"{len(data)} cryptos recuperees")
    return data


@task(
    name="Sauvegarder prix bruts",
    retries=2,
    retry_delay_seconds=15
)
def task_save_raw_prices(data: list):
    logger = get_run_logger()
    if not data:
        logger.warning("Aucune donnee a sauvegarder")
        return 0
    nb = insert_raw_prices(data)
    logger.info(f"{nb} lignes inserees dans raw_crypto_prices")
    return nb


@task(
    name="Charger donnees brutes",
    retries=2,
    retry_delay_seconds=10
)
def task_load_raw_data():
    logger = get_run_logger()
    engine = get_engine()
    df = pd.read_sql(
        "SELECT * FROM raw_crypto_prices ORDER BY fetched_at DESC",
        engine
    )
    logger.info(f"{len(df)} lignes chargees depuis raw_crypto_prices")
    return df


@task(name="Nettoyer les donnees")
def task_clean_data(df: pd.DataFrame):
    logger = get_run_logger()
    clean_df = clean_raw_prices(df)
    logger.info(f"Nettoyage : {len(clean_df)} lignes apres nettoyage")
    return clean_df


@task(
    name="Calculer moyennes horaires",
    retries=2,
    retry_delay_seconds=10
)
def task_hourly_avg(df: pd.DataFrame):
    logger = get_run_logger()
    result = compute_hourly_average(df)
    save_to_db(result, "transform_hourly_avg")
    logger.info(f"Moyennes horaires : {len(result)} lignes sauvegardees")
    return result


@task(
    name="Calculer volumes journaliers",
    retries=2,
    retry_delay_seconds=10
)
def task_daily_volume(df: pd.DataFrame):
    logger = get_run_logger()
    result = compute_daily_volume(df)
    save_to_db(result, "transform_daily_volume")
    logger.info(f"Volumes journaliers : {len(result)} lignes sauvegardees")
    return result


@task(
    name="Calculer classement journalier",
    retries=2,
    retry_delay_seconds=10
)
def task_daily_ranking(df: pd.DataFrame):
    logger = get_run_logger()
    result = compute_daily_ranking(df)
    save_to_db(result, "transform_daily_ranking")
    logger.info(f"Classement : {len(result)} lignes sauvegardees")
    return result


@task(
    name="Detecter alertes de variation",
    retries=2,
    retry_delay_seconds=10
)
def task_detect_alerts(df: pd.DataFrame):
    logger = get_run_logger()
    alerts = detect_price_alerts(df, threshold=5.0)
    if alerts.empty:
        logger.info("Aucune alerte detectee")
    else:
        logger.warning(f"{len(alerts)} alerte(s) detectee(s) !")
        for _, row in alerts.iterrows():
            logger.warning(f"ALERTE : {row['alert_message']}")
        save_to_db(alerts, "transform_alerts")
    return alerts


@task(name="Rapport de synthese")
def task_summary_report(raw_count: int, hourly_df: pd.DataFrame,
                         ranking_df: pd.DataFrame, alerts_df: pd.DataFrame):
    logger = get_run_logger()
    logger.info("=" * 50)
    logger.info(f"RAPPORT PIPELINE - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    logger.info(f"  Donnees ingrees    : {raw_count} cryptos")
    logger.info(f"  Moyennes horaires  : {len(hourly_df)} entrees")
    logger.info(f"  Classement         : {len(ranking_df)} entrees")
    nb_alerts = 0 if alerts_df is None or alerts_df.empty else len(alerts_df)
    logger.info(f"  Alertes actives    : {nb_alerts}")
    logger.info("=" * 50)


# ══════════════════════════════════════════════════════════════════
#  FLOW PRINCIPAL
# ══════════════════════════════════════════════════════════════════

@flow(
    name="Pipeline Crypto Complet",
    description="Ingestion CoinGecko + Transformations + Alertes"
)
def crypto_pipeline_flow():
    logger = get_run_logger()
    logger.info("Demarrage du pipeline crypto complet")

    # Etape 1 : Preparer la base
    task_create_tables()

    # Etape 2 : Ingestion
    data = task_fetch_prices()
    raw_count = task_save_raw_prices(data)

    # Etape 3 : Charger + nettoyer
    raw_df = task_load_raw_data()
    clean_df = task_clean_data(raw_df)

    # Etape 4 : Transformations paralleles
    hourly_df = task_hourly_avg(clean_df)
    volume_df = task_daily_volume(clean_df)
    ranking_df = task_daily_ranking(clean_df)
    alerts_df = task_detect_alerts(clean_df)

    # Etape 5 : Rapport final
    task_summary_report(raw_count, hourly_df, ranking_df, alerts_df)

    logger.info("Pipeline termine avec succes")


# ══════════════════════════════════════════════════════════════════
#  FLOW INGESTION SEULE
# ══════════════════════════════════════════════════════════════════

@flow(name="Ingestion Batch Seulement")
def ingestion_only_flow():
    task_create_tables()
    data = task_fetch_prices()
    task_save_raw_prices(data)


# ══════════════════════════════════════════════════════════════════
#  FLOW TRANSFORMATIONS SEULES
# ══════════════════════════════════════════════════════════════════

@flow(name="Transformations Seulement")
def transformations_only_flow():
    raw_df = task_load_raw_data()
    clean_df = task_clean_data(raw_df)
    hourly_df = task_hourly_avg(clean_df)
    volume_df = task_daily_volume(clean_df)
    ranking_df = task_daily_ranking(clean_df)
    alerts_df = task_detect_alerts(clean_df)
    task_summary_report(
        len(raw_df), hourly_df, ranking_df, alerts_df
    )


if __name__ == "__main__":
    # Lancer le pipeline complet une fois pour tester
    crypto_pipeline_flow()