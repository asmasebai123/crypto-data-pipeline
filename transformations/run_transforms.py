# transformations/run_transforms.py
"""
Pipeline de transformations avec sélection automatique du moteur.

Modes disponibles :
  - AUTO   : Pandas si < 50 000 lignes, Spark au-delà (défaut)
  - PANDAS : force Pandas (développement local, petits volumes)
  - SPARK  : force Spark (démonstration, production, big data)

Utilisation :
  python run_transforms.py                  # mode AUTO
  python run_transforms.py --engine pandas  # force Pandas
  python run_transforms.py --engine spark   # force Spark
  USE_SPARK=true python run_transforms.py   # via variable d'environnement
"""

import sys
import os
import logging
import argparse
import pandas as pd

# Accès au dossier ingestion pour database.py
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'ingestion'))
from sqlalchemy import create_engine

# ── Imports Pandas (transformations originales) ────────────────────────────
from cleaning     import clean_raw_prices, clean_stream_prices
from aggregations import compute_hourly_average, compute_daily_volume
from rankings     import compute_daily_ranking, get_top_performers
from alerts       import detect_price_alerts, detect_stream_alerts

# ── Imports Spark ──────────────────────────────────────────────────────────
from spark_engine import auto_select_engine, stop_spark
from spark_transformations import (
    spark_clean_raw_prices,
    spark_compute_hourly_average,
    spark_compute_daily_volume,
    spark_compute_daily_ranking,
    spark_get_top_performers,
    spark_detect_price_alerts,
    spark_sql_advanced_analysis,
)

# ─── Logs ──────────────────────────────────────────────────────────────────
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

# ─── Connexion DB ──────────────────────────────────────────────────────────
DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://admin:password123@localhost:5432/crypto_db"
)

def get_engine():
    return create_engine(DB_URL)

def load_raw_prices() -> pd.DataFrame:
    engine = get_engine()
    query  = "SELECT * FROM raw_crypto_prices ORDER BY fetched_at DESC"
    df     = pd.read_sql(query, engine)
    logger.info(f"raw_crypto_prices chargé : {len(df)} lignes")
    return df

def load_stream_prices() -> pd.DataFrame:
    engine = get_engine()
    query  = "SELECT * FROM stream_crypto_prices ORDER BY event_time DESC"
    df     = pd.read_sql(query, engine)
    logger.info(f"stream_crypto_prices chargé : {len(df)} lignes")
    return df

def save_to_db(df: pd.DataFrame, table: str):
    if df.empty:
        logger.warning(f"Rien à sauvegarder dans {table}")
        return
    engine = get_engine()
    df.to_sql(table, engine, if_exists="replace", index=False, method="multi")
    logger.info(f"Sauvegarde OK : {len(df)} lignes dans {table}")


# ─── Résolution du moteur ──────────────────────────────────────────────────
def resolve_engine(mode: str, nb_rows: int) -> str:
    """
    Détermine le moteur à utiliser selon le mode et le volume.

    Args:
        mode    : "auto", "pandas" ou "spark"
        nb_rows : nombre de lignes du dataset principal

    Returns:
        "pandas" ou "spark"
    """
    if mode == "pandas":
        logger.info("Moteur forcé : PANDAS")
        return "pandas"
    if mode == "spark":
        logger.info("Moteur forcé : SPARK")
        return "spark"
    # mode "auto"
    return auto_select_engine(nb_rows)


# ─── Pipeline Pandas ────────────────────────────────────────────────────────
def run_pandas_pipeline(clean_df: pd.DataFrame,
                        clean_stream_df: pd.DataFrame) -> dict:
    """Pipeline de transformations complet avec Pandas."""
    logger.info(">>> Moteur : PANDAS")
    return {
        "hourly":   compute_hourly_average(clean_df),
        "volume":   compute_daily_volume(clean_df),
        "ranking":  compute_daily_ranking(clean_df),
        "top":      get_top_performers(clean_df, top_n=3),
        "alerts":   detect_price_alerts(clean_df),
        "s_alerts": detect_stream_alerts(clean_stream_df),
    }


# ─── Pipeline Spark ─────────────────────────────────────────────────────────
def run_spark_pipeline(clean_df: pd.DataFrame,
                       clean_stream_df: pd.DataFrame) -> dict:
    """
    Pipeline de transformations complet avec PySpark.
    Inclut les analyses Spark SQL avancées en bonus.
    """
    logger.info(">>> Moteur : SPARK (PySpark local[*])")

    results = {
        "hourly":   spark_compute_hourly_average(clean_df),
        "volume":   spark_compute_daily_volume(clean_df),
        "ranking":  spark_compute_daily_ranking(clean_df),
        "top":      spark_get_top_performers(clean_df, top_n=3),
        "alerts":   spark_detect_price_alerts(clean_df),
        "s_alerts": pd.DataFrame(),   # stream géré par Pandas (volume faible)
    }

    # Analyses Spark SQL avancées (volatilité, dominance marché, heure optimale)
    logger.info("Lancement des analyses Spark SQL avancées...")
    advanced = spark_sql_advanced_analysis(clean_df)
    results["spark_volatility"] = advanced.get("volatility",  pd.DataFrame())
    results["spark_dominance"]  = advanced.get("dominance",   pd.DataFrame())
    results["spark_best_hour"]  = advanced.get("best_hour",   pd.DataFrame())

    return results


# ─── Affichage des résultats ────────────────────────────────────────────────
def print_results(results: dict, engine: str):
    sep = "─" * 60

    print(f"\n{'═' * 60}")
    print(f"  RÉSULTATS DU PIPELINE  [moteur : {engine.upper()}]")
    print(f"{'═' * 60}")

    print(f"\n{sep}\nMoyenne horaire\n{sep}")
    print(results["hourly"].to_string(index=False) if not results["hourly"].empty else "  (vide)")

    print(f"\n{sep}\nVolume journalier\n{sep}")
    print(results["volume"].to_string(index=False) if not results["volume"].empty else "  (vide)")

    print(f"\n{sep}\nClassement journalier\n{sep}")
    print(results["ranking"].to_string(index=False) if not results["ranking"].empty else "  (vide)")

    print(f"\n{sep}\nTop 3 cryptos\n{sep}")
    print(results["top"].to_string(index=False) if not results["top"].empty else "  (vide)")

    print(f"\n{sep}\nAlertes de variation (batch)\n{sep}")
    if results["alerts"].empty:
        print("  Aucune alerte détectée")
    else:
        print(results["alerts"][
            ["coin_id", "price_change_pct_24h", "alert_level", "alert_message"]
        ].to_string(index=False))

    # Bonus Spark SQL
    if engine == "spark":
        print(f"\n{sep}\n[Spark SQL] Analyse de volatilité\n{sep}")
        if not results.get("spark_volatility", pd.DataFrame()).empty:
            print(results["spark_volatility"].to_string(index=False))

        print(f"\n{sep}\n[Spark SQL] Dominance marché (%)\n{sep}")
        if not results.get("spark_dominance", pd.DataFrame()).empty:
            print(results["spark_dominance"].to_string(index=False))

        print(f"\n{sep}\n[Spark SQL] Heure de prix moyen max par crypto\n{sep}")
        if not results.get("spark_best_hour", pd.DataFrame()).empty:
            print(results["spark_best_hour"].to_string(index=False))


# ─── Point d'entrée principal ───────────────────────────────────────────────
def run_all_transforms(mode: str = "auto"):
    logger.info(f"=== Début pipeline transformations (mode={mode}) ===")

    # 1. Charger les données
    raw_df    = load_raw_prices()
    stream_df = load_stream_prices()

    # 2. Résoudre le moteur à utiliser
    engine = resolve_engine(mode, len(raw_df))

    # 3. Nettoyage avec le moteur choisi
    if engine == "spark":
        clean_df        = spark_clean_raw_prices(raw_df)
        clean_stream_df = clean_stream_prices(stream_df)   # stream reste Pandas
    else:
        clean_df        = clean_raw_prices(raw_df)
        clean_stream_df = clean_stream_prices(stream_df)

    # 4. Transformations
    if engine == "spark":
        results = run_spark_pipeline(clean_df, clean_stream_df)
    else:
        results = run_pandas_pipeline(clean_df, clean_stream_df)

    # 5. Affichage
    print_results(results, engine)

    # 6. Sauvegarde en base (les résultats sont toujours en Pandas à ce stade)
    save_to_db(results["hourly"],  "transform_hourly_avg")
    save_to_db(results["volume"],  "transform_daily_volume")
    save_to_db(results["ranking"], "transform_daily_ranking")
    save_to_db(results["alerts"],  "transform_alerts")

    # Sauvegarde des analyses Spark SQL avancées (si disponibles)
    if engine == "spark":
        save_to_db(results.get("spark_volatility", pd.DataFrame()), "spark_volatility")
        save_to_db(results.get("spark_dominance",  pd.DataFrame()), "spark_market_dominance")

    # 7. Arrêter Spark proprement
    if engine == "spark":
        stop_spark()

    logger.info(f"=== Pipeline transformations terminé [moteur : {engine.upper()}] ===")


# ─── CLI ────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Pipeline transformations crypto — Pandas ou Spark"
    )
    parser.add_argument(
        "--engine",
        choices=["auto", "pandas", "spark"],
        default=os.getenv("ENGINE_MODE", "auto"),
        help=(
            "Moteur de traitement : "
            "auto (défaut, Pandas<50k/Spark≥50k), pandas, spark"
        )
    )
    args = parser.parse_args()
    run_all_transforms(mode=args.engine)
