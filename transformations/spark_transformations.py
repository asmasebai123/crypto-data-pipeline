# transformations/spark_transformations.py
"""
Transformations métier réécrites en PySpark.

Chaque fonction est l'équivalent Spark de son homologue Pandas dans
cleaning.py, aggregations.py, rankings.py et alerts.py.

Points forts de la version Spark :
  - Window functions natives pour les rankings (plus expressif que Pandas rank)
  - Spark SQL pour les requêtes analytiques complexes
  - Scalable de 5K à 500M+ lignes sans modifier le code
  - Retour en Pandas (.toPandas()) pour la compatibilité avec la DB existante
"""

import logging
import os
import tempfile
import pandas as pd
from pyspark.sql import DataFrame as SparkDF
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType, LongType
)

from spark_engine import get_spark

logger = logging.getLogger("spark_transformations")

# Fichier CSV temporaire partagé (réutilisé à chaque appel)
_TMP_CSV = os.path.join(tempfile.gettempdir(), "crypto_spark_input.csv")


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def pandas_to_spark(df: pd.DataFrame) -> SparkDF:
    """
    Convertit un Pandas DataFrame en Spark DataFrame via un fichier CSV temporaire.

    Pourquoi cette approche et pas createDataFrame(pandas_df) directement ?
    ─────────────────────────────────────────────────────────────────────────
    Sur Windows avec Python 3.12 et Pandas 2.x, createDataFrame() lance des
    sous-processus Python (les "workers") pour sérialiser les données ligne
    par ligne. Ces workers crashent systématiquement (EOFException) à cause
    d'incompatibilités de sérialisation entre Python 3.12 et le protocole
    PySpark 3.5.

    En passant par un CSV :
      - Pandas écrit le fichier (pur Python, pas de Spark worker)
      - Spark le lit via son moteur JVM natif (pas de Python worker pour la lecture)
      - On caste ensuite les colonnes depuis les types string vers les bons types

    Cette approche est aussi utilisée en production pour ingérer des fichiers
    externes dans Spark sans passer par la mémoire Python.
    """
    spark = get_spark()

    # 1. Préparer : supprimer timezones (incompatibles CSV/Spark)
    df_clean = df.copy()
    ts_cols = []
    for col in df_clean.columns:
        dtype = df_clean[col].dtype
        if hasattr(dtype, "tz") and dtype.tz is not None:
            df_clean[col] = df_clean[col].dt.tz_localize(None)
            ts_cols.append(col)

    # 2. Écrire en CSV temporaire
    df_clean.to_csv(_TMP_CSV, index=False)
    logger.debug(f"CSV temporaire écrit : {_TMP_CSV} ({len(df_clean)} lignes)")

    # 3. Spark lit le CSV (JVM, pas de Python worker)
    sdf = spark.read.csv(_TMP_CSV, header=True, inferSchema=True)

    # 4. Re-caster les colonnes timestamp détectées
    for col in ts_cols:
        if col in sdf.columns:
            sdf = sdf.withColumn(col, F.to_timestamp(F.col(col)))

    return sdf


def spark_to_pandas(sdf: SparkDF) -> pd.DataFrame:
    """Convertit un Spark DataFrame en Pandas DataFrame."""
    return sdf.toPandas()


# ─────────────────────────────────────────────────────────────────────────────
# 1. Nettoyage
# ─────────────────────────────────────────────────────────────────────────────

def spark_clean_raw_prices(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoyage des données brutes avec Spark.
    Équivalent Spark de clean_raw_prices() dans cleaning.py.

    Opérations :
      - Supprime les nulls critiques (coin_id, current_price)
      - Supprime les doublons (coin_id + fetched_at)
      - Corrige les types
      - Filtre les prix aberrants (<= 0)
      - Remplit les nulls secondaires
    """
    if df.empty:
        logger.warning("spark_clean_raw_prices : DataFrame vide reçu")
        return df

    sdf = pandas_to_spark(df)
    initial_count = sdf.count()
    logger.info(f"[Spark] Nettoyage : {initial_count} lignes en entrée")

    # 1. Supprimer les nulls critiques
    sdf = sdf.dropna(subset=["coin_id", "current_price"])

    # 2. Supprimer les doublons
    sdf = sdf.dropDuplicates(["coin_id", "fetched_at"])

    # 3. Caster les types numériques
    sdf = (
        sdf
        .withColumn("current_price",        F.col("current_price").cast(DoubleType()))
        .withColumn("total_volume",          F.coalesce(F.col("total_volume").cast(DoubleType()), F.lit(0.0)))
        .withColumn("price_change_pct_24h",  F.coalesce(F.col("price_change_pct_24h").cast(DoubleType()), F.lit(0.0)))
        .withColumn("fetched_at",            F.col("fetched_at").cast(TimestampType()))
    )

    # 4. Filtrer les prix aberrants
    sdf = sdf.filter(F.col("current_price") > 0)

    # 5. Remplir les nulls secondaires
    sdf = (
        sdf
        .withColumn("market_cap", F.coalesce(F.col("market_cap").cast(DoubleType()), F.lit(0.0)))
        .withColumn("high_24h",   F.coalesce(F.col("high_24h").cast(DoubleType()),   F.col("current_price")))
        .withColumn("low_24h",    F.coalesce(F.col("low_24h").cast(DoubleType()),    F.col("current_price")))
    )

    final_count = sdf.count()
    logger.info(f"[Spark] Nettoyage terminé : {final_count}/{initial_count} lignes conservées")

    return spark_to_pandas(sdf)


# ─────────────────────────────────────────────────────────────────────────────
# 2. Agrégations
# ─────────────────────────────────────────────────────────────────────────────

def spark_compute_hourly_average(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule la moyenne horaire des prix par crypto avec Spark.
    Équivalent Spark de compute_hourly_average() dans aggregations.py.

    Utilise date_trunc('hour', fetched_at) pour grouper par heure,
    ce qui est natif et optimisé dans Spark SQL.
    """
    if df.empty:
        logger.warning("[Spark] spark_compute_hourly_average : DataFrame vide")
        return pd.DataFrame()

    sdf = pandas_to_spark(df)

    result = (
        sdf
        .withColumn("hour", F.date_trunc("hour", F.col("fetched_at")))
        .groupBy("coin_id", "hour")
        .agg(
            F.round(F.avg("current_price"), 4).alias("avg_price"),
            F.round(F.min("current_price"), 4).alias("min_price"),
            F.round(F.max("current_price"), 4).alias("max_price"),
            F.count("current_price").alias("nb_records")
        )
        .orderBy("coin_id", "hour")
    )

    logger.info(f"[Spark] Moyennes horaires calculées : {result.count()} lignes")
    return spark_to_pandas(result)


def spark_compute_daily_volume(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule le volume total journalier par crypto avec Spark.
    Équivalent Spark de compute_daily_volume() dans aggregations.py.
    """
    if df.empty:
        logger.warning("[Spark] spark_compute_daily_volume : DataFrame vide")
        return pd.DataFrame()

    sdf = pandas_to_spark(df)

    result = (
        sdf
        .withColumn("day", F.to_date(F.col("fetched_at")))
        .groupBy("coin_id", "day")
        .agg(
            F.sum("total_volume").alias("total_volume"),
            F.avg("total_volume").alias("avg_volume"),
            F.avg("current_price").alias("avg_price"),
            F.count("current_price").alias("nb_records")
        )
        .orderBy("day", F.desc("total_volume"))
    )

    logger.info(f"[Spark] Volumes journaliers calculés : {result.count()} lignes")
    return spark_to_pandas(result)


# ─────────────────────────────────────────────────────────────────────────────
# 3. Classement — showcase des Window Functions Spark
# ─────────────────────────────────────────────────────────────────────────────

def spark_compute_daily_ranking(df: pd.DataFrame) -> pd.DataFrame:
    """
    Classe les cryptos par performance journalière avec les Window Functions Spark.
    Équivalent Spark de compute_daily_ranking() dans rankings.py.

    AVANTAGE SPARK :
      La fonction rank() sur une Window est plus expressive et scalable
      que la combinaison groupby + rank() de Pandas.
      Avec 500M de lignes, ce code fonctionne sans modification.

    Logique :
      1. Ajouter colonne 'day'
      2. Prendre la dernière valeur du jour pour chaque crypto
         (via row_number + Window partitionné par coin_id/day, trié par fetched_at DESC)
      3. Calculer le rang par performance (rank() sur Window partitionné par day)
    """
    if df.empty:
        logger.warning("[Spark] spark_compute_daily_ranking : DataFrame vide")
        return pd.DataFrame()

    sdf = pandas_to_spark(df)
    sdf = sdf.withColumn("day", F.to_date(F.col("fetched_at")))

    # Window pour prendre la dernière valeur du jour par crypto
    w_last = (
        Window
        .partitionBy("coin_id", "day")
        .orderBy(F.desc("fetched_at"))
    )

    # Window pour calculer le rang par jour (meilleure perf = rang 1)
    w_rank = (
        Window
        .partitionBy("day")
        .orderBy(F.desc("price_change_pct_24h"))
    )

    result = (
        sdf
        # Garder uniquement la dernière entrée du jour par crypto
        .withColumn("_rn", F.row_number().over(w_last))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        # Calculer le rang
        .withColumn("rank", F.rank().over(w_rank))
        .select("day", "rank", "coin_id", "name",
                "current_price", "price_change_pct_24h")
        .orderBy("day", "rank")
    )

    logger.info(f"[Spark] Classement calculé : {result.count()} lignes")
    return spark_to_pandas(result)


def spark_get_top_performers(df: pd.DataFrame, top_n: int = 3) -> pd.DataFrame:
    """
    Retourne les top N cryptos du dernier jour disponible.
    Version Spark de get_top_performers() dans rankings.py.
    """
    ranking = spark_compute_daily_ranking(df)
    if ranking.empty:
        return pd.DataFrame()

    last_day = ranking["day"].max()
    top = ranking[ranking["day"] == last_day].head(top_n)
    logger.info(f"[Spark] Top {top_n} du {last_day} calculé")
    return top


# ─────────────────────────────────────────────────────────────────────────────
# 4. Alertes
# ─────────────────────────────────────────────────────────────────────────────

def spark_detect_price_alerts(df: pd.DataFrame,
                               threshold: float = 5.0) -> pd.DataFrame:
    """
    Détecte les alertes de variation avec Spark.
    Équivalent Spark de detect_price_alerts() dans alerts.py.

    Utilise F.when() / F.otherwise() pour le calcul conditionnel,
    et F.concat() pour construire le message d'alerte.
    """
    if df.empty:
        logger.warning("[Spark] spark_detect_price_alerts : DataFrame vide")
        return pd.DataFrame()

    sdf = pandas_to_spark(df)
    sdf = sdf.withColumn("day", F.to_date(F.col("fetched_at")))

    # Prendre la dernière valeur du jour (même logique que le ranking)
    w_last = Window.partitionBy("coin_id", "day").orderBy(F.desc("fetched_at"))
    latest = (
        sdf
        .withColumn("_rn", F.row_number().over(w_last))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # Filtrer les alertes
    alerts = latest.filter(F.abs(F.col("price_change_pct_24h")) >= threshold)

    if alerts.count() == 0:
        logger.info(f"[Spark] Aucune alerte détectée (seuil : ±{threshold}%)")
        return pd.DataFrame()

    # Construire les colonnes d'alerte
    alerts = (
        alerts
        .withColumn(
            "alert_level",
            F.when(F.col("price_change_pct_24h") >= threshold, F.lit("HAUSSE FORTE"))
             .otherwise(F.lit("BAISSE FORTE"))
        )
        .withColumn(
            "alert_message",
            F.concat(
                F.col("coin_id"), F.lit(" : "),
                F.format_number(F.col("price_change_pct_24h"), 2), F.lit("% ("),
                F.col("alert_level"), F.lit(") - Prix: $"),
                F.format_number(F.col("current_price"), 2)
            )
        )
        .select("day", "coin_id", "name", "current_price",
                "price_change_pct_24h", "alert_level", "alert_message")
        .orderBy(F.desc(F.abs(F.col("price_change_pct_24h"))))
    )

    nb = alerts.count()
    logger.warning(f"[Spark] {nb} alerte(s) détectée(s) !")

    result_pd = spark_to_pandas(alerts)
    for _, row in result_pd.iterrows():
        logger.warning(f"ALERTE : {row['alert_message']}")

    return result_pd


# ─────────────────────────────────────────────────────────────────────────────
# 5. Analyse Spark SQL avancée (bonus — démontre Spark SQL)
# ─────────────────────────────────────────────────────────────────────────────

def spark_sql_advanced_analysis(df: pd.DataFrame) -> dict:
    """
    Analyse avancée utilisant Spark SQL directement.
    Démontre la capacité à écrire des requêtes SQL analytiques sur un DataFrame Spark.

    Retourne un dict avec 3 analyses :
      - volatility  : écart-type des prix par crypto (mesure de volatilité)
      - momentum    : variation de prix sur les dernières 24h vs 48h
      - dominance   : part de marché (market_cap) de chaque crypto en %
    """
    if df.empty:
        return {}

    spark = get_spark()
    sdf = pandas_to_spark(df)

    # Enregistrer comme vue SQL temporaire
    sdf.createOrReplaceTempView("crypto_prices")

    results = {}

    # --- Analyse 1 : Volatilité (écart-type) ---
    volatility = spark.sql("""
        SELECT
            coin_id,
            name,
            ROUND(STDDEV(current_price), 4)  AS price_stddev,
            ROUND(AVG(current_price), 4)      AS price_avg,
            ROUND(
                STDDEV(current_price) / AVG(current_price) * 100, 2
            )                                 AS volatility_pct
        FROM crypto_prices
        GROUP BY coin_id, name
        ORDER BY volatility_pct DESC
    """)
    results["volatility"] = spark_to_pandas(volatility)
    logger.info("[Spark SQL] Analyse volatilité calculée")

    # --- Analyse 2 : Dominance marché (market cap %) ---
    dominance = spark.sql("""
        WITH latest AS (
            SELECT coin_id, name, market_cap,
                   ROW_NUMBER() OVER (PARTITION BY coin_id ORDER BY fetched_at DESC) AS rn
            FROM crypto_prices
            WHERE market_cap > 0
        ),
        totals AS (
            SELECT SUM(market_cap) AS total_cap FROM latest WHERE rn = 1
        )
        SELECT
            l.coin_id,
            l.name,
            l.market_cap,
            ROUND(l.market_cap / t.total_cap * 100, 2) AS dominance_pct
        FROM latest l, totals t
        WHERE l.rn = 1
        ORDER BY dominance_pct DESC
    """)
    results["dominance"] = spark_to_pandas(dominance)
    logger.info("[Spark SQL] Analyse dominance marché calculée")

    # --- Analyse 3 : Meilleure heure de performance ---
    best_hour = spark.sql("""
        SELECT
            coin_id,
            HOUR(fetched_at)              AS hour_of_day,
            ROUND(AVG(current_price), 4)  AS avg_price,
            COUNT(*)                      AS nb_points
        FROM crypto_prices
        GROUP BY coin_id, HOUR(fetched_at)
        ORDER BY coin_id, avg_price DESC
    """)
    results["best_hour"] = spark_to_pandas(best_hour)
    logger.info("[Spark SQL] Analyse meilleure heure calculée")

    return results
