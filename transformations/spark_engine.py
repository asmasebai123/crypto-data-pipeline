# transformations/spark_engine.py
"""
Gestionnaire de la SparkSession pour le pipeline crypto.

Justification de l'utilisation de Spark :
  - Architecture production-ready : si les données passent de 5K à 5M lignes
    (ajout de cryptos, fréquence minute, historique multi-années), Spark
    s'active sans réécriture du pipeline.
  - Window functions : classement et calculs glissants bien plus expressifs
    qu'un groupby Pandas.
  - Spark SQL : requêtes analytiques complexes en SQL standard sur les DataFrames.
  - Mode local[*] : tourne sans cluster, en local sur tous les cœurs disponibles.
"""

from pyspark.sql import SparkSession
import logging

logger = logging.getLogger("spark_engine")

_spark: SparkSession | None = None


def get_spark() -> SparkSession:
    """
    Retourne (ou crée) la SparkSession singleton.
    Configuration stable pour Windows + Python 3.12 + petits volumes.

    Stratégie Windows :
      - local[1] : un seul thread, évite les conflits de processus Python
      - Arrow désactivé : évite le crash du worker sur Pandas 2.x / Python 3.12
      - Les données sont passées via fichier CSV temporaire (pas de createDataFrame
        direct), donc les Python workers ne sont jamais sollicités pour sérialiser
        des données — c'est le JVM Spark qui lit directement le fichier.
    """
    global _spark
    if _spark is None:
        _spark = (
            SparkSession.builder
            .appName("CryptoDataPipeline")
            .master("local[1]")                              # 1 thread : stable sur Windows
            .config("spark.sql.shuffle.partitions", "2")     # petits volumes
            .config("spark.driver.memory", "1g")
            # ── Désactivations nécessaires sur Windows ──
            .config("spark.sql.execution.arrow.pyspark.enabled", "false")
            .config("spark.ui.enabled", "false")
            .config("spark.python.worker.faulthandler.enabled", "true")
            # ── Lecture fichiers locaux ──
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .getOrCreate()
        )
        _spark.sparkContext.setLogLevel("ERROR")
        logger.info("SparkSession démarrée (local[1], Windows-safe)")
    return _spark


def stop_spark() -> None:
    """Arrête la SparkSession proprement (utile en fin de script)."""
    global _spark
    if _spark is not None:
        _spark.stop()
        _spark = None
        logger.info("SparkSession arrêtée")


def auto_select_engine(nb_rows: int, threshold: int = 50_000) -> str:
    """
    Sélectionne automatiquement le moteur selon le volume de données.

    - Pandas  : rapide et léger sous le seuil (idéal pour notre projet)
    - Spark   : scalable au-dessus du seuil (production, big data)

    Args:
        nb_rows   : nombre de lignes dans le dataset
        threshold : seuil de basculement (défaut : 50 000 lignes)

    Returns:
        "spark" ou "pandas"
    """
    engine = "spark" if nb_rows >= threshold else "pandas"
    logger.info(
        f"Auto-sélection moteur : {nb_rows} lignes "
        f"({'≥' if nb_rows >= threshold else '<'} {threshold}) → {engine.upper()}"
    )
    return engine
