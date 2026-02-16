# transformations/aggregations.py
import pandas as pd
import logging

logger = logging.getLogger("aggregations")


def compute_hourly_average(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule la moyenne horaire des prix pour chaque crypto.
    Regroupe par coin_id + heure, calcule mean(current_price).
    """
    if df.empty:
        logger.warning("compute_hourly_average : DataFrame vide")
        return pd.DataFrame()

    df = df.copy()
    # Tronquer le timestamp à l'heure (ex: 15:37 -> 15:00)
    df["hour"] = df["fetched_at"].dt.floor("h")

    result = (
        df.groupby(["coin_id", "hour"])
        .agg(
            avg_price   = ("current_price", "mean"),
            min_price   = ("current_price", "min"),
            max_price   = ("current_price", "max"),
            nb_records  = ("current_price", "count")
        )
        .reset_index()
        .round({"avg_price": 4, "min_price": 4, "max_price": 4})
    )

    logger.info(f"Moyennes horaires calculees : {len(result)} lignes")
    return result


def compute_daily_volume(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule le volume total échangé par crypto par jour.
    """
    if df.empty:
        logger.warning("compute_daily_volume : DataFrame vide")
        return pd.DataFrame()

    df = df.copy()
    df["day"] = df["fetched_at"].dt.date

    result = (
        df.groupby(["coin_id", "day"])
        .agg(
            total_volume    = ("total_volume", "sum"),
            avg_volume      = ("total_volume", "mean"),
            avg_price       = ("current_price", "mean"),
            nb_records      = ("current_price", "count")
        )
        .reset_index()
        .sort_values(["day", "total_volume"], ascending=[True, False])
    )

    logger.info(f"Volumes journaliers calcules : {len(result)} lignes")
    return result