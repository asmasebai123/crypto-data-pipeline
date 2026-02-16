# transformations/rankings.py
import pandas as pd
import logging

logger = logging.getLogger("rankings")


def compute_daily_ranking(df: pd.DataFrame) -> pd.DataFrame:
    """
    Classe les cryptomonnaies par performance journalière
    (price_change_pct_24h) pour chaque jour.
    Rang 1 = meilleure performance.
    """
    if df.empty:
        logger.warning("compute_daily_ranking : DataFrame vide")
        return pd.DataFrame()

    df   = df.copy()
    df["day"] = df["fetched_at"].dt.date

    # Prendre la dernière valeur connue du jour pour chaque crypto
    latest = (
        df.sort_values("fetched_at")
          .groupby(["coin_id", "day"])
          .last()
          .reset_index()
    )

    # Classer par performance dans chaque jour
    latest["rank"] = (
        latest.groupby("day")["price_change_pct_24h"]
              .rank(ascending=False, method="dense")
              .astype(int)
    )

    result = (
        latest[["day", "rank", "coin_id", "name",
                "current_price", "price_change_pct_24h"]]
        .sort_values(["day", "rank"])
    )

    logger.info(f"Classement calcule : {len(result)} lignes")
    return result


def get_top_performers(df: pd.DataFrame, top_n: int = 3) -> pd.DataFrame:
    """
    Retourne les top N cryptos du jour (meilleures performances).
    """
    ranking = compute_daily_ranking(df)
    if ranking.empty:
        return pd.DataFrame()

    # Prendre le dernier jour disponible
    last_day = ranking["day"].max()
    top      = ranking[ranking["day"] == last_day].head(top_n)

    logger.info(f"Top {top_n} du {last_day} calcule")
    return top