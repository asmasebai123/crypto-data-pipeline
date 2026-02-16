# transformations/alerts.py
import pandas as pd
import logging
from datetime import datetime

logger = logging.getLogger("alerts")

ALERT_THRESHOLD = 5.0   # seuil en %


def detect_price_alerts(df: pd.DataFrame,
                         threshold: float = ALERT_THRESHOLD) -> pd.DataFrame:
    """
    Détecte les cryptos dont la variation 24h dépasse ±threshold%.
    Retourne un DataFrame avec le niveau d'alerte.
    """
    if df.empty:
        logger.warning("detect_price_alerts : DataFrame vide")
        return pd.DataFrame()

    df = df.copy()
    df["day"] = df["fetched_at"].dt.date

    # Prendre la dernière valeur du jour
    latest = (
        df.sort_values("fetched_at")
          .groupby(["coin_id", "day"])
          .last()
          .reset_index()
    )

    # Filtrer les variations dépassant le seuil
    alerts = latest[
        latest["price_change_pct_24h"].abs() >= threshold
    ].copy()

    if alerts.empty:
        logger.info(f"Aucune alerte detectee (seuil : +-{threshold}%)")
        return pd.DataFrame()

    # Ajouter le niveau d'alerte
    alerts["alert_level"] = alerts["price_change_pct_24h"].apply(
        lambda x: "HAUSSE FORTE" if x >= threshold else "BAISSE FORTE"
    )
    alerts["alert_message"] = alerts.apply(
        lambda r: (
            f"{r['coin_id']} : {r['price_change_pct_24h']:+.2f}% "
            f"({r['alert_level']}) - Prix: ${r['current_price']:,.2f}"
        ),
        axis=1
    )

    result = alerts[[
        "day", "coin_id", "name", "current_price",
        "price_change_pct_24h", "alert_level", "alert_message"
    ]].sort_values("price_change_pct_24h", key=abs, ascending=False)

    logger.warning(f"{len(result)} alerte(s) detectee(s) !")
    for _, row in result.iterrows():
        logger.warning(f"ALERTE : {row['alert_message']}")

    return result


def detect_stream_alerts(df: pd.DataFrame,
                          threshold: float = ALERT_THRESHOLD) -> pd.DataFrame:
    """
    Détecte les alertes dans le flux streaming (variation_pct).
    """
    if df.empty:
        return pd.DataFrame()

    alerts = df[df["variation_pct"].abs() >= threshold].copy()

    if not alerts.empty:
        alerts["alert_level"] = alerts["variation_pct"].apply(
            lambda x: "HAUSSE FORTE" if x >= threshold else "BAISSE FORTE"
        )
        for _, row in alerts.iterrows():
            logger.warning(
                f"STREAM ALERTE : {row['coin_id']} "
                f"{row['variation_pct']:+.2f}% ({row['alert_level']})"
            )

    return alerts