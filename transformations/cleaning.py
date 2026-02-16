# transformations/cleaning.py
import pandas as pd
import logging

logger = logging.getLogger("cleaning")

def clean_raw_prices(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoie les données brutes :
    - Supprime les lignes avec valeurs nulles critiques
    - Supprime les doublons
    - Corrige les types de colonnes
    - Supprime les prix aberrants (<=0)
    """
    initial_rows = len(df)
    logger.info(f"Nettoyage : {initial_rows} lignes en entree")

    if df.empty:
        logger.warning("DataFrame vide recu")
        return df

    # 1. Supprimer les lignes sans coin_id ou sans prix
    df = df.dropna(subset=["coin_id", "current_price"])
    logger.info(f"Apres suppression nulls critiques : {len(df)} lignes")

    # 2. Supprimer les doublons (meme coin + meme timestamp)
    df = df.drop_duplicates(subset=["coin_id", "fetched_at"])
    logger.info(f"Apres suppression doublons : {len(df)} lignes")

    # 3. Corriger les types
    df["current_price"]        = pd.to_numeric(df["current_price"], errors="coerce")
    df["total_volume"]         = pd.to_numeric(df["total_volume"], errors="coerce").fillna(0)
    df["price_change_pct_24h"] = pd.to_numeric(df["price_change_pct_24h"], errors="coerce").fillna(0)
    df["fetched_at"]           = pd.to_datetime(df["fetched_at"], errors="coerce")

    # 4. Supprimer les prix aberrants (<= 0)
    before = len(df)
    df = df[df["current_price"] > 0]
    removed = before - len(df)
    if removed > 0:
        logger.warning(f"{removed} lignes supprimees (prix <= 0)")

    # 5. Remplir les nulls restants
    df["market_cap"]  = df["market_cap"].fillna(0)
    df["high_24h"]    = df["high_24h"].fillna(df["current_price"])
    df["low_24h"]     = df["low_24h"].fillna(df["current_price"])

    logger.info(f"Nettoyage termine : {len(df)}/{initial_rows} lignes conservees")
    return df.reset_index(drop=True)


def clean_stream_prices(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoie les données du stream Kafka.
    """
    if df.empty:
        return df

    df = df.dropna(subset=["coin_id", "price"])
    df = df.drop_duplicates(subset=["coin_id", "event_time"])
    df["price"]         = pd.to_numeric(df["price"], errors="coerce")
    df["variation_pct"] = pd.to_numeric(df["variation_pct"], errors="coerce").fillna(0)
    df["event_time"]    = pd.to_datetime(df["event_time"], errors="coerce")
    df = df[df["price"] > 0]

    return df.reset_index(drop=True)