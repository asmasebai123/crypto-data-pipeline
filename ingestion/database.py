# ingestion/database.py
import psycopg2
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# ─── Charger DATABASE_URL depuis .env ────────────────────────────
def load_database_url():
    import os

    # 1. Vérifier os.environ (variables d'environnement système)
    database_url = os.environ.get("DATABASE_URL")
    if database_url and database_url.strip():
        logger.info(f"DATABASE_URL trouvé dans os.environ")
        return database_url.strip()

    # 2. Charger depuis .env (même répertoire ou parent)
    env_paths = [".env", "../.env", "../../.env"]
    for env_file in env_paths:
        try:
            if not os.path.exists(env_file):
                continue
            with open(env_file, encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        if line.startswith("DATABASE_URL="):
                            value = line.split("=", 1)[1].strip()
                            if value:
                                logger.info(f"DATABASE_URL trouvé dans {env_file}")
                                return value
        except Exception as e:
            logger.debug(f"Erreur lecture {env_file}: {e}")

    logger.warning("DATABASE_URL non trouvé, utilisation localhost")
    return None

# ─── Connexion ────────────────────────────────────────────────────
def get_connection():
    import urllib.parse

    # Essayer d'abord Neon Cloud
    database_url = load_database_url()

    if database_url:
        # Format: postgresql://user:password@host:port/dbname
        try:
            parsed = urllib.parse.urlparse(database_url)
            logger.info(f"Connexion à Neon Cloud: {parsed.hostname}")
            return psycopg2.connect(
                host=parsed.hostname,
                port=parsed.port or 5432,
                dbname=parsed.path.lstrip('/'),
                user=parsed.username,
                password=parsed.password,
                sslmode='require'  # Neon Cloud requiert SSL
            )
        except Exception as e:
            logger.warning(f"Erreur connexion Neon: {e}, utilisation localhost")

    # Fallback: localhost (développement local)
    logger.info("Connexion à PostgreSQL local (localhost)")
    return psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="crypto_db",
        user="admin",
        password="password123"
    )

# ─── Création des tables ──────────────────────────────────────────
def create_tables():
    sql_raw = """
    CREATE TABLE IF NOT EXISTS raw_crypto_prices (
        id              SERIAL PRIMARY KEY,
        coin_id         VARCHAR(50)     NOT NULL,
        name            VARCHAR(100)    NOT NULL,
        symbol          VARCHAR(20)     NOT NULL,
        current_price   NUMERIC(20, 8)  NOT NULL,
        market_cap      NUMERIC(30, 2),
        total_volume    NUMERIC(30, 2),
        price_change_24h        NUMERIC(20, 8),
        price_change_pct_24h    NUMERIC(10, 4),
        high_24h        NUMERIC(20, 8),
        low_24h         NUMERIC(20, 8),
        fetched_at      TIMESTAMP       DEFAULT NOW()
    );
    """

    sql_stream = """
    CREATE TABLE IF NOT EXISTS stream_crypto_prices (
        id              SERIAL PRIMARY KEY,
        coin_id         VARCHAR(50)     NOT NULL,
        price           NUMERIC(20, 8)  NOT NULL,
        variation_pct   NUMERIC(10, 4),
        event_time      TIMESTAMP       NOT NULL,
        received_at     TIMESTAMP       DEFAULT NOW()
    );
    """

    try:
        conn = get_connection()
        cur  = conn.cursor()
        cur.execute(sql_raw)
        cur.execute(sql_stream)
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Tables créées avec succès")
        print(" Tables créées : raw_crypto_prices, stream_crypto_prices")
    except Exception as e:
        logger.error(f" Erreur création tables : {e}")
        raise


# ─── Insertion dans raw_crypto_prices ────────────────────────────
def insert_raw_prices(data: list):
    sql = """
    INSERT INTO raw_crypto_prices
        (coin_id, name, symbol, current_price, market_cap,
         total_volume, price_change_24h, price_change_pct_24h,
         high_24h, low_24h, fetched_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    try:
        conn = get_connection()
        cur  = conn.cursor()
        rows = []
        for coin in data:
            rows.append((
                coin.get('id'),
                coin.get('name'),
                coin.get('symbol'),
                coin.get('current_price'),
                coin.get('market_cap'),
                coin.get('total_volume'),
                coin.get('price_change_24h'),
                coin.get('price_change_percentage_24h'),
                coin.get('high_24h'),
                coin.get('low_24h'),
                datetime.utcnow()
            ))
        cur.executemany(sql, rows)
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f" {len(rows)} lignes insérées dans raw_crypto_prices")
        return len(rows)
    except Exception as e:
        logger.error(f" Erreur insertion raw : {e}")
        raise


# ─── Insertion dans stream_crypto_prices ─────────────────────────
def insert_stream_price(coin_id, price, variation_pct, event_time):
    sql = """
    INSERT INTO stream_crypto_prices
        (coin_id, price, variation_pct, event_time)
    VALUES (%s, %s, %s, %s)
    """
    try:
        conn = get_connection()
        cur  = conn.cursor()
        cur.execute(sql, (coin_id, price, variation_pct, event_time))
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f" Stream inséré : {coin_id} @ {price}")
    except Exception as e:
        logger.error(f" Erreur insertion stream : {e}")
        raise


if __name__ == "__main__":
    create_tables()