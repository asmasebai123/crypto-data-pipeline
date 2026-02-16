# ingestion/database.py
import psycopg2
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# ─── Connexion ────────────────────────────────────────────────────
def get_connection():
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