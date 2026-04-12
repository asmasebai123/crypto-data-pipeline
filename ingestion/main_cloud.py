# ingestion/main_cloud.py
"""
Script qui alimente directement Neon (sans passer par PostgreSQL local).
À lancer en continu pour alimenter le dashboard.
"""
import sys
import os
import requests
import urllib3
import logging
import time
import psycopg2
from datetime import datetime

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ─── Configuration logs ───────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/cloud_ingestion.log", encoding="utf-8")
    ]
)
logger = logging.getLogger("cloud_ingestion")

# ─── URL Neon (remplace avec ta vraie URL) ───────────────────────
NEON_URL = "postgresql://neondb_owner:npg_bgGCEw9sofm6@ep-spring-paper-agif76oh-pooler.c-2.eu-central-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

# ─── Clé API CoinGecko ───────────────────────────────────────────
def load_api_key():
    try:
        with open(".env") as f:
            for line in f:
                if line.startswith("COINGECKO_API_KEY"):
                    return line.split("=", 1)[1].strip()
    except FileNotFoundError:
        pass
    return None

API_KEY = load_api_key()


def get_neon_conn():
    return psycopg2.connect(NEON_URL)


def fetch_crypto_prices():
    """Récupère les prix depuis CoinGecko."""
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "ids": "bitcoin,ethereum,ripple,binancecoin,solana",
        "order": "market_cap_desc",
        "per_page": 10,
        "page": 1,
        "sparkline": False
    }
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json"
    }
    if API_KEY:
        headers["x-cg-demo-api-key"] = API_KEY

    for attempt in range(1, 4):
        try:
            logger.info(f"Tentative {attempt}/3 - Appel API CoinGecko")
            r = requests.get(url, params=params, headers=headers,
                             timeout=30, verify=False)
            
            if r.status_code == 429:
                logger.warning("Rate limit - attente 60s...")
                time.sleep(60)
                continue
            
            r.raise_for_status()
            data = r.json()
            logger.info(f"OK - {len(data)} cryptos recuperees")
            return data
        
        except Exception as e:
            logger.error(f"Erreur tentative {attempt}: {e}")
            if attempt < 3:
                time.sleep(10)
    
    return []


def insert_to_neon(data):
    """Insère directement dans Neon."""
    if not data:
        return 0
    
    sql = """
    INSERT INTO raw_crypto_prices
        (coin_id, name, symbol, current_price, market_cap,
         total_volume, price_change_24h, price_change_pct_24h,
         high_24h, low_24h, fetched_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    conn = get_neon_conn()
    cur = conn.cursor()
    
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
    
    logger.info(f"OK - {len(rows)} lignes inserees dans Neon")
    return len(rows)


def run_transformations_on_neon():
    """Lance les transformations directement sur Neon."""
    logger.info("Lancement transformations...")
    
    conn = get_neon_conn()
    cur = conn.cursor()
    
    # Moyenne horaire
    cur.execute("""
        DELETE FROM transform_hourly_avg;
        INSERT INTO transform_hourly_avg
        SELECT 
            coin_id,
            date_trunc('hour', fetched_at) as hour,
            ROUND(AVG(current_price)::numeric, 4) as avg_price,
            ROUND(MIN(current_price)::numeric, 4) as min_price,
            ROUND(MAX(current_price)::numeric, 4) as max_price,
            COUNT(*) as nb_records
        FROM raw_crypto_prices
        WHERE fetched_at >= NOW() - INTERVAL '7 days'
        GROUP BY coin_id, date_trunc('hour', fetched_at);
    """)
    
    # Volume journalier
    cur.execute("""
        DELETE FROM transform_daily_volume;
        INSERT INTO transform_daily_volume
        SELECT 
            coin_id,
            DATE(fetched_at) as day,
            SUM(total_volume) as total_volume,
            AVG(total_volume) as avg_volume,
            ROUND(AVG(current_price)::numeric, 4) as avg_price,
            COUNT(*) as nb_records
        FROM raw_crypto_prices
        WHERE fetched_at >= NOW() - INTERVAL '7 days'
        GROUP BY coin_id, DATE(fetched_at);
    """)
    
    # Classement
    cur.execute("""
        DELETE FROM transform_daily_ranking;
        INSERT INTO transform_daily_ranking
        SELECT 
            day, rank, coin_id, name, current_price, price_change_pct_24h
        FROM (
            SELECT 
                DATE(fetched_at) as day,
                coin_id, name, current_price, price_change_pct_24h,
                RANK() OVER (PARTITION BY DATE(fetched_at) 
                             ORDER BY price_change_pct_24h DESC) as rank,
                ROW_NUMBER() OVER (PARTITION BY coin_id, DATE(fetched_at) 
                                   ORDER BY fetched_at DESC) as rn
            FROM raw_crypto_prices
            WHERE fetched_at >= NOW() - INTERVAL '7 days'
        ) sub
        WHERE rn = 1;
    """)
    
    # Alertes
    cur.execute("""
        DELETE FROM transform_alerts;
        INSERT INTO transform_alerts
        SELECT 
            DATE(fetched_at) as day,
            coin_id, name, current_price, price_change_pct_24h,
            CASE 
                WHEN price_change_pct_24h >= 5 THEN 'HAUSSE FORTE'
                WHEN price_change_pct_24h <= -5 THEN 'BAISSE FORTE'
            END as alert_level,
            coin_id || ' : ' || 
            CASE WHEN price_change_pct_24h >= 0 THEN '+' ELSE '' END ||
            ROUND(price_change_pct_24h::numeric, 2)::text || '% (' ||
            CASE 
                WHEN price_change_pct_24h >= 5 THEN 'HAUSSE FORTE'
                WHEN price_change_pct_24h <= -5 THEN 'BAISSE FORTE'
            END || ') - Prix: $' || 
            ROUND(current_price::numeric, 2)::text as alert_message
        FROM (
            SELECT coin_id, name, current_price, price_change_pct_24h, fetched_at,
                   ROW_NUMBER() OVER (PARTITION BY coin_id, DATE(fetched_at) 
                                      ORDER BY fetched_at DESC) as rn
            FROM raw_crypto_prices
            WHERE fetched_at >= NOW() - INTERVAL '24 hours'
              AND ABS(price_change_pct_24h) >= 5
        ) sub
        WHERE rn = 1;
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    
    logger.info("OK - Transformations terminees")


def run_pipeline_loop(interval_minutes=10):
    """Boucle principale."""
    logger.info("=" * 60)
    logger.info("  PIPELINE CLOUD - ALIMENTATION NEON")
    logger.info("=" * 60)
    logger.info(f"Intervalle : {interval_minutes} minutes")
    logger.info(f"Dashboard  : https://crypto-data-pipeline-8mrtpexxnwydrp9dbehvt5.streamlit.app")
    logger.info("")
    
    cycle = 0
    while True:
        cycle += 1
        start = datetime.now()
        
        logger.info(f"--- CYCLE {cycle} ---")
        
        # Ingestion
        data = fetch_crypto_prices()
        if data:
            insert_to_neon(data)
            run_transformations_on_neon()
        else:
            logger.warning("Aucune donnee recuperee")
        
        # Attendre jusqu'au prochain cycle
        elapsed = (datetime.now() - start).seconds
        wait = max(0, interval_minutes * 60 - elapsed)
        logger.info(f"Prochain cycle dans {wait // 60}m {wait % 60}s")
        logger.info("")
        time.sleep(wait)


if __name__ == "__main__":
    run_pipeline_loop(interval_minutes=10)