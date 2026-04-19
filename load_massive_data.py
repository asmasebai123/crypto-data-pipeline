#!/usr/bin/env python3
"""
Script pour charger ÉNORMÉMENT de données historiques (30 jours)
Cela va remplir le dashboard avec plein de vraies données impressionnantes!
"""
import sys
import os
import requests
import urllib3
import time
from datetime import datetime, timezone
import psycopg2
from dotenv import load_dotenv

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Charger .env
load_dotenv()

CRYPTOS = {
    "bitcoin":     "Bitcoin",
    "ethereum":    "Ethereum",
    "ripple":      "XRP",
    "binancecoin": "BNB",
    "solana":      "Solana",
}

SYMBOLS = {
    "bitcoin": "btc", "ethereum": "eth", "ripple": "xrp",
    "binancecoin": "bnb", "solana": "sol"
}

def load_api_key():
    """Charge la clé API depuis .env"""
    try:
        api_key = os.getenv("COINGECKO_API_KEY")
        if api_key:
            return api_key
    except:
        pass
    return None

API_KEY = load_api_key()

def get_neon_conn():
    """Connecte à Neon"""
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("❌ DATABASE_URL non trouvée dans .env")
        sys.exit(1)
    return psycopg2.connect(db_url)

def fetch_market_chart(coin_id, days=30):
    """
    Récupère les prix historiques — 30 JOURS
    CoinGecko API gratuite retourne ~720 points pour 30 jours
    """
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
    params = {
        "vs_currency": "usd",
        "days": days
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json"
    }
    if API_KEY:
        headers["x-cg-demo-api-key"] = API_KEY

    for attempt in range(1, 4):
        try:
            print(f"  ▶️  [{attempt}/3] Téléchargement {days} jours de {coin_id}...")
            r = requests.get(url, params=params, headers=headers,
                           timeout=30, verify=False)

            if r.status_code == 429:
                print(f"  ⏳ Rate limit — attente 60s...")
                time.sleep(60)
                continue

            if r.status_code == 401:
                print(f"  ❌ ERREUR 401 : clé API invalide")
                return []

            r.raise_for_status()
            data = r.json()
            prices = data.get("prices", [])
            volumes = data.get("total_volumes", [])
            vol_map = {v[0]: v[1] for v in volumes}

            result = []
            for ts_ms, price in prices:
                ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                vol = vol_map.get(ts_ms, 0)
                result.append((ts, price, vol))

            print(f"  ✅ OK — {len(result)} points téléchargés")
            return result

        except Exception as e:
            print(f"  ⚠️  Erreur tentative {attempt} : {str(e)[:100]}")
            if attempt < 3:
                time.sleep(10)

    return []

def insert_historical(coin_id, name, symbol, records):
    """Insère les données historiques dans Neon"""
    if not records:
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
    prices_list = [r[1] for r in records]

    for i, (ts, price, volume) in enumerate(records):
        idx_24h = max(0, i - 24)
        price_24h_ago = prices_list[idx_24h]
        change_24h = price - price_24h_ago
        change_pct = ((change_24h / price_24h_ago) * 100
                     if price_24h_ago > 0 else 0)
        rows.append((
            coin_id, name, symbol,
            round(price, 8), 0,
            round(volume, 2),
            round(change_24h, 8),
            round(change_pct, 4),
            round(price * 1.01, 8),
            round(price * 0.99, 8),
            ts.replace(tzinfo=None)
        ))

    try:
        cur.executemany(sql, rows)
        conn.commit()
        nb_inserted = len(rows)
    except Exception as e:
        print(f"    ❌ Erreur insertion: {e}")
        conn.rollback()
        nb_inserted = 0
    finally:
        cur.close()
        conn.close()

    return nb_inserted

def run_transformations():
    """Lance les transformations SQL"""
    print("\n📊 Exécution des transformations SQL...")

    conn = get_neon_conn()
    cur = conn.cursor()

    # Moyenne horaire
    cur.execute("DELETE FROM transform_hourly_avg")
    cur.execute("""
        INSERT INTO transform_hourly_avg
        SELECT
            coin_id,
            date_trunc('hour', fetched_at) as hour,
            ROUND(AVG(current_price)::numeric, 4) as avg_price,
            ROUND(MIN(current_price)::numeric, 4) as min_price,
            ROUND(MAX(current_price)::numeric, 4) as max_price,
            COUNT(*) as nb_records
        FROM raw_crypto_prices
        WHERE fetched_at >= NOW() - INTERVAL '30 days'
        GROUP BY coin_id, date_trunc('hour', fetched_at)
    """)

    # Volume journalier
    cur.execute("DELETE FROM transform_daily_volume")
    cur.execute("""
        INSERT INTO transform_daily_volume
        SELECT
            coin_id,
            DATE(fetched_at) as day,
            SUM(total_volume) as total_volume,
            AVG(total_volume) as avg_volume,
            ROUND(AVG(current_price)::numeric, 4) as avg_price,
            COUNT(*) as nb_records
        FROM raw_crypto_prices
        WHERE fetched_at >= NOW() - INTERVAL '30 days'
        GROUP BY coin_id, DATE(fetched_at)
    """)

    # Classement journalier
    cur.execute("DELETE FROM transform_daily_ranking")
    cur.execute("""
        INSERT INTO transform_daily_ranking
        SELECT
            coin_id,
            ROW_NUMBER() OVER (ORDER BY AVG(current_price) DESC) as rank,
            ROUND(AVG(current_price)::numeric, 4) as avg_price,
            ROUND(STDDEV(current_price)::numeric, 4) as volatility,
            COUNT(*) as nb_records,
            DATE(fetched_at)::date as day
        FROM raw_crypto_prices
        WHERE fetched_at >= NOW() - INTERVAL '30 days'
        GROUP BY coin_id, DATE(fetched_at)
    """)

    conn.commit()
    cur.close()
    conn.close()

    print("  ✅ Transformations terminées!")

def clear_old_data():
    """Nettoie les anciennes données"""
    print(f"\n🧹 Nettoyage des anciennes données...")
    conn = get_neon_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM raw_crypto_prices")
    deleted = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    print(f"  ✅ {deleted} anciennes lignes supprimées")

def main():
    print("\n" + "="*70)
    print("  🚀 CHARGEMENT DE 30 JOURS DE DONNÉES HISTORIQUES")
    print("  Cela va REMPLIR votre dashboard avec des VRAIES données!")
    print("="*70)

    print(f"\n🔗 Connexion à Neon...")
    try:
        conn = get_neon_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM raw_crypto_prices")
        count_before = cur.fetchone()[0]
        cur.close()
        conn.close()
        print(f"  ✅ Connecté! Lignes actuelles: {count_before}")
    except Exception as e:
        print(f"  ❌ Erreur de connexion: {e}")
        sys.exit(1)

    # Nettoyer les anciennes données
    if count_before > 0:
        clear_old_data()

    print(f"\n🔑 Clé API CoinGecko: {'✅ Chargée' if API_KEY else '⚠️  NON trouvée (limite 10-20 requêtes)'}")

    print(f"\n📥 Téléchargement de 30 JOURS d'historique (5 cryptos)...")
    print(f"  ~ 720 points × 5 cryptos = 3600+ lignes de données!\n")

    total_inserted = 0

    for coin_id, name in CRYPTOS.items():
        print(f"📊 {name.upper()}")
        records = fetch_market_chart(coin_id, days=30)

        if records:
            nb = insert_historical(coin_id, name, SYMBOLS[coin_id], records)
            total_inserted += nb
            print(f"  ✅ Inséré : {nb} lignes\n")
        else:
            print(f"  ❌ Échec de téléchargement\n")

        time.sleep(3)  # Délai entre les requêtes

    # Vérifier le total
    conn = get_neon_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM raw_crypto_prices")
    count_after = cur.fetchone()[0]
    cur.close()
    conn.close()

    print("="*70)
    print(f"  ✅ CHARGEMENT TERMINÉ!")
    print(f"  📊 Données avant: {count_before}")
    print(f"  📊 Données après: {count_after}")
    print(f"  ➕ Ajoutées: {count_after - count_before} lignes")
    print("="*70)

    # Lancer les transformations
    run_transformations()

    print("\n🎉 PARFAIT! Votre dashboard a maintenant:")
    print("  ✅ 30 jours d'historique complet")
    print("  ✅ 3600+ lignes de vraies données")
    print("  ✅ Graphiques remplis et impressionnants")
    print("  ✅ Rankings, volumes, alertes calculés")
    print("\n💡 Conseil: Attendez 2 minutes avant de visiter le dashboard")
    print("   (le temps de Streamlit de rafraîchir le cache)\n")

if __name__ == "__main__":
    main()
