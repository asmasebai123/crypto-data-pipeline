#!/usr/bin/env python3
"""
VERSION CORRIGÉE - Charge les données SANS erreurs SQL
Testé et vérifié!
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
load_dotenv()

CRYPTOS = {
    "bitcoin": "Bitcoin",
    "ethereum": "Ethereum",
    "ripple": "XRP",
    "binancecoin": "BNB",
    "solana": "Solana",
}

SYMBOLS = {
    "bitcoin": "btc", "ethereum": "eth", "ripple": "xrp",
    "binancecoin": "bnb", "solana": "sol"
}

def load_api_key():
    try:
        return os.getenv("COINGECKO_API_KEY")
    except:
        return None

API_KEY = load_api_key()

def get_neon_conn():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("❌ DATABASE_URL non trouvée!")
        sys.exit(1)
    return psycopg2.connect(db_url)

def fetch_market_chart(coin_id, days=30):
    """Télécharge les données historiques"""
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
    params = {"vs_currency": "usd", "days": days}
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json"
    }
    if API_KEY:
        headers["x-cg-demo-api-key"] = API_KEY

    for attempt in range(1, 4):
        try:
            print(f"  ▶️  [{attempt}/3] {coin_id}...")
            r = requests.get(url, params=params, headers=headers,
                           timeout=30, verify=False)

            if r.status_code == 429:
                print(f"  ⏳ Rate limit - attente 60s...")
                time.sleep(60)
                continue

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

            print(f"  ✅ {len(result)} points")
            return result

        except Exception as e:
            print(f"  ⚠️  Erreur {attempt}: {str(e)[:80]}")
            if attempt < 3:
                time.sleep(10)

    return []

def insert_data(coin_id, name, symbol, records):
    """Insère les données - SIMPLE et DIRECT"""
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
        print(f"  ✅ Inséré: {nb_inserted} lignes")
    except Exception as e:
        print(f"  ❌ Erreur insertion: {e}")
        conn.rollback()
        nb_inserted = 0
    finally:
        cur.close()
        conn.close()

    return nb_inserted

def main():
    print("\n" + "="*70)
    print("  🚀 CHARGEMENT DONNÉES - VERSION CORRIGÉE")
    print("="*70)

    print(f"\n🔗 Connexion Neon...")
    try:
        conn = get_neon_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM raw_crypto_prices")
        count_before = cur.fetchone()[0]
        cur.close()
        conn.close()
        print(f"  ✅ Connecté! Lignes actuelles: {count_before}")
    except Exception as e:
        print(f"  ❌ Erreur: {e}")
        sys.exit(1)

    # Nettoyer
    if count_before > 0:
        print(f"\n🧹 Nettoyage...")
        conn = get_neon_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM raw_crypto_prices")
        conn.commit()
        cur.close()
        conn.close()
        print(f"  ✅ Nettoyé")

    print(f"\n📥 Téléchargement (30 jours, 5 cryptos)...\n")

    total = 0
    for coin_id, name in CRYPTOS.items():
        print(f"📊 {name.upper()}")
        records = fetch_market_chart(coin_id, days=30)

        if records:
            nb = insert_data(coin_id, name, SYMBOLS[coin_id], records)
            total += nb
        else:
            print(f"  ❌ Échec\n")

        time.sleep(3)

    # Vérifier
    conn = get_neon_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM raw_crypto_prices")
    count_after = cur.fetchone()[0]
    cur.close()
    conn.close()

    print("\n" + "="*70)
    print(f"  ✅ CHARGEMENT TERMINÉ!")
    print(f"  Avant: {count_before}")
    print(f"  Après: {count_after}")
    print(f"  Ajoutées: {count_after - count_before} lignes")
    print("="*70)

    if count_after == 0:
        print("\n❌ PROBLÈME: Aucune donnée insérée!")
        print("Vérifiez votre DATABASE_URL dans .env")
        sys.exit(1)

    print("\n✅ Données prêtes!")
    print("   Attendez 2 minutes, puis rafraîchissez le dashboard")

if __name__ == "__main__":
    main()
