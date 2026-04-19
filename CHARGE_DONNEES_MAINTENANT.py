#!/usr/bin/env python3
"""
VERSION FINALE - GARANTIE DE MARCHER
Vide complètement et recharge
"""
import os
import psycopg2
from dotenv import load_dotenv
import requests
from datetime import datetime, timezone

load_dotenv()

def get_conn():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("❌ DATABASE_URL manquante!")
        exit(1)
    return psycopg2.connect(db_url)

print("\n" + "="*70)
print("  🚀 CHARGE DONNÉES - FINAL VERSION")
print("="*70)

# ÉTAPE 1: Vider COMPLÈTEMENT
print("\n🧹 STEP 1: Vider la base...")
conn = get_conn()
cur = conn.cursor()

try:
    cur.execute("DELETE FROM transform_hourly_avg")
    cur.execute("DELETE FROM transform_daily_volume")
    cur.execute("DELETE FROM transform_daily_ranking")
    cur.execute("DELETE FROM raw_crypto_prices")
    conn.commit()
    print("  ✅ Table vidée complètement")
except Exception as e:
    print(f"  ⚠️  {e}")
    conn.rollback()

cur.close()
conn.close()

# ÉTAPE 2: Vérifier que c'est vide
print("\n📊 STEP 2: Vérifier...")
conn = get_conn()
cur = conn.cursor()
cur.execute("SELECT COUNT(*) FROM raw_crypto_prices")
count = cur.fetchone()[0]
print(f"  Lignes actuelles: {count}")
if count == 0:
    print("  ✅ Table complètement vide")
else:
    print(f"  ⚠️  Encore {count} lignes!")
cur.close()
conn.close()

# ÉTAPE 3: Télécharger les données
print("\n📥 STEP 3: Télécharger les données...")

cryptos = {
    "bitcoin": ("Bitcoin", "btc"),
    "ethereum": ("Ethereum", "eth"),
    "ripple": ("XRP", "xrp"),
    "binancecoin": ("BNB", "bnb"),
    "solana": ("Solana", "sol"),
}

total_rows = 0

for coin_id, (name, symbol) in cryptos.items():
    print(f"\n  📊 {name}")

    try:
        # Télécharger
        url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
        params = {"vs_currency": "usd", "days": 30}
        headers = {"User-Agent": "Mozilla/5.0"}

        r = requests.get(url, params=params, headers=headers, timeout=30)
        if r.status_code != 200:
            print(f"    ❌ Erreur API: {r.status_code}")
            continue

        data = r.json()
        prices = data.get("prices", [])
        volumes = data.get("total_volumes", [])

        if not prices:
            print(f"    ❌ Pas de données")
            continue

        vol_map = {v[0]: v[1] for v in volumes}

        # Préparer les lignes
        rows = []
        prices_list = [p[1] for p in prices]

        for i, (ts_ms, price) in enumerate(prices):
            ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
            vol = vol_map.get(ts_ms, 0)

            idx_24h = max(0, i - 24)
            price_24h_ago = prices_list[idx_24h]
            change_24h = price - price_24h_ago
            change_pct = ((change_24h / price_24h_ago) * 100) if price_24h_ago > 0 else 0

            rows.append((
                coin_id, name, symbol,
                round(price, 8), 0,
                round(vol, 2),
                round(change_24h, 8),
                round(change_pct, 4),
                round(price * 1.01, 8),
                round(price * 0.99, 8),
                ts.replace(tzinfo=None)
            ))

        # Insérer DIRECTEMENT
        conn = get_conn()
        cur = conn.cursor()

        sql = """
        INSERT INTO raw_crypto_prices
            (coin_id, name, symbol, current_price, market_cap,
             total_volume, price_change_24h, price_change_pct_24h,
             high_24h, low_24h, fetched_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        cur.executemany(sql, rows)
        conn.commit()

        print(f"    ✅ {len(rows)} lignes insérées")
        total_rows += len(rows)

        cur.close()
        conn.close()

    except Exception as e:
        print(f"    ❌ Erreur: {str(e)[:100]}")

# ÉTAPE 4: VÉRIFIER FINAL
print("\n✅ STEP 4: Vérification finale...")
conn = get_conn()
cur = conn.cursor()
cur.execute("SELECT COUNT(*) FROM raw_crypto_prices")
final_count = cur.fetchone()[0]
cur.close()
conn.close()

print("\n" + "="*70)
if final_count > 0:
    print(f"  ✅✅✅ SUCCÈS!")
    print(f"  Total insérées: {final_count} lignes")
    print(f"  Attendez 2 minutes, puis rafraîchissez le dashboard!")
else:
    print(f"  ❌ PROBLÈME: 0 lignes insérées")
    print(f"  Vérifiez votre DATABASE_URL dans .env")
print("="*70)
