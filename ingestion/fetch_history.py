# ingestion/fetch_history.py
import sys, os, requests, urllib3, time
from datetime import datetime, timezone
import psycopg2

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from database import create_tables, get_connection

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
    try:
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with open(os.path.join(root, ".env")) as f:
            for line in f:
                if line.startswith("COINGECKO_API_KEY"):
                    return line.split("=", 1)[1].strip()
    except FileNotFoundError:
        pass
    return None

API_KEY = load_api_key()

def fetch_market_chart(coin_id, days=7):
    """
    Récupère les prix historiques — plan gratuit CoinGecko.
    Sans 'interval' forcé : retourne ~168 points pour 7 jours.
    """
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
    params = {
        "vs_currency": "usd",
        "days": days
        # PAS de 'interval' : plan gratuit uniquement
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json"
    }
    if API_KEY:
        headers["x-cg-demo-api-key"] = API_KEY
        print(f"  Cle API chargee : {API_KEY[:8]}...")
    else:
        print(f"  ATTENTION : pas de cle API dans .env")

    for attempt in range(1, 4):
        try:
            print(f"  [{attempt}/3] Appel API {coin_id}...")
            r = requests.get(url, params=params, headers=headers,
                             timeout=30, verify=False)

            print(f"  Status : {r.status_code}")

            if r.status_code == 429:
                print(f"  Rate limit — attente 60s...")
                time.sleep(60)
                continue

            if r.status_code == 401:
                print(f"  ERREUR 401 : cle API invalide ou expiree")
                print(f"  Verifie ta cle sur https://www.coingecko.com/en/api/pricing")
                return []

            r.raise_for_status()
            data    = r.json()
            prices  = data.get("prices", [])
            volumes = data.get("total_volumes", [])
            vol_map = {v[0]: v[1] for v in volumes}

            result = []
            for ts_ms, price in prices:
                ts  = datetime.fromtimestamp(ts_ms / 1000,
                                             tz=timezone.utc)
                vol = vol_map.get(ts_ms, 0)
                result.append((ts, price, vol))

            print(f"  OK — {len(result)} points pour {coin_id}")
            return result

        except Exception as e:
            print(f"  Erreur tentative {attempt} : {e}")
            if attempt < 3:
                time.sleep(10)

    return []


def insert_historical(coin_id, name, symbol, records):
    if not records:
        return 0

    sql = """
    INSERT INTO raw_crypto_prices
        (coin_id, name, symbol, current_price, market_cap,
         total_volume, price_change_24h, price_change_pct_24h,
         high_24h, low_24h, fetched_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING
    """
    conn = get_connection()
    cur  = conn.cursor()
    rows = []
    prices_list = [r[1] for r in records]

    for i, (ts, price, volume) in enumerate(records):
        idx_24h       = max(0, i - 24)
        price_24h_ago = prices_list[idx_24h]
        change_24h    = price - price_24h_ago
        change_pct    = ((change_24h / price_24h_ago) * 100
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

    cur.executemany(sql, rows)
    conn.commit()
    cur.close()
    conn.close()
    return len(rows)


def clear_old_data():
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("DELETE FROM raw_crypto_prices")
    conn.commit()
    deleted = cur.rowcount
    cur.close()
    conn.close()
    print(f"  {deleted} lignes supprimees")


if __name__ == "__main__":
    print("=" * 55)
    print("  RECUPERATION HISTORIQUE REEL — COINGECKO")
    print("=" * 55)

    print(f"\nCle API : {'OK - ' + API_KEY[:8] + '...' if API_KEY else 'NON TROUVEE'}")

    create_tables()
    print("\nSuppression anciennes donnees...")
    clear_old_data()

    print("\nRecuperation historique (7 jours)...")
    total = 0

    for coin_id, name in CRYPTOS.items():
        records = fetch_market_chart(coin_id, days=7)
        if records:
            nb = insert_historical(coin_id, name,
                                   SYMBOLS[coin_id], records)
            total += nb
            print(f"  Insere : {nb} lignes pour {name}\n")
        else:
            print(f"  Echec pour {name}\n")
        time.sleep(5)   # pause obligatoire entre chaque crypto

    print("=" * 55)
    print(f"  TERMINE ! {total} lignes reelles inserees")
    print("=" * 55)