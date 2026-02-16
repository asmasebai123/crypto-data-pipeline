# ingestion/populate_data.py
"""
Script pour remplir la base avec beaucoup de donnees historiques simulees.
Simule 7 jours de donnees toutes les 10 minutes = ~1000 lignes par crypto.
"""
import sys
import os
import random
import urllib3
import requests
import psycopg2
from datetime import datetime, timedelta

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from database import create_tables, get_connection

# ─── Prix de base réels (approximatifs) ───────────────────────────
BASE_PRICES = {
    "bitcoin":     {"name": "Bitcoin",     "symbol": "btc",
                    "price": 95000, "market_cap": 1800000000000,
                    "volume": 28000000000},
    "ethereum":    {"name": "Ethereum",    "symbol": "eth",
                    "price": 3200,  "market_cap": 385000000000,
                    "volume": 12000000000},
    "solana":      {"name": "Solana",      "symbol": "sol",
                    "price": 185,   "market_cap": 85000000000,
                    "volume": 4000000000},
    "ripple":      {"name": "XRP",         "symbol": "xrp",
                    "price": 0.55,  "market_cap": 30000000000,
                    "volume": 1500000000},
    "binancecoin": {"name": "BNB",         "symbol": "bnb",
                    "price": 610,   "market_cap": 90000000000,
                    "volume": 2000000000},
}


def simulate_price_history(base_price, nb_points):
    """Simule une série de prix avec marche aléatoire réaliste."""
    prices = [base_price]
    for _ in range(nb_points - 1):
        # Variation entre -1.5% et +1.5% par intervalle
        variation = random.uniform(-0.015, 0.015)
        # Tendance légère (drift)
        drift = random.uniform(-0.001, 0.001)
        new_price = prices[-1] * (1 + variation + drift)
        # Garde le prix dans des limites réalistes
        new_price = max(new_price, base_price * 0.7)
        new_price = min(new_price, base_price * 1.3)
        prices.append(round(new_price, 8))
    return prices


def generate_historical_data(days=7, interval_minutes=10):
    """
    Génère des données historiques simulées.
    days=7, interval=10min → 7*24*6 = 1008 points par crypto.
    """
    nb_points = int(days * 24 * 60 / interval_minutes)
    now       = datetime.utcnow()
    start     = now - timedelta(days=days)

    # Générer les timestamps
    timestamps = [
        start + timedelta(minutes=i * interval_minutes)
        for i in range(nb_points)
    ]

    all_rows = []

    for coin_id, info in BASE_PRICES.items():
        print(f"  Generation des donnees pour {info['name']}...")

        prices  = simulate_price_history(info["price"], nb_points)
        volumes = [
            info["volume"] * random.uniform(0.7, 1.3)
            for _ in range(nb_points)
        ]

        for i, (ts, price, volume) in enumerate(zip(timestamps, prices, volumes)):
            # Calcul variation 24h (compare avec 144 points avant = 24h)
            idx_24h = max(0, i - 144)
            price_24h_ago = prices[idx_24h]
            change_24h    = price - price_24h_ago
            change_pct_24h = ((price - price_24h_ago) / price_24h_ago * 100
                              if price_24h_ago > 0 else 0)

            all_rows.append((
                coin_id,
                info["name"],
                info["symbol"],
                price,
                info["market_cap"] * (price / info["price"]),  # market cap ajusté
                volume,
                change_24h,
                round(change_pct_24h, 4),
                price * 1.02,   # high_24h simulé
                price * 0.98,   # low_24h simulé
                ts
            ))

    return all_rows


def insert_bulk(rows):
    """Insère toutes les lignes en une seule transaction."""
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

    # Insertion par batch de 500 pour la performance
    batch_size = 500
    total      = len(rows)
    inserted   = 0

    for i in range(0, total, batch_size):
        batch = rows[i:i + batch_size]
        cur.executemany(sql, batch)
        conn.commit()
        inserted += len(batch)
        print(f"  Progression : {inserted}/{total} lignes inserees...")

    cur.close()
    conn.close()
    return inserted


def fetch_real_prices_once():
    """Récupère les prix réels actuels et les insère."""
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

    # Charger la clé API depuis .env
    api_key = None
    try:
        with open(".env") as f:
            for line in f:
                if line.startswith("COINGECKO_API_KEY"):
                    api_key = line.split("=", 1)[1].strip()
    except FileNotFoundError:
        pass

    if api_key:
        headers["x-cg-demo-api-key"] = api_key

    try:
        r = requests.get(url, params=params, headers=headers,
                         timeout=30, verify=False)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"  API indisponible ({e}), utilisation des donnees simulees uniquement")
        return []


if __name__ == "__main__":
    print("=" * 55)
    print("  REMPLISSAGE BASE DE DONNEES — DONNEES HISTORIQUES")
    print("=" * 55)

    # 1. Créer les tables
    print("\n[1/4] Creation des tables...")
    create_tables()
    print("      OK")

    # 2. Récupérer prix réels actuels
    print("\n[2/4] Recuperation prix reels actuels...")
    real_data = fetch_real_prices_once()
    if real_data:
        # Mettre à jour les prix de base avec les vrais prix
        for coin in real_data:
            if coin["id"] in BASE_PRICES:
                BASE_PRICES[coin["id"]]["price"]  = coin["current_price"]
                BASE_PRICES[coin["id"]]["volume"] = coin["total_volume"]
        print(f"      {len(real_data)} cryptos recuperees")
    else:
        print("      Utilisation des prix par defaut")

    # 3. Générer données historiques (7 jours)
    print("\n[3/4] Generation donnees historiques (7 jours x 5 cryptos)...")
    rows = generate_historical_data(days=7, interval_minutes=10)
    print(f"      {len(rows):,} lignes generees")

    # 4. Insérer en base
    print("\n[4/4] Insertion en base de donnees...")
    nb = insert_bulk(rows)

    print("\n" + "=" * 55)
    print(f"  TERMINE ! {nb:,} lignes inserees dans raw_crypto_prices")
    print(f"  Periode : 7 derniers jours")
    print(f"  Cryptos : {len(BASE_PRICES)} (BTC, ETH, SOL, XRP, BNB)")
    print(f"  Intervalle : toutes les 10 minutes")
    print("=" * 55)
    print("\nRelance le dashboard Streamlit pour voir les donnees !")