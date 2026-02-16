# ingestion/main.py
import sys
import os
import requests
import urllib3
import logging
import time
from datetime import datetime
from database import insert_raw_prices, create_tables

# ─── Fix encodage Windows (emojis dans les logs) ──────────────────
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')
if sys.stderr.encoding != 'utf-8':
    sys.stderr.reconfigure(encoding='utf-8')

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ─── Dossier logs ─────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)

# ─── Configuration des logs ───────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(stream=open(sys.stdout.fileno(),
                              mode='w', encoding='utf-8', closefd=False)),
        logging.FileHandler("logs/ingestion.log", encoding="utf-8")
    ]
)
logger = logging.getLogger("batch_ingestion")

# ─── Clé API CoinGecko ────────────────────────────────────────────
# Charge depuis .env manuellement (sans dépendance python-dotenv)
def load_api_key():
    try:
        with open(".env") as f:
            for line in f:
                line = line.strip()
                if line.startswith("COINGECKO_API_KEY"):
                    return line.split("=", 1)[1].strip()
    except FileNotFoundError:
        pass
    return None

API_KEY = load_api_key()


# ─── Appel API avec retry ─────────────────────────────────────────
def fetch_crypto_prices(max_retries=3, wait_seconds=60):
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
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json"
    }

    # Ajout de la cle API si disponible
    if API_KEY:
        headers["x-cg-demo-api-key"] = API_KEY
        logger.info("Cle API CoinGecko chargee")
    else:
        logger.warning("Aucune cle API - risque de blocage 403")

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Tentative {attempt}/{max_retries} - appel API CoinGecko")
            response = requests.get(
                url, params=params, headers=headers,
                timeout=30, verify=False
            )

            if response.status_code == 429:
                logger.warning(f"Rate limit (429). Attente {wait_seconds}s...")
                time.sleep(wait_seconds)
                continue

            if response.status_code == 403:
                logger.error("Erreur 403 : acces refuse - verifie ta cle API dans .env")
                logger.error(f"Reponse : {response.text[:200]}")
                break

            response.raise_for_status()
            data = response.json()
            logger.info(f"OK - {len(data)} cryptos recuperees")
            return data

        except requests.exceptions.SSLError as e:
            logger.error(f"Erreur SSL : {e}")
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Erreur connexion (tentative {attempt}) : {e}")
        except requests.exceptions.Timeout:
            logger.error(f"Timeout (tentative {attempt})")
        except requests.exceptions.HTTPError as e:
            logger.error(f"Erreur HTTP : {e}")

        if attempt < max_retries:
            logger.info("Retry dans 10 secondes...")
            time.sleep(10)

    logger.critical("ECHEC - toutes les tentatives ont echoue")
    return []


# ─── Affichage terminal ───────────────────────────────────────────
def display_prices(data):
    if not data:
        print("Aucune donnee.")
        return
    print("\n" + "=" * 65)
    print(f"  Crypto Prices - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 65)
    print(f"  {'Nom':<14} {'Prix USD':>12}  {'24h':>8}  {'Volume':>18}")
    print("-" * 65)
    for coin in data:
        name   = coin.get('name', '')
        price  = coin.get('current_price', 0) or 0
        change = coin.get('price_change_percentage_24h', 0) or 0
        volume = coin.get('total_volume', 0) or 0
        arrow  = "+" if change >= 0 else "-"
        print(f"  {name:<14} ${price:>12,.2f}  {arrow}{abs(change):>6.2f}%  ${volume:>16,.0f}")
    print("=" * 65 + "\n")


# ─── Boucle principale ────────────────────────────────────────────
def run_batch_ingestion(interval_minutes=10, run_once=False):
    logger.info("Demarrage ingestion batch")
    create_tables()

    while True:
        start = datetime.now()
        data  = fetch_crypto_prices()

        if data:
            display_prices(data)
            try:
                nb = insert_raw_prices(data)
                logger.info(f"{nb} lignes sauvegardees en base")
            except Exception as e:
                logger.error(f"Erreur sauvegarde DB : {e}")
        else:
            logger.warning("Aucune donnee recuperee ce cycle")

        if run_once:
            break

        elapsed = (datetime.now() - start).seconds
        wait    = max(0, interval_minutes * 60 - elapsed)
        logger.info(f"Prochain cycle dans {wait // 60}m {wait % 60}s")
        time.sleep(wait)


if __name__ == "__main__":
    run_batch_ingestion(interval_minutes=10, run_once=True)