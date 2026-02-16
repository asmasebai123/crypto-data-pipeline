# ingestion/kafka_producer.py
import json
import time
import random
import logging
import urllib3
import requests
from datetime import datetime
from kafka import KafkaProducer

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/kafka_producer.log")
    ]
)
logger = logging.getLogger("kafka_producer")

TOPIC   = "crypto_prices"
CRYPTOS = ["bitcoin", "ethereum", "ripple", "binancecoin", "solana"]

# ─── Prix de référence (mis à jour depuis l'API) ──────────────────
def get_reference_prices():
    url    = "https://api.coingecko.com/api/v3/simple/price"
    params = {"ids": ",".join(CRYPTOS), "vs_currencies": "usd"}
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    try:
        r = requests.get(url, params=params, headers=headers,
                         timeout=15, verify=False)
        r.raise_for_status()
        prices = {k: v["usd"] for k, v in r.json().items()}
        logger.info(f"Prix de référence récupérés : {prices}")
        return prices
    except Exception as e:
        logger.warning(f"Impossible de récupérer les prix réels, utilisation de prix fictifs : {e}")
        return {
            "bitcoin": 95000, "ethereum": 3200, "ripple": 0.55,
            "binancecoin": 420, "solana": 185
        }


# ─── Simuler une variation de prix ───────────────────────────────
def simulate_price(base_price):
    variation_pct = random.uniform(-2.0, 2.0)          # ±2% de variation
    new_price     = base_price * (1 + variation_pct / 100)
    return round(new_price, 8), round(variation_pct, 4)


# ─── Producteur principal ─────────────────────────────────────────
def run_producer(interval_seconds=5):
    logger.info("🚀 Démarrage Kafka Producer")

    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5
    )

    base_prices = get_reference_prices()

    try:
        while True:
            coin_id    = random.choice(CRYPTOS)
            base_price = base_prices.get(coin_id, 100)
            price, variation = simulate_price(base_price)

            message = {
                "coin_id":       coin_id,
                "price":         price,
                "variation_pct": variation,
                "event_time":    datetime.utcnow().isoformat()
            }

            producer.send(TOPIC, value=message)
            logger.info(f"📤 Envoyé → {coin_id}: ${price:,.4f} ({variation:+.2f}%)")

            # Mettre à jour le prix de base pour la prochaine simulation
            base_prices[coin_id] = price

            time.sleep(interval_seconds)

    except KeyboardInterrupt:
        logger.info("⛔ Producteur arrêté")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    run_producer(interval_seconds=5)