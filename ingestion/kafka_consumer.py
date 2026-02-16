# ingestion/kafka_consumer.py
import json
import logging
from datetime import datetime
from kafka import KafkaConsumer
from database import insert_stream_price, create_tables

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/kafka_consumer.log")
    ]
)
logger = logging.getLogger("kafka_consumer")

TOPIC = "crypto_prices"


def run_consumer():
    logger.info(" Démarrage Kafka Consumer")
    create_tables()

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=["localhost:9092"],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",       # Lire depuis le début si nouveau groupe
        enable_auto_commit=True,
        group_id="crypto_consumer_group"
    )

    logger.info(f" En écoute sur le topic '{TOPIC}'...")

    try:
        for message in consumer:
            data = message.value

            coin_id       = data.get("coin_id")
            price         = data.get("price")
            variation_pct = data.get("variation_pct")
            event_time    = datetime.fromisoformat(data.get("event_time"))

            logger.info(f" Reçu : {coin_id} @ ${price:,.4f} ({variation_pct:+.2f}%)")

            try:
                insert_stream_price(coin_id, price, variation_pct, event_time)
                logger.info(f" Sauvegardé en base : {coin_id}")
            except Exception as e:
                logger.error(f"Erreur sauvegarde : {e}")

    except KeyboardInterrupt:
        logger.info(" Consommateur arrêté")
    finally:
        consumer.close()


if __name__ == "__main__":
    run_consumer()