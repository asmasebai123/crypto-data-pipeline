#!/usr/bin/env python3
"""
Script de démarrage pour Railway
Gère les erreurs de connexion et relance automatiquement
"""
import subprocess
import sys
import time
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s'
)
logger = logging.getLogger("railway_starter")

def start_pipeline():
    """Démarre le pipeline avec gestion des erreurs"""

    retry_count = 0
    max_retries = 5

    while retry_count < max_retries:
        try:
            logger.info(f"Démarrage du pipeline (tentative {retry_count + 1}/{max_retries})")

            # Lancer le pipeline
            process = subprocess.Popen(
                [sys.executable, "run_pipeline.py", "--mode", "batch", "--no-kafka"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )

            # Attendre le processus
            stdout, stderr = process.communicate()

            if process.returncode != 0:
                logger.error(f"Pipeline a échoué avec code {process.returncode}")
                logger.error(f"STDERR: {stderr}")
                retry_count += 1
                time.sleep(5)
                continue

            logger.info("Pipeline terminé avec succès")
            break

        except KeyboardInterrupt:
            logger.info("Arrêt du pipeline (CTRL+C)")
            break
        except Exception as e:
            logger.error(f"Erreur: {e}")
            retry_count += 1
            time.sleep(5)

    if retry_count >= max_retries:
        logger.error(f"Pipeline a échoué après {max_retries} tentatives")
        sys.exit(1)

if __name__ == "__main__":
    logger.info("Railway Starter - Crypto Data Pipeline")
    logger.info(f"DATABASE_URL: {'défini' if os.environ.get('DATABASE_URL') else 'NON défini'}")
    start_pipeline()
