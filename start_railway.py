#!/usr/bin/env python3
"""
Script de démarrage pour Railway - Wrapper pour main_cloud.py
main_cloud.py gère sa propre boucle de 10 minutes
"""
import sys
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s'
)
logger = logging.getLogger("railway_wrapper")

if __name__ == "__main__":
    logger.info("🚀 Railway Wrapper - Démarrage main_cloud.py")
    logger.info(f"🔗 DATABASE_URL: {'✅ Défini' if os.environ.get('DATABASE_URL') else '❌ NON défini'}")

    # Importer et lancer main_cloud
    from ingestion.main_cloud import run_pipeline_loop

    try:
        # Lancer la boucle infinie
        run_pipeline_loop(interval_minutes=10)
    except KeyboardInterrupt:
        logger.info("⏹️  Arrêt du pipeline")
        sys.exit(0)
    except Exception as e:
        logger.error(f"❌ Erreur: {e}")
        sys.exit(1)
