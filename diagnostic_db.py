#!/usr/bin/env python3
"""
Script diagnostic pour vérifier l'état de la base de données Neon
"""
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd

# Charger .env
load_dotenv()

# Récupérer DATABASE_URL
db_url = os.getenv("DATABASE_URL")
if not db_url:
    print("❌ DATABASE_URL non trouvée dans .env")
    exit(1)

print(f"✅ Connexion à: {db_url[:50]}...")

try:
    engine = create_engine(db_url)

    with engine.connect() as conn:
        # Récupérer toutes les tables
        result = conn.execute(text("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
        """))
        tables = [row[0] for row in result]

        print("\n📊 === DIAGNOSTIC BASE DE DONNÉES ===\n")

        for table in tables:
            try:
                count_result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = count_result.scalar()

                # Récupérer la date la plus récente
                latest_result = conn.execute(text(f"""
                    SELECT * FROM {table} ORDER BY 1 DESC LIMIT 1
                """))
                latest = latest_result.fetchone()

                print(f"📋 Table: {table}")
                print(f"   Lignes: {count}")
                if latest and count > 0:
                    print(f"   Dernière entrée: {latest[0] if latest else 'N/A'}")
                print()

            except Exception as e:
                print(f"❌ Erreur pour {table}: {e}\n")

        print("\n" + "="*50)
        print("RÉSUMÉ:")
        print("="*50)

        # Vérifier les données batch
        batch_count = conn.execute(text("SELECT COUNT(*) FROM raw_crypto_prices")).scalar()
        stream_count = conn.execute(text("SELECT COUNT(*) FROM stream_crypto_prices")).scalar()

        print(f"✅ Données BATCH (raw_crypto_prices): {batch_count} lignes")
        print(f"✅ Données STREAMING (stream_crypto_prices): {stream_count} lignes")
        print(f"📊 TOTAL: {batch_count + stream_count} lignes")

        if batch_count == 0 and stream_count == 0:
            print("\n⚠️  PROBLÈME GRAVE: Aucune donnée collectée!")
            print("Le pipeline n'a jamais été exécuté ou a échoué.")
        elif batch_count < 100:
            print(f"\n⚠️  ATTENTION: Très peu de données ({batch_count} lignes)")
            print("Le pipeline ne s'exécute pas régulièrement toutes les 10 minutes")
        else:
            print(f"\n✅ Quantité de données correcte")

except Exception as e:
    print(f"❌ Erreur de connexion: {e}")
    exit(1)
