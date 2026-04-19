#!/usr/bin/env python3
"""
Script diagnostic simplifié pour vérifier l'état de la base de données Neon
"""
import os
import psycopg2
from dotenv import load_dotenv

# Charger .env
load_dotenv()

# Récupérer DATABASE_URL
db_url = os.getenv("DATABASE_URL")
if not db_url:
    print("❌ DATABASE_URL non trouvée dans .env")
    exit(1)

print(f"✅ Connexion à: {db_url[:80]}...")

try:
    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()

    print("\n📊 === DIAGNOSTIC BASE DE DONNÉES ===\n")

    # Récupérer toutes les tables
    cursor.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name
    """)
    tables = [row[0] for row in cursor.fetchall()]

    for table in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]

            print(f"📋 {table:<40} | {count:>6} lignes")

        except Exception as e:
            print(f"❌ Erreur pour {table}: {e}")

    print("\n" + "="*60)
    print("RÉSUMÉ:")
    print("="*60)

    cursor.execute("SELECT COUNT(*) FROM raw_crypto_prices")
    batch_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM stream_crypto_prices")
    stream_count = cursor.fetchone()[0]

    total = batch_count + stream_count

    print(f"✅ Données BATCH (raw_crypto_prices):     {batch_count:>6} lignes")
    print(f"✅ Données STREAMING (stream_crypto_prices): {stream_count:>6} lignes")
    print(f"📊 TOTAL:                                {total:>6} lignes")

    if batch_count == 0 and stream_count == 0:
        print("\n❌ PROBLÈME GRAVE: Aucune donnée collectée!")
        print("   Le pipeline n'a jamais été exécuté ou a échoué complètement.")
    elif total < 100:
        print(f"\n⚠️  ATTENTION: Très peu de données ({total} lignes)")
        print("   Le pipeline ne s'exécute pas régulièrement toutes les 10 minutes")
        print("   CAUSE: Railway lance le script UNE FOIS et s'arrête")
    else:
        print(f"\n✅ Quantité de données acceptable")

    # Vérifier la date des données
    cursor.execute("""
        SELECT MAX(fetched_at) FROM raw_crypto_prices
    """)
    latest_date = cursor.fetchone()[0]

    if latest_date:
        print(f"\n📅 Dernière collecte: {latest_date}")
        import datetime
        now = datetime.datetime.now(datetime.timezone.utc)
        delta = (now - latest_date).total_seconds() / 60
        print(f"⏱️  Il y a: {delta:.0f} minutes")

        if delta > 30:
            print("❌ Les données ne sont pas à jour (> 30 min)")
            print("   Le pipeline n'a pas été relancé récemment")

    cursor.close()
    conn.close()

except Exception as e:
    print(f"❌ Erreur de connexion: {e}")
    exit(1)
