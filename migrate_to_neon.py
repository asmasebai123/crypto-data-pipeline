#!/usr/bin/env python3
"""
Script de migration : transfert des données du PostgreSQL local vers Neon Cloud
"""
import psycopg2
import os
import sys
import urllib.parse
from datetime import datetime

def get_local_connection():
    """Connexion au PostgreSQL local"""
    return psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="crypto_db",
        user="admin",
        password="password123"
    )

def get_neon_connection(neon_url):
    """Connexion à Neon Cloud"""
    parsed = urllib.parse.urlparse(neon_url)
    return psycopg2.connect(
        host=parsed.hostname,
        port=parsed.port or 5432,
        dbname=parsed.path.lstrip('/'),
        user=parsed.username,
        password=parsed.password,
        sslmode='require'
    )

def migrate_data():
    """Migre les données du local vers Neon"""

    # 1. Récupérer l'URL Neon
    neon_url = os.environ.get("DATABASE_URL")

    if not neon_url:
        print("❌ DATABASE_URL non défini!")
        print("   Définissez d'abord: export DATABASE_URL='postgresql://user:password@host:port/db'")
        sys.exit(1)

    print("🔄 Migration des données vers Neon Cloud...")
    print(f"   Neon: {neon_url[:50]}...")

    try:
        # Connexions
        local_conn = get_local_connection()
        neon_conn = get_neon_connection(neon_url)

        local_cur = local_conn.cursor()
        neon_cur = neon_conn.cursor()

        print("\n📊 Étape 1: Créer les tables dans Neon...")

        # Créer les tables
        sql_raw = """
        CREATE TABLE IF NOT EXISTS raw_crypto_prices (
            id              SERIAL PRIMARY KEY,
            coin_id         VARCHAR(50)     NOT NULL,
            name            VARCHAR(100)    NOT NULL,
            symbol          VARCHAR(20)     NOT NULL,
            current_price   NUMERIC(20, 8)  NOT NULL,
            market_cap      NUMERIC(30, 2),
            total_volume    NUMERIC(30, 2),
            price_change_24h        NUMERIC(20, 8),
            price_change_pct_24h    NUMERIC(10, 4),
            high_24h        NUMERIC(20, 8),
            low_24h         NUMERIC(20, 8),
            fetched_at      TIMESTAMP       DEFAULT NOW()
        );
        """

        sql_stream = """
        CREATE TABLE IF NOT EXISTS stream_crypto_prices (
            id              SERIAL PRIMARY KEY,
            coin_id         VARCHAR(50)     NOT NULL,
            price           NUMERIC(20, 8)  NOT NULL,
            variation_pct   NUMERIC(10, 4),
            event_time      TIMESTAMP       NOT NULL,
            received_at     TIMESTAMP       DEFAULT NOW()
        );
        """

        neon_cur.execute(sql_raw)
        neon_cur.execute(sql_stream)
        neon_conn.commit()
        print("✅ Tables créées dans Neon")

        # Transférer raw_crypto_prices
        print("\n📤 Étape 2: Transférer raw_crypto_prices...")
        local_cur.execute("SELECT * FROM raw_crypto_prices")
        rows = local_cur.fetchall()

        if rows:
            insert_sql = """
            INSERT INTO raw_crypto_prices
                (coin_id, name, symbol, current_price, market_cap,
                 total_volume, price_change_24h, price_change_pct_24h,
                 high_24h, low_24h, fetched_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            neon_cur.executemany(insert_sql,
                [(r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9], r[10], r[11]) for r in rows])
            neon_conn.commit()
            print(f"✅ {len(rows)} lignes transférées")
        else:
            print("   (aucune donnée)")

        # Transférer stream_crypto_prices
        print("\n📤 Étape 3: Transférer stream_crypto_prices...")
        local_cur.execute("SELECT * FROM stream_crypto_prices")
        rows = local_cur.fetchall()

        if rows:
            insert_sql = """
            INSERT INTO stream_crypto_prices
                (coin_id, price, variation_pct, event_time, received_at)
            VALUES (%s, %s, %s, %s, %s)
            """
            neon_cur.executemany(insert_sql,
                [(r[1], r[2], r[3], r[4], r[5]) for r in rows])
            neon_conn.commit()
            print(f"✅ {len(rows)} lignes transférées")
        else:
            print("   (aucune donnée)")

        # Vérification
        print("\n✔️ Vérification des données dans Neon...")
        neon_cur.execute("SELECT COUNT(*) FROM raw_crypto_prices")
        count = neon_cur.fetchone()[0]
        print(f"   raw_crypto_prices: {count} lignes")

        neon_cur.execute("SELECT COUNT(*) FROM stream_crypto_prices")
        count = neon_cur.fetchone()[0]
        print(f"   stream_crypto_prices: {count} lignes")

        print("\n✨ Migration réussie! Le dashboard cloud devrait afficher les données maintenant.")

        # Fermeture
        local_cur.close()
        neon_cur.close()
        local_conn.close()
        neon_conn.close()

    except Exception as e:
        print(f"\n❌ Erreur: {e}")
        sys.exit(1)

if __name__ == "__main__":
    migrate_data()
