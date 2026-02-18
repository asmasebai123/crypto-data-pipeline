# ingestion/migrate_to_neon.py
"""
Migre les données de PostgreSQL local vers Neon (cloud).
"""
import sys
import os
import psycopg2

# ────────────────────────────────────────────────────────────────
# COLLE TON URL NEON ICI (avec le nouveau mot de passe)
# ────────────────────────────────────────────────────────────────
NEON_URL = "postgresql://neondb_owner:npg_bgGCEw9sofm6@ep-spring-paper-agif76oh-pooler.c-2.eu-central-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

# Connexion locale (Docker)
LOCAL = {
    "host": "localhost",
    "port": 5432,
    "dbname": "crypto_db",
    "user": "admin",
    "password": "password123"
}


def get_local_conn():
    return psycopg2.connect(**LOCAL)


def get_neon_conn():
    return psycopg2.connect(NEON_URL)


def create_tables_neon():
    """Crée les tables dans Neon."""
    print("Creation des tables dans Neon...")
    
    tables_sql = [
        """CREATE TABLE IF NOT EXISTS raw_crypto_prices (
            id SERIAL PRIMARY KEY,
            coin_id VARCHAR(50) NOT NULL,
            name VARCHAR(100) NOT NULL,
            symbol VARCHAR(20) NOT NULL,
            current_price NUMERIC(20,8) NOT NULL,
            market_cap NUMERIC(30,2),
            total_volume NUMERIC(30,2),
            price_change_24h NUMERIC(20,8),
            price_change_pct_24h NUMERIC(10,4),
            high_24h NUMERIC(20,8),
            low_24h NUMERIC(20,8),
            fetched_at TIMESTAMP DEFAULT NOW()
        );""",
        
        """CREATE TABLE IF NOT EXISTS transform_hourly_avg (
            coin_id VARCHAR(50),
            hour TIMESTAMP,
            avg_price NUMERIC(20,4),
            min_price NUMERIC(20,4),
            max_price NUMERIC(20,4),
            nb_records INT
        );""",
        
        """CREATE TABLE IF NOT EXISTS transform_daily_volume (
            coin_id VARCHAR(50),
            day DATE,
            total_volume NUMERIC(30,2),
            avg_volume NUMERIC(30,2),
            avg_price NUMERIC(20,4),
            nb_records INT
        );""",
        
        """CREATE TABLE IF NOT EXISTS transform_daily_ranking (
            day DATE,
            rank INT,
            coin_id VARCHAR(50),
            name VARCHAR(100),
            current_price NUMERIC(20,4),
            price_change_pct_24h NUMERIC(10,4)
        );""",
        
        """CREATE TABLE IF NOT EXISTS transform_alerts (
            day DATE,
            coin_id VARCHAR(50),
            name VARCHAR(100),
            current_price NUMERIC(20,4),
            price_change_pct_24h NUMERIC(10,4),
            alert_level VARCHAR(50),
            alert_message TEXT
        );""",
        
        """CREATE TABLE IF NOT EXISTS stream_crypto_prices (
            id SERIAL PRIMARY KEY,
            coin_id VARCHAR(50) NOT NULL,
            price NUMERIC(20,8) NOT NULL,
            variation_pct NUMERIC(10,4),
            event_time TIMESTAMP NOT NULL,
            received_at TIMESTAMP DEFAULT NOW()
        );"""
    ]
    
    conn = get_neon_conn()
    cur = conn.cursor()
    
    for sql in tables_sql:
        cur.execute(sql)
    
    conn.commit()
    cur.close()
    conn.close()
    print("  Tables creees avec succes")


def migrate_table(table_name):
    """Migre une table de local vers Neon."""
    print(f"Migration de {table_name}...")
    
    # Lire depuis local
    local_conn = get_local_conn()
    local_cur = local_conn.cursor()
    
    local_cur.execute(f"SELECT * FROM {table_name}")
    rows = local_cur.fetchall()
    
    if not rows:
        print(f"  Table {table_name} vide, ignoree")
        local_cur.close()
        local_conn.close()
        return
    
    # Récupérer les noms de colonnes (sans 'id' auto-increment)
    cols = [desc[0] for desc in local_cur.description]
    cols_without_id = [c for c in cols if c != 'id']
    
    local_cur.close()
    local_conn.close()
    
    # Réouvrir pour récupérer les données sans id
    local_conn = get_local_conn()
    local_cur = local_conn.cursor()
    
    col_names = ",".join(cols_without_id)
    local_cur.execute(f"SELECT {col_names} FROM {table_name}")
    rows_data = local_cur.fetchall()
    
    local_cur.close()
    local_conn.close()
    
    # Écrire dans Neon
    neon_conn = get_neon_conn()
    neon_cur = neon_conn.cursor()
    
    # Supprimer les anciennes données
    neon_cur.execute(f"DELETE FROM {table_name}")
    
    # Insérer les nouvelles
    placeholders = ",".join(["%s"] * len(cols_without_id))
    sql = f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})"
    
    neon_cur.executemany(sql, rows_data)
    neon_conn.commit()
    
    neon_cur.close()
    neon_conn.close()
    
    print(f"  {len(rows_data)} lignes migrees")


if __name__ == "__main__":
    print("=" * 60)
    print("  MIGRATION POSTGRESQL LOCAL → NEON CLOUD")
    print("=" * 60)
    
    # Vérifier connexion Neon
    try:
        conn = get_neon_conn()
        print("Connexion Neon : OK")
        conn.close()
    except Exception as e:
        print(f"Erreur connexion Neon : {e}")
        exit(1)
    
    # Vérifier connexion locale
    try:
        conn = get_local_conn()
        print("Connexion locale : OK\n")
        conn.close()
    except Exception as e:
        print(f"Erreur connexion locale : {e}")
        print("Assure-toi que Docker tourne : docker compose up -d")
        exit(1)
    
    # Créer les tables
    create_tables_neon()
    
    # Migrer chaque table
    tables = [
        "raw_crypto_prices",
        "transform_hourly_avg",
        "transform_daily_volume",
        "transform_daily_ranking",
        "transform_alerts",
        "stream_crypto_prices"
    ]
    
    print()
    for table in tables:
        try:
            migrate_table(table)
        except Exception as e:
            print(f"  ERREUR : {e}")
    
    print()
    print("=" * 60)
    print("  MIGRATION TERMINEE")
    print("=" * 60)
    print("\nProchaine etape : deployer sur Streamlit Cloud")