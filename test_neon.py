# test_neon.py
import psycopg2

# Colle ton URL Neon ici
URL = "postgresql://neondb_owner:npg_bgGCEw9sofm6@ep-spring-paper-agif76oh-pooler.c-2.eu-central-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
try:
    conn = psycopg2.connect(URL)
    print("Connexion Neon OK !")
    conn.close()
except Exception as e:
    print(f"Erreur : {e}")