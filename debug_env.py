#!/usr/bin/env python3
"""
Script de debug pour vérifier les variables du .env
"""
import os

print("=" * 70)
print("🔍 DEBUG: Vérification du chargement de DATABASE_URL")
print("=" * 70)

# 1. Vérifier os.environ
database_url_env = os.environ.get("DATABASE_URL")
print(f"\n1. os.environ.get('DATABASE_URL'): {database_url_env}")

# 2. Vérifier le .env
print(f"\n2. Contenu du .env:")
try:
    with open(".env") as f:
        lines = f.readlines()
        for i, line in enumerate(lines, 1):
            if "DATABASE_URL" in line:
                # Masquer le mot de passe
                masked = line.split("@")[0] + "@***" if "@" in line else line
                print(f"   Ligne {i}: {masked.strip()}")
            elif line.strip() and not line.startswith("#"):
                print(f"   Ligne {i}: {line.strip()[:50]}...")
except FileNotFoundError:
    print("   ❌ Fichier .env non trouvé!")

# 3. Tester la connexion
print(f"\n3. Test de connexion:")
from ingestion.database import get_connection, load_database_url

database_url = load_database_url()
if database_url:
    host = database_url.split("@")[1].split("/")[0] if "@" in database_url else "?"
    print(f"   ✅ DATABASE_URL chargé")
    print(f"   Host: {host}")
else:
    print(f"   ❌ DATABASE_URL non chargé")

# 4. Vérifier la connexion
try:
    conn = get_connection()
    print(f"   ✅ Connexion établie!")
    conn.close()
except Exception as e:
    print(f"   ❌ Erreur: {e}")

print("\n" + "=" * 70)
