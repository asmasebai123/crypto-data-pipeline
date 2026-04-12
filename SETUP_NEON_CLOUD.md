# Configuration de Neon Cloud ☁️

## Problème identifié
- ✅ Le dashboard **local** affiche bien les données
- ❌ Le dashboard **cloud** affiche "Aucune donnée"

**Raison**: Le pipeline local écrit dans PostgreSQL Docker, pas dans Neon Cloud.

---

## Solution: 4 étapes

### 1️⃣ Récupérer l'URL Neon

**Sur le dashboard Neon Cloud:**
1. Allez à https://console.neon.tech
2. Ouvrez votre projet `crypto_data_pipeline`
3. Onglet **Databases** → Sélectionnez `neondb`
4. **Connection string** → Copiez l'URL `postgresql://...`

**Exemple de format:**
```
postgresql://neondb_owner:npg_abc123def456@ep-spring-paper-xyz.c-2.eu-central-1.aws.neon.tech/neondb?sslmode=require
```

---

### 2️⃣ Ajouter DATABASE_URL à votre `.env` local

Créez/modifiez le fichier `.env` à la racine du projet:

```bash
COINGECKO_API_KEY=your_key_here
DATABASE_URL=postgresql://neondb_owner:password@host/neondb?sslmode=require
```

---

### 3️⃣ Migrer les données locales vers Neon

En local, exécutez:

```bash
# Windows
set DATABASE_URL=postgresql://...
python migrate_to_neon.py

# Mac/Linux
export DATABASE_URL=postgresql://...
python migrate_to_neon.py
```

**Résultat attendu:**
```
✅ Tables créées dans Neon
✅ 50 lignes transférées
✅ Migration réussie!
```

---

### 4️⃣ Configurer Streamlit Cloud

Sur **https://share.streamlit.io** → Votre app → **Settings** → **Secrets**:

Ajoutez exactement:
```toml
DATABASE_URL = "postgresql://neondb_owner:password@host/neondb?sslmode=require"
```

**Important**:
- Pas de guillemets supplémentaires
- Pas de `[database]` section
- Vérifiez que c'est **exactement** votre URL Neon

---

### 5️⃣ Faire tourner le pipeline en continu

Vous avez maintenant 2 options:

#### Option A: Continuer localement
Le pipeline local (`run_pipeline.py`) écrit **directement** dans Neon Cloud maintenant (grâce à la variable `DATABASE_URL`).

```bash
export DATABASE_URL=postgresql://...
python run_pipeline.py --mode full
```

Le dashboard cloud reçoit les données en temps réel ✨

#### Option B: Utiliser Prefect (scheduling)
Pour que le pipeline tourne **automatiquement** toutes les 10 minutes:

```bash
prefect deployment build ingestion/main.py -n crypto-batch --apply
prefect deployment run crypto-batch
```

---

## Vérification

### Localement
```bash
# Vérifier la connexion Neon
export DATABASE_URL=postgresql://...
python -c "from ingestion.database import get_connection; conn = get_connection(); print('✅ Connecté à Neon')"
```

### Sur Streamlit Cloud
1. Allez à votre dashboard: https://crypto-data-pipeline-...streamlit.app
2. Attendez 10 secondes (refresh)
3. Vous devez voir les prix en haut + les graphiques

Si aucune donnée:
- Vérifiez le **SECRET** dans Streamlit Settings
- Attendez que le pipeline local envoie de nouvelles données
- Rechargez la page (Ctrl+Shift+R)

---

## Commandes utiles

```bash
# Tester la connexion Neon
psql postgresql://user:password@host/db -c "SELECT COUNT(*) FROM raw_crypto_prices"

# Voir les logs
tail -f logs/ingestion.log

# Arrêter le pipeline
Ctrl+C
```

---

## Troubleshooting

| Erreur | Solution |
|--------|----------|
| `ERROR: (psycopg2.OperationalError) FATAL: password authentication failed` | Vérifiez le mot de passe dans DATABASE_URL |
| `No data yet` en cloud | Lancez le pipeline local avec DATABASE_URL défini |
| `sslmode` error | Assurez-vous que `sslmode=require` est dans l'URL |
| Database permissions | Utilisez le **Neon user** par défaut (pas de création de tables) |

---

## Résumé des fichiers modifiés

✅ `ingestion/database.py` - Lit DATABASE_URL si présent
✅ `ingestion/main_cloud.py` - Utilise DATABASE_URL
✅ `migrate_to_neon.py` - Script de migration (créé)
✅ `dashboard/config.py` - Déjà modifié pour Streamlit secrets

**Prêt? Lancez `python migrate_to_neon.py` 🚀**
