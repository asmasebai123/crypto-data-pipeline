# 🎤 Séquence de Présentation VS Code au Professeur

## 📋 Plan de présentation (15-20 minutes)

```
1. Ouvrir le projet (2 min)
2. Montrer l'architecture (2 min)
3. Explorer les fichiers clés (10 min)
4. Montrer le dashboard (3 min)
5. Montrer les logs (2 min)
```

---

## ✅ AVANT LA PRÉSENTATION

- [ ] Ouvrir VS Code avec le projet
- [ ] Ouvrir le terminal
- [ ] Ouvrir le dashboard Streamlit dans le navigateur
- [ ] Avoir les logs Railway ouverts dans un autre onglet
- [ ] Imprimer le guide des fichiers (ou l'avoir sur le téléphone)

---

# 🚀 PENDANT LA PRÉSENTATION

## ÉTAPE 1: Structure du projet (2 min)

**Dites au prof:**
> "Voici mon projet Crypto Data Pipeline. C'est une architecture complète de data engineering."

**Montrez:**
```
Cliquez sur l'Explorateur de fichiers (gauche)
Montrez la structure:

crypto_data_project/
├── 📁 ingestion/        ← Collecte des données
├── 📁 dashboard/        ← Interface visuelle
├── Procfile             ← Config Railway
├── start_railway.py     ← Orchestration
├── load_massive_data.py ← Chargement données
└── requirements.txt     ← Dépendances
```

**Explique:**
- "J'ai 3 composants principaux: Ingestion, Dashboard, et Orchestration"

---

## ÉTAPE 2: Expliquer le cœur du projet (3 min)

### Ouvrir: `ingestion/main_cloud.py`

**Montrez:**
```python
def run_pipeline_loop(interval_minutes=10):
    """Boucle principale."""
    while True:
        # 1. Récupère les prix
        data = fetch_crypto_prices()

        # 2. Insère dans la base
        if data:
            insert_to_neon(data)
            run_transformations_on_neon()

        # 3. Attend 10 minutes
        time.sleep(interval_minutes * 60)
```

**Dites au prof:**
> "Voici le cœur du projet. C'est main_cloud.py qui tourne 24/7 sur Railway.
> Toutes les 10 minutes, il :
> 1. Appelle l'API CoinGecko
> 2. Insère les données dans Neon PostgreSQL
> 3. Lance les transformations SQL
>
> Ça fonctionne en boucle infinie, sans mon intervention!"

---

## ÉTAPE 3: Montrer l'orchestration (2 min)

### Ouvrir: `start_railway.py`

**Montrez:**
```python
from ingestion.main_cloud import run_pipeline_loop

if __name__ == "__main__":
    logger.info("🚀 Railway Scheduler - Crypto Data Pipeline 24/7")
    run_pipeline_loop(interval_minutes=10)
```

**Dites au prof:**
> "Ceci est le point d'entrée sur Railway. Railway exécute ce script automatiquement.
> Il importe et lance main_cloud.py, qui tourne sans arrêt."

**Montrez aussi:**
- `Procfile` → "Ce fichier dit à Railway: 'Exécute start_railway.py'"
- `runtime.txt` → "Et utilise Python 3.12"

---

## ÉTAPE 4: Montrer le dashboard (3 min)

### Ouvrir: `dashboard/app.py`

**Montrez les sections:**

```python
# CONFIG
st.set_page_config(
    page_title="Crypto Pipeline Dashboard",
    page_icon="₿",
    layout="wide"
)

# 5 ONGLETS
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Vue d'ensemble",
    "📈 Analyse technique",
    "⚠️ Alertes & Volume",
    "⚡ Streaming Kafka",
    "🔥 Spark Analytics"
])
```

**Dites au prof:**
> "Le dashboard a 5 onglets:
> 1. Vue d'ensemble - Aperçu rapide
> 2. Analyse technique - Pour les traders
> 3. Alertes - Variations détectées
> 4. Streaming - Flux temps réel
> 5. Spark - Analyses avancées
>
> Tout est mis à jour automatiquement depuis la base Neon!"

**Puis ouvrez le dashboard Streamlit dans un autre onglet et montrez:**
- Les graphiques
- Les cartes KPI
- Les données

---

## ÉTAPE 5: Montrer la base de données (2 min)

### Ouvrir: `ingestion/database.py`

**Montrez:**
```python
def create_tables():
    """Crée les tables PostgreSQL."""
    tables = {
        'raw_crypto_prices': '...',           # Données brutes
        'stream_crypto_prices': '...',        # Streaming
        'transform_hourly_avg': '...',        # Moyennes horaires
        'transform_daily_volume': '...',      # Volumes
        'transform_daily_ranking': '...',     # Classements
        'spark_volatility': '...',            # Volatilité
        'spark_dominance': '...',             # Dominance marché
    }
```

**Dites au prof:**
> "J'ai 7 tables:
> - 2 pour les données brutes (batch et streaming)
> - 5 pour les transformations (moyennes, volumes, rankings, analyses avancées)
>
> Tout est en Neon PostgreSQL Cloud, donc accessible 24/7."

---

## ÉTAPE 6: Montrer les dépendances (1 min)

### Ouvrir: `requirements.txt`

**Montrez:**
```
psycopg2-binary==2.9.9     ← PostgreSQL
streamlit==1.42.0           ← Dashboard
pandas==2.2.3               ← Transformations
pyspark==3.5.1              ← Big data
requests==2.31.0            ← API calls
plotly==5.24.0              ← Graphiques
prefect==3.0.0              ← Orchestration
```

**Dites au prof:**
> "J'utilise les meilleures librairies Python pour data engineering.
> Pandas pour les petits volumes, Spark pour les gros volumes."

---

## ÉTAPE 7: Montrer les logs (2 min)

### Montrez: `logs/pipeline.log` ou Railway dashboard

**Ouvrez le terminal et tapez:**
```bash
tail -20 logs/pipeline.log
```

**Ou allez sur:** https://railway.app (votre dashboard)

**Montrez:**
```
2026-04-14 14:30:00 | INFO | Pipeline started
2026-04-14 14:31:00 | INFO | 5 cryptos collected
2026-04-14 14:31:05 | INFO | Data inserted into Neon
2026-04-14 14:31:10 | INFO | Transformations completed
2026-04-14 14:31:15 | INFO | Next cycle in 10m 0s
```

**Dites au prof:**
> "Voici les logs. On peut voir que le pipeline s'exécute automatiquement toutes les 10 minutes.
> Chaque cycle:
> 1. Collecte 5 cryptos
> 2. Insère dans Neon
> 3. Lance les transformations
> 4. Attend 10 minutes
> 5. Relance...
>
> Zéro intervention humaine. C'est 100% automatisé!"

---

## ÉTAPE 8: Les résultats (2 min)

**Montrez le dashboard Streamlit:**

1. **Vue d'ensemble:**
   - "Voici les 5 cryptos actuelles"
   - "Graphique sur 30 jours d'historique"
   - "Classement automatique"

2. **Analyse technique:**
   - "Candlestick avec OHLC"
   - "Heatmap de corrélation"

3. **Alertes:**
   - "Détection automatique des variations > 5%"
   - "Historique complet"

4. **Spark Analytics:**
   - "Volatilité calculée en SQL"
   - "Dominance du marché"

**Dites au prof:**
> "Et voilà le résultat final! Un dashboard professionnel avec:
> - 3600+ lignes de vraies données
> - 5 onglets d'analyse
> - Graphiques interactifs
> - Mises à jour automatiques
> - Accessible 24/7 depuis n'importe où"

---

# 💬 RÉPONSES AUX QUESTIONS POSSIBLES

## Q: "C'est où que ça tourne?"
**R:** Sur Railway (un service cloud). Voici l'URL: `https://railway.app`

## Q: "Comment les données arrivent?"
**R:** Via CoinGecko API. Je fais une requête toutes les 10 minutes.

## Q: "Où sont les données?"
**R:** Dans Neon PostgreSQL Cloud (une base PostgreSQL sans serveur).

## Q: "Ça coûte combien?"
**R:**
- Railway: Gratuit (5$ de crédits/mois)
- Neon: Gratuit (plan gratuit)
- CoinGecko: Gratuit (API publique)
- Streamlit Cloud: Gratuit
- **Total: $0** (ou $5 si je veux plus de ressources)

## Q: "Comment tu as fait les transformations?"
**R:** Avec SQL dans PostgreSQL. Voir database.py. Pour les gros volumes, j'utilise PySpark.

## Q: "Et Kafka?"
**R:** C'est pour la démo locale uniquement. En production, je n'en ai pas besoin car le batch toutes les 10 minutes suffit.

## Q: "Comment tu sais que ça marche?"
**R:** Les logs (montrez), le dashboard (montrez), et le nombre de lignes dans la base (3600+).

---

# 🎯 POINTS À INSISTER

1. **Architecture production-ready** ✅
   - "C'est une vraie architecture qu'on trouve en entreprise"

2. **100% Automatisé** ✅
   - "Zero maintenance. Ça tourne tout seul 24/7"

3. **Données massives** ✅
   - "3600+ lignes avec 30 jours d'historique"
   - "Et ça collecte continuellement"

4. **Accessible partout** ✅
   - "Le dashboard est public, accessible de n'importe où"

5. **Gratuit** ✅
   - "Les services cloud sont gratuits"

6. **Scalable** ✅
   - "Architecture dual-engine: Pandas pour petit, Spark pour grand"

---

# 📝 SCRIPT POUR FINIR

**Dites au prof:**

> "En résumé, j'ai construit une pipeline data engineering complète:
>
> **Architecture:** CoinGecko → Ingestion Batch → Transformations SQL → Dashboard
>
> **Déploiement:** Tout tourne automatiquement sur Railway + Neon + Streamlit Cloud
>
> **Données:** 3600+ lignes d'historique + collecte continue 24/7
>
> **Interface:** Dashboard avec 5 onglets d'analyse professionnels
>
> **Compétences démontrées:**
> - Python (pandas, pyspark, requests, streamlit)
> - SQL (transformations complexes)
> - PostgreSQL (schema, queries)
> - Cloud (Railway, Neon, Streamlit Cloud)
> - Orchestration (Prefect)
> - Git + GitHub
>
> C'est une vraie solution qu'on pourrait mettre en production!"

---

**Bon courage! 🎉 Vous allez impressionner votre prof!**
