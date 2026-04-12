# 📊 Guide Complet du Projet Crypto Data Pipeline

## Table des Matières
1. [Introduction](#introduction)
2. [Concept Général](#concept-général)
3. [Les 5 Étapes du Pipeline](#les-5-étapes-du-pipeline)
4. [Architecture Générale](#architecture-générale)
5. [Technologies Utilisées](#technologies-utilisées)
6. [Structure des Fichiers](#structure-des-fichiers)
7. [Flux de Données Détaillé](#flux-de-données-détaillé)
8. [Spark : L'Intégration Intelligente](#spark--lintégration-intelligente)
9. [Le Dashboard Streamlit](#le-dashboard-streamlit)
10. [Déploiement Cloud](#déploiement-cloud)
11. [Résumé & Points Clés](#résumé--points-clés)

---

## Introduction

### Qu'est-ce qu'un Data Pipeline ?

Un **data pipeline** (pipeline de données) est une chaîne de processus automatisés qui collectent, traitent et livrent des données. Imaginez une chaîne de production dans une usine :

- **Entrée (Ingestion)** : les matières premières arrivent
- **Transformation** : les matières sont transformées en produits
- **Stockage** : les produits finis sont entreposés
- **Livraison (Visualisation)** : les produits sont livrés au client

Notre projet fait exactement cela, mais avec des **données de cryptomonnaies**.

### Résumé en 3 mots
- **QUOI** : Collecter et analyser les prix des cryptomonnaies en temps réel
- **POURQUOI** : Détecter les tendances, les anomalies et les opportunités d'achat/vente
- **COMMENT** : Utiliser Python, une base de données PostgreSQL, et un dashboard Streamlit interactif

---

## Concept Général

### L'Idée Centrale

Ce projet crée un **système d'analyse financière automatisé** qui :

1. **Récupère** les données des prix des cryptomonnaies (Bitcoin, Ethereum, etc.)
2. **Les stocke** dans une base de données PostgreSQL
3. **Les transforme** en statistiques utiles (volatilité, classements, alertes)
4. **Les affiche** via un dashboard web interactif et professionnel
5. **S'exécute automatiquement** selon un calendrier (chaque heure, chaque jour, etc.)

### Cas d'Usage Concrets

Avec ce pipeline, on peut :

- **Surveiller les prix** : voir l'évolution du Bitcoin et de l'Ethereum en temps réel
- **Détecter les alertes** : être averti quand un prix monte de plus de 5% ou baisse de plus de 3%
- **Analyser la volatilité** : savoir quelles cryptos sont les plus instables
- **Comparer les coins** : voir le classement des cryptos par market cap
- **Étudier les corrélations** : comprendre comment les prix des différentes cryptos se comportent ensemble
- **Analyser les volumes** : observer quand et comment le marché est actif

---

## Les 5 Étapes du Pipeline

### 🔄 Étape 1 : Ingestion (Collecte des Données)

**Qu'est-ce qu'on fait ?**
On récupère les données brutes des cryptomonnaies depuis une API.

**Comment ?**
- **API utilisée** : CoinGecko API (gratuite, sans clé API)
- **Données collectées** :
  - Prix actuel, haut du jour, bas du jour
  - Volume d'échange
  - Market cap (capitalisation boursière)
  - Variations (24h, 7j, 30j)
  - Horodatage précis

**Où est le code ?**
- `scripts/ingest.py` : récupère les données via l'API
- Exécution : `python scripts/ingest.py`

**Résultat**
Les données brutes sont sauvegardées dans les tables PostgreSQL :
- `prices` : historique des prix
- `market_data` : données de marché complètes
- `volumes` : données de volume d'échange

---

### 💾 Étape 2 : Stockage (Base de Données)

**Qu'est-ce qu'on fait ?**
On stocke les données dans une base de données structurée pour qu'elles soient persistantes et interrogeables.

**Comment ?**
- **Base de données** : PostgreSQL (Cloud Neon)
- **Tables principales** :
  ```
  prices
  ├── id (clé primaire)
  ├── coin_id (ex: "bitcoin")
  ├── current_price
  ├── high_24h, low_24h
  ├── market_cap
  ├── timestamp
  └── ...

  market_data
  ├── id
  ├── coin_id
  ├── market_cap
  ├── volume_24h
  ├── percentage_change_24h
  └── timestamp

  volumes
  ├── id
  ├── coin_id
  ├── volume
  ├── timestamp
  └── ...
  ```

**Avantages du PostgreSQL**
- ✅ Données persistantes (ne disparaissent pas)
- ✅ Requêtes rapides avec indexation
- ✅ Intégrité des données garantie
- ✅ Scalable (peut gérer des millions de lignes)
- ✅ Accessible depuis le cloud (Neon)

**Où est la connexion ?**
- `config/database.py` : gère la connexion PostgreSQL
- Utilise des variables d'environnement pour la sécurité (pas de mots de passe en dur)

---

### 🔧 Étape 3 : Transformation (Processing)

**Qu'est-ce qu'on fait ?**
On transforme les données brutes en statistiques utiles pour l'analyse.

**Comment ?**

#### 3.1 - Nettoyage des Données
**Fichier** : `transformations/clean.py`

```python
Données brutes
    ↓
Vérifier les valeurs nulles
    ↓
Supprimer les doublons
    ↓
Corriger les types (string → float, etc)
    ↓
Données propres
```

**Transformations appliquées** :
- Suppression des valeurs NULL
- Conversion des types (ex: "123.45" string → 123.45 float)
- Suppression des doublons
- Normalisation des timestamps

#### 3.2 - Agrégations (Statistiques Résumées)
**Fichier** : `transformations/aggregate.py`

Exemple : à partir de 1000 prix horaires du Bitcoin, on calcule :

```
Prix horaires (1000 lignes) :
2026-01-01 00:00:00 : 42,500.00
2026-01-01 01:00:00 : 42,510.50
2026-01-01 02:00:00 : 42,480.25
... (1000 lignes)
    ↓
Agrégation JOURNALIÈRE (1 ligne par jour) :
2026-01-01 :
  - Prix min : 42,480.25
  - Prix max : 42,510.50
  - Prix moyen : 42,495.75
  - Volume total : 15,234.5 BTC
```

**Transformations appliquées** :
- MIN, MAX, AVG (minimum, maximum, moyenne)
- SUM (somme des volumes)
- STDDEV (écart-type pour volatilité)

#### 3.3 - Classements (Rankings)
**Fichier** : `transformations/rank.py`

On classifie les cryptomonnaies par rapport à leurs performances :

```
Avant classement :
Bitcoin : 42,500.00
Ethereum : 2,250.00
Ripple : 0.55

Après classement (par prix) :
1. Bitcoin     : 42,500.00
2. Ethereum    : 2,250.00
3. Ripple      : 0.55

Et aussi (par volume) :
1. Ethereum    : 25 milliards
2. Bitcoin     : 23 milliards
3. Solana      : 8 milliards
```

**Méthodes utilisées** :
- `ROW_NUMBER()` : numération simple
- `RANK()` : avec gestion des égalités
- Window functions pour les calculs relatifs

#### 3.4 - Détection d'Alertes
**Fichier** : `transformations/alerts.py`

On détecte les événements importants :

```
Bitcoin hier  : 42,000
Bitcoin aujourd'hui : 44,100

Variation : (44,100 - 42,000) / 42,000 × 100 = 5%
    ↓
5% > 5% ? OUI
    ↓
ALERTE HAUSSE ✅ (green badge)

Ethereum hier  : 2,250
Ethereum aujourd'hui : 2,180

Variation : (2,180 - 2,250) / 2,250 × 100 = -3.1%
    ↓
-3.1% < -3% ? OUI
    ↓
ALERTE BAISSE ⚠️ (red badge)
```

**Seuils d'alerte**:
- HAUSSE : variation > +5%
- BAISSE : variation < -3%
- NORMAL : entre -3% et +5%

#### 3.5 - Calculs Avancés (Spark Analytics)
**Fichier** : `transformations/spark_transformations.py`

Avec Spark, on peut faire des calculs complexes sur l'historique entier :

```
VOLATILITÉ (Écart-type des prix) :
Bitcoin sur 30 jours :
  - Prix min : 40,000
  - Prix max : 45,000
  - Prix moyen : 42,500
  - Écart-type : 1,200
  - Volatilité % = (1,200 / 42,500) × 100 = 2.8%

DOMINANCE DE MARCHÉ (% du marché total) :
Total crypto : 2 trillion USD
Bitcoin : 800 milliards
Dominance Bitcoin = (800B / 2T) × 100 = 40%

HEURE DE POINTE (quand le volume est maximal) :
Volume par heure :
  10:00-11:00 : 2.5M BTC
  11:00-12:00 : 3.2M BTC  ← HEURE DE POINTE
  12:00-13:00 : 2.1M BTC
```

---

### 🤖 Étape 4 : Automatisation (Exécution Programmée)

**Qu'est-ce qu'on fait ?**
On lance le pipeline automatiquement à intervalles réguliers.

**Comment ?**

#### Option 1 : Prefect (Orchestration Locale)
**Fichier** : `prefect_flows.py`

```python
from prefect import flow, task

@flow(name="crypto_pipeline")
def crypto_pipeline():
    data = ingest_data()        # Récupère les données
    cleaned = clean_data(data)  # Les nettoie
    stats = aggregate(cleaned)  # Calcule les stats
    save_to_db(stats)          # Les sauvegarde

# Schedule : chaque heure
crypto_pipeline.serve(
    interval=3600  # 3600 secondes = 1 heure
)
```

**Avantages**:
- ✅ Gestion des erreurs et retry automatiques
- ✅ Logs détaillés et monitoring
- ✅ Interface Web pour voir l'historique
- ✅ Prévention des executions simultanées

#### Option 2 : Streamlit Cloud (Déploiement Cloud)
Sur Streamlit Cloud :
- Le script s'exécute à chaque accès
- Un système de cache évite les appels répétés
- On peut ajouter un bouton "Rafraîchir manuellement"

**Configuration**:
```
Utilisateur accède à : https://crypto-data-pipeline.streamlit.app/
    ↓
Streamlit exécute app.py
    ↓
Données affichées (mise en cache)
    ↓
Prochain refresh : 60 minutes ou clic manuel
```

---

### 📊 Étape 5 : Visualisation (Dashboard)

**Qu'est-ce qu'on fait ?**
On affiche les données et analyses de façon claire et interactive.

**Comment ?**
- **Outil** : Streamlit + Plotly
- **Lieu de déploiement** : Cloud Streamlit (https://crypto-data-pipeline.streamlit.app/)

**Les 5 Onglets du Dashboard**

#### Onglet 1️⃣ : Vue d'Ensemble (📊)
Affiche :
- **KPI Cards** : les 4 principales métriques
  - Prix BTC actuel
  - Prix ETH actuel
  - Nombre de cryptos suivies
  - Volatilité moyenne

- **Évolution des Prix** : graphique ligne interactif
  - Montre les prix historiques des 30 derniers jours
  - Zoom et navigation interactifs

- **Classement Journalier** : tableau des meilleures performances
  - Quels coins ont le plus monté aujourd'hui
  - Quels coins ont le plus baissé

#### Onglet 2️⃣ : Analyse Technique (📈)
Affiche :
- **Graphique Candlestick** : OHLC (Open, High, Low, Close) horaire
  - Bougies vertes = prix montant
  - Bougies rouges = prix baissant
  - Très utilisé par les traders

- **Matrice de Corrélation** : heatmap montrant comment les prix sont liés
  - 1.0 = parfaitement corrélés (montent/baissent ensemble)
  - 0.0 = pas corrélés
  - -1.0 = inversement corrélés

- **Performance Relative** : base 100
  - Si Bitcoin part de 100, à jour N il est à 105 = +5%
  - Permet de comparer coins avec prix très différents

#### Onglet 3️⃣ : Alertes & Volume (⚠️)
Affiche :
- **Badges d'Alertes** :
  - 🟢 Vert = NORMAL (pas d'alerte)
  - 🔴 Rouge = HAUSSE (variation > +5%)
  - 🟡 Jaune = BAISSE (variation < -3%)

- **Variations en Barres** : comparaison visuelle des changes 24h/7j/30j

- **Graphique de Volume** : aire chart montrant l'activité
  - Pics = moment d'haute activité
  - Creux = marché calme

#### Onglet 4️⃣ : Streaming Kafka (⚡)
Affiche (si Kafka est activé) :
- **Graphique Scatter** : prix des cryptos en temps quasi-réel

- **Histogramme de Variations** : distribution des mouvements de prix

- **Tableau d'Événements** : stream en direct des événements Kafka
  - Nouveaux prix arrivant
  - Horodatage précis

#### Onglet 5️⃣ : Spark Analytics (🔥)
Affiche (données Spark) :
- **Volatilité par Crypto** : graphique barre
  - Montre l'écart-type des prix
  - Cryptos stables vs volatiles

- **Dominance de Marché** : graphique donut
  - % du marché total pour chaque crypto
  - Bitcoin généralement dominant (40-50%)

- **Meilleure Heure** : quand le volume est maximum

---

## Architecture Générale

### Vue d'Ensemble du Système

```
┌─────────────────────────────────────────────────────────────┐
│                    CRYPTO DATA PIPELINE                     │
└─────────────────────────────────────────────────────────────┘

                            🌐 ENTRÉES

                    CoinGecko API (gratuit)
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                      INGESTION LAYER                        │
│  scripts/ingest.py → Récupère les prix + données brutes    │
└─────────────────────────────────────────────────────────────┘
                            ↓
                      (données brutes)
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                    STOCKAGE (DATABASE)                      │
│  PostgreSQL (Cloud Neon)                                    │
│  Tables: prices, market_data, volumes                      │
└─────────────────────────────────────────────────────────────┘
                            ↓
                      (données brutes)
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                  TRANSFORMATION LAYER                       │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ Avec PANDAS (< 50k lignes) :                        │   │
│  │ • clean.py → nettoyage                             │   │
│  │ • aggregate.py → agrégations (min, max, avg)       │   │
│  │ • rank.py → classements                            │   │
│  │ • alerts.py → détection d'anomalies                │   │
│  └─────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ Avec SPARK (≥ 50k lignes) :                         │   │
│  │ • spark_transformations.py                         │   │
│  │ • Window functions (ROW_NUMBER, RANK)              │   │
│  │ • Spark SQL (STDDEV, SUM, GROUP BY)                │   │
│  │ • Advanced Analytics (volatilité, dominance)       │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                            ↓
                    (données transformées)
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                 AUTOMATISATION (SCHEDULER)                  │
│  Prefect (local) OU Streamlit Cloud (auto-exécution)       │
│  Fréquence : toutes les heures                            │
└─────────────────────────────────────────────────────────────┘
                            ↓
                      (exécution programmée)
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                 VISUALISATION (DASHBOARD)                   │
│  Streamlit + Plotly                                        │
│  URL: https://crypto-data-pipeline.streamlit.app/         │
│  5 onglets avec graphiques interactifs                    │
└─────────────────────────────────────────────────────────────┘
                            ↓
                      👥 UTILISATEURS

```

### Flux de Données Complet

```
Jour 0 (lundi 00:00) :
  ├─ ingest.py récupère les prix
  ├─ Sauvegarde dans PostgreSQL
  ├─ Transformation (Pandas ou Spark)
  └─ Dashboard mis à jour ✅

Jour 0 (lundi 01:00) :
  ├─ Nouveau cycle automatique
  ├─ Nouvelles données
  ├─ Nouvelles analyses
  └─ Dashboard rafraîchi ✅

... (répété 24 fois par jour) ...

Jour 7 (dimanche 23:00) :
  └─ Maintenant on a 7 jours × 24 cycles = 168 données par crypto
     → Assez pour des analyses statistiques significatives
```

---

## Technologies Utilisées

### Le Stack Technique Complet

| Couche | Technologie | Rôle | Raison du Choix |
|--------|-------------|------|-----------------|
| **API Source** | CoinGecko API | Récupérer les prix crypto | Gratuit, sans clé API, données fiables |
| **Langage Principal** | Python 3.12 | Tout le code | Flexible, riche écosystème data |
| **Traitement Léger** | Pandas 2.2 | Transformation (<50k lignes) | Rapide, intuitif, colonnes typées |
| **Traitement Lourd** | PySpark 3.5 | Transformation (≥50k lignes) | Parallélisé, scalable, SQL native |
| **Base de Données** | PostgreSQL (Neon) | Stocker les données | ACID, requêtes rapides, cloud |
| **Message Queue** | Apache Kafka | Streaming temps réel (optionnel) | Événements continus, scalable |
| **Orchestration** | Prefect | Scheduler & Monitoring | Gestion des erreurs, logs, UI |
| **Dashboard** | Streamlit 1.42 | Interface utilisateur | Rapide, Python natif, cloud-ready |
| **Graphiques** | Plotly 5.24 | Visualisations interactives | Zoom, hover, export |
| **ORM Database** | SQLAlchemy 2.0 | Requêtes SQL sécurisées | Évite les injections SQL |
| **Requêtes HTTP** | Requests 2.32 | Appels API | Simple, stable |
| **Communication DB** | psycopg2 2.9 | Driver PostgreSQL | Optimisé, thread-safe |

### Pourquoi Ces Choix ?

#### Python 3.12
- ✅ Idéal pour la data science
- ✅ Pandas, Spark, Streamlit tous en Python
- ✅ Développement rapide

#### Pandas vs Spark (Architecture Hybride)
```
< 50k lignes      →   PANDAS
  Rapide, pas de surcharge

≥ 50k lignes      →   SPARK
  Parallélisé, scalable, prêt production
```

**Avantage** : On n'utilise Spark que si nécessaire = pas de lenteur inutile

#### PostgreSQL Cloud (Neon)
```
Local: données disparaissent au redémarrage
Cloud: données persistantes, accessible partout, backups automatiques
```

#### Streamlit pour le Dashboard
- ✅ Code Python simple (pas de HTML/CSS/JavaScript compliqué)
- ✅ Déploiement gratuit sur Streamlit Cloud
- ✅ Recharge automatique à chaque modification
- ✅ Intégration native Plotly

#### Plotly pour les Graphiques
- ✅ Interactif (zoom, hover, sauvegarde)
- ✅ Candlestick, heatmap, scatter, donut inclus
- ✅ Thème dark professionnel

---

## Structure des Fichiers

### Organisation du Projet

```
crypto_data_project/
│
├── 📄 README.md                          # Documentation générale
├── 📄 requirements.txt                   # Dépendances Python
│
├── 🔧 config/
│   ├── __init__.py
│   ├── database.py                       # Connexion PostgreSQL
│   └── constants.py                      # Constantes (seuils alertes, etc)
│
├── 📥 scripts/
│   ├── __init__.py
│   ├── ingest.py                         # Récupère les données via API
│   └── cli.py                            # Interface ligne de commande
│
├── 🔄 transformations/
│   ├── __init__.py
│   ├── clean.py                          # Nettoyage des données
│   ├── aggregate.py                      # Agrégations (min, max, avg)
│   ├── rank.py                           # Classements
│   ├── alerts.py                         # Détection d'alertes
│   ├── spark_engine.py                   # Gestionnaire SparkSession
│   ├── spark_transformations.py          # Transformations Spark avancées
│   └── run_transforms.py                 # Exécuteur principal (Pandas ou Spark)
│
├── 📊 dashboard/
│   ├── __init__.py
│   └── app.py                            # Interface Streamlit (5 onglets)
│
├── 🔀 prefect_flows.py                   # Orchestration avec Prefect
│
├── 📋 rapport.tex                        # Rapport technique LaTeX
│
└── 🐳 .streamlit/
    └── config.toml                       # Configuration Streamlit
```

### Rôle de Chaque Fichier

#### `config/database.py`
```python
# Gère la connexion PostgreSQL
from_env = os.getenv("DATABASE_URL")
connection = create_engine(from_env)

# Utilisation :
df = pd.read_sql("SELECT * FROM prices", connection)
df.to_sql("prices", connection, if_exists="append")
```

#### `scripts/ingest.py`
```python
# Récupère les données via CoinGecko API
response = requests.get(
    "https://api.coingecko.com/api/v3/simple/price",
    params={
        "ids": "bitcoin,ethereum,ripple",
        "vs_currencies": "usd",
        "include_market_cap": True,
        "include_24hr_vol": True
    }
)
# Sauvegarde dans PostgreSQL
```

#### `transformations/clean.py`
```python
def clean_prices(df):
    """Nettoie les données brutes"""
    df = df.dropna()                  # Supprime les NULL
    df['price'] = df['price'].astype(float)  # Conversion type
    df = df.drop_duplicates()         # Supprime doublons
    return df
```

#### `transformations/spark_engine.py`
```python
def get_spark():
    """Crée ou retourne la SparkSession singleton"""
    # Configuration Windows-safe (local[1], Arrow disabled)
    spark = SparkSession.builder
        .appName("CryptoDataPipeline")
        .master("local[1]")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .getOrCreate()
    return spark

def auto_select_engine(nb_rows, threshold=50_000):
    """Sélectionne Pandas ou Spark automatiquement"""
    return "spark" if nb_rows >= threshold else "pandas"
```

#### `transformations/spark_transformations.py`
```python
def spark_sql_advanced_analysis(sdf):
    """Calculs avancés avec Spark SQL"""

    # Volatilité (écart-type des prix)
    volatility = sdf.sql("""
        SELECT coin_id,
               STDDEV(current_price) as std_price,
               AVG(current_price) as avg_price,
               (STDDEV(current_price) / AVG(current_price)) * 100 as volatility_pct
        FROM prices
        GROUP BY coin_id
    """)

    # Dominance de marché
    dominance = sdf.sql("""
        SELECT coin_id,
               market_cap,
               (market_cap / SUM(market_cap) OVER()) * 100 as dominance_pct
        FROM market_data
    """)

    return volatility, dominance
```

#### `dashboard/app.py`
```python
import streamlit as st
import plotly.graph_objects as go

st.set_page_config(page_title="Crypto Dashboard", layout="wide")

# Thème dark
st.markdown("""<style>
    body { background-color: #0f1117; color: #c8d8f0; }
</style>""", unsafe_allow_html=True)

# 5 onglets
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Vue d'ensemble",
    "📈 Analyse technique",
    "⚠️ Alertes & Volume",
    "⚡ Streaming Kafka",
    "🔥 Spark Analytics"
])

with tab1:
    st.metric("BTC", "$42,500")
    # ... plus de contenu

# Exécution sur Streamlit Cloud
if __name__ == "__main__":
    app()
```

#### `prefect_flows.py`
```python
from prefect import flow, task
import schedule
import time

@task
def ingest_task():
    return ingest()  # Récupère les données

@task
def transform_task(data):
    return transform(data)  # Les transforme

@task
def save_task(results):
    save_to_db(results)  # Les sauvegarde

@flow
def crypto_pipeline():
    """Pipeline complet"""
    data = ingest_task()
    results = transform_task(data)
    save_task(results)

# Scheduler : exécute chaque heure
if __name__ == "__main__":
    crypto_pipeline.serve(interval=3600)
```

---

## Flux de Données Détaillé

### Exemple Concret : Une Journée du Pipeline

#### 08:00 - Démarrage du Pipeline

```
[08:00:00] ✅ Prefect vérifie l'horaire
[08:00:01] ✅ Déclenche crypto_pipeline()

╔════════════════════════════════════════╗
║     PHASE 1 : INGESTION               ║
╚════════════════════════════════════════╝

[08:00:02] → Appel CoinGecko API
[08:00:03] ← Réponse JSON reçue (50 KB)

Données brutes reçues :
{
  "bitcoin": {
    "usd": 42500,
    "market_cap": 820000000000,
    "volume_24h": 25000000000
  },
  "ethereum": {
    "usd": 2250,
    "market_cap": 270000000000,
    "volume_24h": 12000000000
  },
  ...
}
```

#### 08:00:05 - Stockage

```
╔════════════════════════════════════════╗
║     PHASE 2 : STOCKAGE                ║
╚════════════════════════════════════════╝

[08:00:04] → Connexion à PostgreSQL
[08:00:05] ✅ Connecté

[08:00:06] INSERT INTO prices VALUES:
  (bitcoin, 42500.00, 2026-04-06 08:00:00)
  (ethereum, 2250.00, 2026-04-06 08:00:00)
  ... (20 coins)

[08:00:07] ✅ Insertion réussie (20 lignes)
```

#### 08:00:10 - Transformation (Auto-Sélection)

```
╔════════════════════════════════════════╗
║     PHASE 3 : TRANSFORMATION          ║
╚════════════════════════════════════════╝

[08:00:08] → Chargement des données
[08:00:08] → COUNT(*) = 8,760 lignes (30 jours × 24 heures)

[08:00:09] Sélection moteur :
  8,760 < 50,000 ?  OUI
  → PANDAS (rapide et léger) ✅

[08:00:10] Nettoyage :
  ├─ Vérification NULL : 0 trouvées ✅
  ├─ Doublons : 0 trouvées ✅
  ├─ Conversion types : OK ✅
  └─ Résultat : 8,760 lignes propres

[08:00:12] Agrégations (groupby jour) :
  ├─ Bitcoin :
  │   ├─ 2026-04-01 : min=41900, max=42600, avg=42250
  │   ├─ 2026-04-02 : min=42100, max=42800, avg=42450
  │   └─ ... (30 jours)
  ├─ Ethereum :
  │   └─ ... (30 jours)
  └─ ... (autres coins)

  Résultat : 30 jours × 20 coins = 600 lignes résumées

[08:00:15] Classements (ROW_NUMBER) :
  Pour le jour 2026-04-06 :
  1. Bitcoin : +2.5% (meilleures performances)
  2. Ethereum : +1.8%
  3. Ripple : +1.2%
  ...
  20. [worst coin] : -3.1%

[08:00:17] Détection d'Alertes :
  Bitcoin : hier 41500 → aujourd'hui 42500
  Variation : (42500-41500)/41500 = 2.4%
  Résultat : NORMAL (entre -3% et +5%) 🟢

  Ethereum : hier 2300 → aujourd'hui 2220
  Variation : (2220-2300)/2300 = -3.5%
  Résultat : ALERTE BAISSE 🔴 (< -3%)

[08:00:20] ✅ Transformation terminée
```

#### 08:00:25 - Sauvegarde des Résultats

```
╔════════════════════════════════════════╗
║     PHASE 4 : SAUVEGARDE              ║
╚════════════════════════════════════════╝

[08:00:21] INSERT INTO daily_aggregates :
  600 lignes résumées

[08:00:22] INSERT INTO rankings :
  30 lignes de classements

[08:00:23] INSERT INTO alerts :
  3 alertes (2 NORMAL, 1 BAISSE)

[08:00:25] ✅ Sauvegarde réussie
```

#### 08:00:30 - Mise à Jour du Dashboard

```
╔════════════════════════════════════════╗
║     PHASE 5 : VISUALISATION           ║
╚════════════════════════════════════════╝

[08:00:26] Streamlit Cloud vérifie le cache
[08:00:27] Cache expiré ? OUI (60 minutes) ✅
[08:00:28] Recharge les données

Dashboard chargé avec :
  ✅ KPI Cards mis à jour
  ✅ Graphiques rafraîchis
  ✅ Classements actualisés
  ✅ Alertes visibles
  ✅ Analytics Spark calculées

[08:00:30] 🎉 Utilisateur voit les dernières données
```

#### 09:00 - Cycle Suivant

```
[09:00:00] Le pipeline recommence (cycle 2)
  → Nouvelles données collectées
  → Transformations répétées
  → Dashboard rafraîchi automatiquement

Jour après jour, heure après heure... 🔄
```

---

## Spark : L'Intégration Intelligente

### Pourquoi Spark pour un Petit Projet ?

**Le Dilemme** :
> "Mon prof veut que j'utilise Spark, mais j'ai seulement 5000 lignes de données !"

**La Solution** : Architecture hybride **Pandas + Spark**

### Le Concept de Sélection Automatique

```
Flux de données :
  Ingestion → PostgreSQL → Transformation
                              ↓
                      Combien de lignes ?
                         ↙    ↘
                    < 50k      ≥ 50k
                      ↓          ↓
                   PANDAS      SPARK
                   Rapide      Scalable
```

### Spark dans Notre Projet

#### Fichier : `transformations/spark_engine.py`

```python
def get_spark():
    """Crée une SparkSession singleton"""
    spark = SparkSession.builder
        .appName("CryptoDataPipeline")
        .master("local[1]")  # Mode local (un seul processus)
        .config("spark.sql.shuffle.partitions", "2")  # Petit volume
        .config("spark.driver.memory", "1g")  # 1GB RAM
        # ── Désactivations nécessaires sur Windows ──
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")  # Évite crash
        .config("spark.ui.enabled", "false")  # Pas de UI Web
        .getOrCreate()
    return spark

def auto_select_engine(nb_rows, threshold=50_000):
    """
    Sélectionne automatiquement :
    - PANDAS si nb_rows < 50 000
    - SPARK si nb_rows ≥ 50 000
    """
    engine = "spark" if nb_rows >= threshold else "pandas"
    logger.info(f"{nb_rows} lignes → {engine.upper()}")
    return engine
```

**Stratégie Windows**:
- `local[1]` = 1 thread (pas de conflit processus Python)
- Arrow désactivé = évite les crashes Python 3.12
- Fichiers CSV temporaires = pas de sérialisation Python

#### Fichier : `transformations/spark_transformations.py`

**Problème**: Les transformations avec Spark?

```python
def pandas_to_spark(df, spark):
    """Convertit Pandas → Spark via fichier CSV"""
    # ❌ WRONG : sdf = spark.createDataFrame(df)
    #    → Crash Python worker sur Windows

    # ✅ RIGHT : utiliser fichier CSV
    tmp_csv = "/tmp/temp_dataframe.csv"
    df.to_csv(tmp_csv, index=False)
    sdf = spark.read.csv(tmp_csv, header=True, inferSchema=True)

    # Pourquoi ? JVM Spark lit le fichier directement
    # Python workers n'ont jamais à sérialiser les données
    return sdf
```

**Transformations Spark SQL**:

```python
def spark_sql_advanced_analysis(sdf):
    """Analyses avancées impossibles en Pandas simple"""

    # VOLATILITÉ (écart-type) :
    volatility = spark.sql("""
        SELECT coin_id,
               ROUND(STDDEV(current_price), 2) as std_dev,
               ROUND(AVG(current_price), 2) as avg_price,
               ROUND((STDDEV(current_price) / AVG(current_price)) * 100, 2) as volatility_pct
        FROM prices
        WHERE timestamp >= DATE_SUB(CURRENT_DATE(), 30)
        GROUP BY coin_id
        ORDER BY volatility_pct DESC
    """)

    # DOMINANCE (% du marché) :
    dominance = spark.sql("""
        SELECT coin_id,
               ROUND(market_cap, 0) as market_cap,
               ROUND((market_cap / SUM(market_cap) OVER()) * 100, 2) as dominance_pct
        FROM market_data
        WHERE timestamp = (SELECT MAX(timestamp) FROM market_data)
        ORDER BY dominance_pct DESC
    """)

    return volatility, dominance
```

### Pourquoi C'est Intelligent ?

| Scenario | Ancien | Nouveau |
|----------|--------|---------|
| 5K lignes | Spark (lent, complexe) ❌ | Pandas (rapide) ✅ |
| 50K lignes | Pandas (OUT OF MEMORY) ❌ | Spark (parallélisé) ✅ |
| 500K lignes | Spark nécessaire ✅ | Spark nécessaire ✅ |
| **Production** | Scalable avec Spark ✅ | **Déjà Spark pour >50K** ✅ |

---

## Le Dashboard Streamlit

### Architecture Générale

```
app.py (640 lignes)
  ├─ Configuration : page, thème, CSS
  ├─ Sidebar : options, rafraîchissement
  ├─ 5 Onglets (st.tabs)
  │   ├─ Tab 1 : Vue d'ensemble
  │   ├─ Tab 2 : Analyse technique
  │   ├─ Tab 3 : Alertes
  │   ├─ Tab 4 : Streaming Kafka
  │   └─ Tab 5 : Spark Analytics
  └─ Caching : @st.cache_data (60s)
```

### Thème Dark Professionnel

```python
dark_css = """
<style>
    body {
        background-color: #0f1117;      /* Noir profond */
        color: #c8d8f0;                 /* Bleu clair */
    }
    .metric { color: #ffffff; }         /* Blanc pour KPIs */
    .alert-good { color: #00e0a0; }    /* Vert */
    .alert-bad { color: #ff5c6c; }     /* Rouge */
    .alert-warn { color: #ffd700; }    /* Jaune */
</style>
"""
st.markdown(dark_css, unsafe_allow_html=True)
```

### Onglet 1 : Vue d'Ensemble

```python
with tab1:
    # KPI Cards (4 colonnes)
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("BTC", "$42,500", "↑ 2.5%")
    col2.metric("ETH", "$2,250", "↓ -1.2%")
    col3.metric("Cryptos", "20", "")
    col4.metric("Volatilité Moy", "3.2%", "")

    # Graphique ligne (30 jours)
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=dates, y=btc_prices, name="Bitcoin"))
    fig.add_trace(go.Scatter(x=dates, y=eth_prices, name="Ethereum"))
    st.plotly_chart(fig, use_container_width=True)

    # Tableau classement
    st.dataframe(top_performers)
```

### Onglet 2 : Analyse Technique

```python
with tab2:
    # Candlestick OHLC (24h hourly)
    fig = go.Figure(data=[
        go.Candlestick(
            x=hourly_times,
            open=open_prices,
            high=high_prices,
            low=low_prices,
            close=close_prices,
            name="Bitcoin (hourly)"
        )
    ])
    st.plotly_chart(fig)

    # Heatmap corrélation
    corr_matrix = df[['bitcoin', 'ethereum', 'ripple']].corr()
    fig = go.Figure(data=go.Heatmap(z=corr_matrix.values))
    st.plotly_chart(fig)

    # Performance relative (base 100)
    btc_perf = (btc_prices / btc_prices[0]) * 100
    eth_perf = (eth_prices / eth_prices[0]) * 100
    # Plot...
```

### Onglet 3 : Alertes & Volume

```python
with tab3:
    # Badges d'alertes
    col1, col2, col3 = st.columns(3)
    col1.markdown("🟢 Bitcoin : NORMAL")
    col2.markdown("🔴 Ethereum : HAUSSE (+2.8%)")
    col3.markdown("🟡 Ripple : BAISSE (-3.1%)")

    # Variations (24h, 7j, 30j)
    variations_df = pd.DataFrame({
        'Crypto': ['Bitcoin', 'Ethereum', 'Ripple'],
        '24h': [2.5, -1.2, -3.1],
        '7j': [5.2, 3.1, -2.0],
        '30j': [15.3, 8.5, 1.2]
    })
    st.dataframe(variations_df)

    # Volume (area chart)
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=dates, y=volumes, fill='tozeroy'))
    st.plotly_chart(fig)
```

### Onglet 4 : Streaming Kafka (Optionnel)

```python
with tab4:
    # Scatter des prix temps réel
    fig = go.Figure(data=go.Scatter(
        x=prices,
        y=variations,
        mode='markers',
        marker=dict(size=volumes/1e6)
    ))
    st.plotly_chart(fig)

    # Histogramme des variations
    fig = go.Figure(data=[
        go.Histogram(x=all_variations, nbinsx=30)
    ])
    st.plotly_chart(fig)

    # Tableau événements Kafka
    st.dataframe(kafka_events)
```

### Onglet 5 : Spark Analytics

```python
with tab5:
    # Volatilité (calculée via Spark SQL)
    vol_df = load_volatility_data()  # FROM PostgreSQL (Spark a sauvegardé)

    fig = go.Figure(data=[
        go.Bar(x=vol_df['coin_id'], y=vol_df['volatility_pct'])
    ])
    st.plotly_chart(fig)

    # Dominance (donut chart)
    dom_df = load_dominance_data()

    fig = go.Figure(data=[
        go.Pie(labels=dom_df['coin_id'], values=dom_df['dominance_pct'])
    ])
    st.plotly_chart(fig)

    # Meilleure heure du jour
    st.metric("Heure de pointe", "11:00-12:00", "Volume max")
```

### Caching pour Performance

```python
@st.cache_data(ttl=3600)  # Cache 1 heure
def load_prices():
    """Charge les prix (ne s'exécute qu'une fois par heure)"""
    return pd.read_sql("SELECT * FROM prices", conn)

@st.cache_data(ttl=300)   # Cache 5 minutes
def load_alerts():
    """Charge les alertes (rafraîchies plus souvent)"""
    return pd.read_sql("SELECT * FROM alerts", conn)
```

---

## Déploiement Cloud

### De Local à Production

```
PHASE 1 : Développement Local
  ├─ Code : VS Code / PyCharm
  ├─ Données : PostgreSQL local (ou Neon dev)
  ├─ Spark : local[*] sur ma machine
  ├─ Dashboard : localhost:8501
  └─ Test : `streamlit run dashboard/app.py`

                    ↓ (Git push)

PHASE 2 : Déploiement Streamlit Cloud
  ├─ Repo : GitHub (crypto_data_project)
  ├─ Streamlit Cloud : https://share.streamlit.io
  ├─ Configuration : .streamlit/config.toml
  ├─ Secrets : DATABASE_URL (variables d'environnement)
  ├─ Exécution : auto à chaque reload
  └─ URL : https://crypto-data-pipeline.streamlit.app/
```

### Configuration Streamlit Cloud

```toml
# .streamlit/config.toml
[client]
showErrorDetails = true

[theme]
primaryColor = "#2E75B5"
backgroundColor = "#0f1117"
secondaryBackgroundColor = "#161b22"
textColor = "#c8d8f0"
font = "sans serif"

[server]
maxUploadSize = 200
enableCORS = false
```

### Secrets (Variables d'Environnement)

```
Streamlit Cloud Dashboard → Secrets
├─ DATABASE_URL = postgresql://user:pass@host:5432/db
├─ COINGECKO_API_KEY = (pas nécessaire, API libre)
└─ PREFECT_API_URL = (optionnel si Prefect Cloud)
```

### Limitations Cloud Streamlit

| Limite | Valeur | Impact |
|--------|--------|--------|
| Stockage disque | 1 GB | OK pour nos données |
| Mémoire RAM | 1 GB | OK (pas de gros Spark local) |
| CPU | 1 core | OK (Streamlit est léger) |
| Temps exécution | 30 min | OK (notre pipeline = 30s) |
| Requêtes entrantes | Illimitées | ✅ |

### Pourquoi Pas de Spark sur Cloud ?

```
Spark sur Streamlit Cloud ?

✅ AVANTAGES :
  - Analyses avancées locales
  - Pas d'appels API distants

❌ INCONVÉNIENTS :
  - Spark = 500+ MB à télécharger
  - Dépasserait mémoire 1GB
  - Lenteur intolerable
  - Coût compute élevé

✅ SOLUTION (ce qu'on fait) :
  - Spark tourne LOCAL (sur ma machine)
  - Résultats sauvegardés dans PostgreSQL
  - Dashboard les récupère via SQL
  - Cloud ne charge que les résultats (léger)
```

**Exemple Concret**:

```python
# LOCAL (ma machine avec Spark 3.5)
from spark_engine import get_spark

sdf = spark.read.parquet("prices.parquet")
volatility = sdf.sql("SELECT ... STDDEV(...) ...")
volatility.write.mode("overwrite").option(
    "url", DATABASE_URL
).jdbc(..., "spark_volatility", ...)

# CLOUD (Streamlit App)
@st.cache_data(ttl=3600)
def load_volatility():
    # Juste une requête SQL simple
    return pd.read_sql(
        "SELECT * FROM spark_volatility",
        conn
    )
```

---

## Résumé & Points Clés

### Les 3 Piliers du Projet

#### 1️⃣ **Collecte Automatisée**
- CoinGecko API récupère les prix
- Hourly ingestion (toutes les heures)
- PostgreSQL stocke tout persistemment
- Perfect orchestr l'exécution

#### 2️⃣ **Traitement Intelligent**
- **< 50k lignes** : Pandas (rapide)
- **≥ 50k lignes** : Spark (scalable)
- 5 transformations : clean, aggregate, rank, alerts, analytics
- Spark SQL pour calculs avancés

#### 3️⃣ **Visualisation Professionnelle**
- Streamlit Dashboard (Python native)
- 5 onglets avec graphiques Plotly interactifs
- Thème dark moderne (#0f1117, #c8d8f0)
- Déploiement cloud gratuit

### Flux Simplifié

```
CoinGecko API
    ↓
Pandas/Spark Transformation
    ↓
PostgreSQL (persistance)
    ↓
Streamlit Dashboard (visualisation)
    ↓
Utilisateur voit les données 🎉
```

### Problèmes Résolus

| Problème | Solution |
|----------|----------|
| "Comment utiliser Spark avec peu de données ?" | Sélection automatique (> 50k threshold) |
| "Spark crash sur Windows ?" | Conversion via CSV, pas direct Python |
| "Texte invisible sur fond dark ?" | Couleurs explicites (#c8d8f0, #ffffff) |
| "Graphiques sans zoom/interact ?" | Plotly native (hover, zoom, sauvegarde) |
| "Spark Analytics sur Cloud ?" | Calculé local, résultats sauvegardés dans PostgreSQL |

### Chiffres du Projet

- **Sources données** : 1 (CoinGecko API)
- **Cryptomonnaies suivies** : 20+ coins
- **Fréquence collecte** : 1 fois par heure (24 cycles/jour)
- **Tables PostgreSQL** : 5 tables principales
- **Transformations** : 5 transformations différentes
- **Onglets dashboard** : 5 tabs interactifs
- **Lignes de code** : ~2000 lignes Python
- **Technologies stack** : 12 principaux outils

### Capacité de Scalabilité

```
Jour 1 :    24 data points × 20 coins = 480 lignes
Mois 1 :    720 data points × 20 coins = 14,400 lignes
Année 1 :   8,760 data points × 20 coins = 175,200 lignes
                         ↓
                  SEUIL SPARK (50k)
                         ↓
           Bascule automatique à Spark ✅
           (aucun code à modifier)
```

### Architecture Prête Production

- ✅ Authentification sécurisée (env variables)
- ✅ Gestion erreurs (try/catch, Prefect retry)
- ✅ Logging (fichiers + Prefect UI)
- ✅ Caching (Streamlit cache 1h)
- ✅ Scalabilité (Pandas→Spark automatique)
- ✅ Cloud-ready (PostgreSQL Cloud, Streamlit Cloud)
- ✅ Monitoring (KPI, alertes, dashboards)

---

## Prochaines Étapes

### Pour Améliorer le Projet

1. **Kafka Streaming** (optionnel)
   - Données temps réel (plutôt qu'hourly)
   - Détection d'anomalies instantanées
   - Alertes push (email, SMS)

2. **Machine Learning**
   - Prédiction des prix (LSTM, Prophet)
   - Clustering des cryptos (similaires)
   - Anomaly detection (isolation forest)

3. **Optimisations**
   - Indexation PostgreSQL (queries plus rapides)
   - Partitioning par date (gestion 1M+ lignes)
   - Spark sur cluster (AWS, GCP, Databricks)

4. **Fonctionnalités**
   - Comparaison portfolios
   - Backtesting stratégies
   - Export rapports (PDF, email)
   - Authentification utilisateur (multi-user)

---

## Conclusion

Ce projet démontre une **architecture data pipeline complète et production-ready** :

- **Ingestion** automatisée depuis une API publique
- **Stockage** persistant dans PostgreSQL Cloud
- **Transformation** intelligente (Pandas/Spark hybride)
- **Automatisation** via Prefect scheduling
- **Visualisation** professionnelle avec Streamlit + Plotly
- **Scalabilité** prête pour million+ lignes

Le projet est également **pédagogique** : il montre comment utiliser Spark efficacement même avec peu de données, grâce à une sélection automatique basée sur le volume.

---

**Pour poser des questions à ton ami(e)** :

- "Comment explique-tu le pipeline à quelqu'un qui ne connaît pas la data ?"
- "Pourquoi choisir PostgreSQL plutôt qu'une simple CSV ?"
- "Pourquoi Spark si on n'a que 5000 lignes ?"
- "Comment le dashboard se met à jour automatiquement ?"
- "Si un prix manque, qu'est-ce qui se passe ?"

Ces questions vont solidifier la compréhension ! 🚀
