# 🚀 Crypto Data Pipeline — End-to-End

Pipeline Data Engineering complet pour collecter, traiter et visualiser les prix des cryptomonnaies en temps réel.

**🌐 Dashboard en ligne** : https://crypto-data-pipeline-8mrtpexxnwydrp9dbehvt5.streamlit.app

---

## 📊 Diagramme d'architecture
```
┌─────────────────────────────────────────────────────────────────┐
│                    CRYPTO DATA PIPELINE                         │
└─────────────────────────────────────────────────────────────────┘

   SOURCES                STOCKAGE              TRAITEMENT           SORTIE
   ───────                ────────              ──────────           ──────

CoinGecko API          PostgreSQL           Transformations      Streamlit
(batch/10min)     ──►   (Neon Cloud)    ──►     Pandas        ──►  Dashboard
                           │                      │                  (URL)
Kafka Producer            │                      │
(streaming/5s)     ──►    │                  Prefect
                          │                (orchestration)
                   Docker Compose
                   (dev local)
```

---

## 🛠️ Stack technologique

| Couche | Technologie | Justification |
|--------|-------------|---------------|
| **Ingestion Batch** | Python + CoinGecko API | API publique gratuite, données structurées fiables |
| **Ingestion Streaming** | Apache Kafka | Standard industrie pour flux temps réel, scalable |
| **Stockage** | PostgreSQL (Neon) | Données structurées, requêtes analytiques SQL, cloud gratuit |
| **Transformation** | Pandas | Volume modéré (~5000 lignes), développement rapide |
| **Orchestration** | Prefect | Dashboard intégré, retry automatique, plus simple qu'Airflow |
| **Visualisation** | Streamlit + Plotly | Déploiement rapide, interface interactive, gratuit |
| **Déploiement** | Docker Compose + Cloud | Reproductibilité locale + accessibilité publique |

---

## 📂 Structure du projet
```
crypto_data_project/
├── ingestion/
│   ├── main.py                 # Ingestion batch CoinGecko (10 min)
│   ├── database.py             # Connexion PostgreSQL + création tables
│   ├── kafka_producer.py       # Producteur Kafka (simulation)
│   ├── kafka_consumer.py       # Consommateur Kafka → PostgreSQL
│   ├── fetch_history.py        # Récupération historique réel
│   └── migrate_to_neon.py      # Migration données vers cloud
├── transformations/
│   ├── cleaning.py             # Nettoyage (nulls, doublons, outliers)
│   ├── aggregations.py         # Moyenne horaire + volume journalier
│   ├── rankings.py             # Classement par performance 24h
│   ├── alerts.py               # Détection variations > ±5%
│   └── run_transforms.py       # Pipeline complet transformations
├── orchestration/
│   ├── flows.py                # Flows Prefect avec retry
│   └── scheduler.py            # Planification automatique
├── dashboard/
│   ├── app.py                  # Dashboard Streamlit principal
│   └── config.py               # Configuration multi-environnement
├── tests/
│   └── test_transformations.py # 18 tests unitaires
├── docker-compose.yml          # PostgreSQL + Kafka + Zookeeper
├── requirements.txt            # Dépendances Python
├── .env.example               # Template configuration
└── README.md                   # Documentation (ce fichier)
```

---

## 📡 Sources de données

### Source 1 — Batch (CoinGecko API)
- **Endpoint** : `https://api.coingecko.com/api/v3/coins/markets`
- **Fréquence** : Toutes les 10 minutes
- **Données** : Prix, volume, capitalisation, variation 24h
- **Cryptos** : Bitcoin, Ethereum, Solana, XRP, BNB
- **Plan** : Gratuit (avec clé API)

### Source 2 — Streaming (Kafka)
- **Topic** : `crypto_prices`
- **Producteur** : Simulation de variations de prix réalistes
- **Fréquence** : Toutes les 5 secondes
- **Données** : Prix + variation instantanée

---

## 🔄 Transformations métier

| # | Transformation | Fichier | Description |
|---|----------------|---------|-------------|
| 1 | **Nettoyage** | `cleaning.py` | Suppression nulls, doublons, prix aberrants (<0) |
| 2 | **Moyenne horaire** | `aggregations.py` | Prix moyen/min/max par crypto par heure |
| 3 | **Volume journalier** | `aggregations.py` | Volume total échangé par jour |
| 4 | **Classement** | `rankings.py` | Ranking par performance 24h (meilleur = rang 1) |
| 5 | **Alertes** | `alerts.py` | Détection variation > ±5% (hausse/baisse forte) |

**Exemple de résultat** :
```
Alerte détectée : Bitcoin +12.87% (HAUSSE FORTE) - Prix: $59,905.71
```

---

## ⚙️ Orchestration

- **Outil** : Prefect 3.x
- **Fréquence** : Pipeline complet toutes les 10 minutes
- **Retry** : 3 tentatives automatiques sur échec API
- **Dépendances** : Ingestion → Nettoyage → Transformations → Alertes
- **Logs** : Sauvegardés dans `logs/` avec horodatage

**Flow principal** :
```python
1. Créer tables si nécessaire
2. Appeler API CoinGecko (avec retry)
3. Sauvegarder données brutes
4. Nettoyer les données
5. Calculer transformations en parallèle
6. Générer rapport de synthèse
```

---

## 🖥️ Dashboard interactif

**URL publique** : https://crypto-data-pipeline-8mrtpexxnwydrp9dbehvt5.streamlit.app

**Fonctionnalités** :
- 📊 Graphique évolution des prix sur 7 jours (Plotly interactif)
- 🏆 Top 5 cryptos du jour par performance
- ⚠️ Alertes de volatilité en temps réel (seuil configurable)
- 📈 Volume journalier échangé par crypto
- 🔄 Actualisation automatique toutes les 60 secondes
- 📱 Responsive (mobile, tablette, desktop)

---

## 🚀 Installation et démarrage

### Prérequis
- **Docker Desktop** (installé et démarré)
- **Python 3.10+**
- **Git**
- **Clé API CoinGecko** (gratuite sur coingecko.com)

### Installation rapide
```bash
# 1. Cloner le dépôt
git clone https://github.com/asmasebai123/crypto-data-pipeline.git
cd crypto-data-pipeline

# 2. Créer l'environnement virtuel
python -m venv venv
venv\Scripts\activate        # Windows
source venv/bin/activate     # Linux/Mac

# 3. Installer les dépendances
pip install -r requirements.txt

# 4. Configurer les variables d'environnement
cp .env.example .env
# Éditer .env et ajouter ta clé CoinGecko

# 5. Démarrer les services Docker
docker compose up -d

# 6. Initialiser la base de données
python ingestion/database.py

# 7. Lancer le pipeline complet
python orchestration/flows.py

# 8. Lancer le dashboard (optionnel en local)
streamlit run dashboard/app.py
```

**Dashboard accessible sur** : http://localhost:8501

---

## 🧪 Tests
```bash
# Lancer tous les tests
pytest tests/ -v

# Résultat attendu
======================== 18 passed in 0.42s ========================
```

**Tests couverts** :
- Nettoyage des données (5 tests)
- Agrégations (3 tests)
- Détection d'alertes (5 tests)
- Classement (2 tests)
- Qualité des données (3 tests)

---

## 🔧 Difficultés rencontrées et solutions

| Problème | Solution |
|----------|----------|
| **SSL Certificate Error** (réseau universitaire) | `verify=False` + `urllib3.disable_warnings` |
| **CoinGecko 403 Forbidden** (nouvelle politique) | Inscription gratuite + clé API dans headers |
| **kafka-python incompatible Python 3.12** | Migration vers `kafka-python-ng` |
| **Déploiement cloud base de données** | Neon PostgreSQL gratuit + migration script |
| **requirements.txt Anaconda paths** | Nettoyage manuel + librairies essentielles uniquement |

---

## 📈 Résultats

- **Données collectées** : ~5050 lignes sur 7 jours (5 cryptos × 168 points/jour)
- **Fréquence mise à jour** : Toutes les 10 minutes (batch) + 5 secondes (streaming)
- **Uptime dashboard** : 24/7 via Streamlit Cloud
- **Temps de réponse** : <2 secondes (requêtes PostgreSQL)
- **Tests** : 100% de passage (18/18)

---

## 🎯 Limites et perspectives d'amélioration

### Limites actuelles
- Volume limité : 5 cryptos uniquement
- Granularité : données horaires (API gratuite)
- Streaming : simulation, pas de vraie connexion WebSocket
- Pandas : limite ~1M lignes (pas scalable pour big data)

### Améliorations possibles
1. **Scalabilité** : Remplacer Pandas par PySpark pour gros volumes
2. **Real-time** : Connexion WebSocket directe aux exchanges (Binance, Coinbase)
3. **ML/AI** : Modèle prédictif des prix (LSTM, Prophet)
4. **Alertes** : Notifications Slack/Email sur alertes critiques
5. **Monitoring** : Grafana + Prometheus pour métriques pipeline
6. **CI/CD** : GitHub Actions pour tests automatiques

---

## 📦 Déploiement

### Environnement de développement (local)
```bash
docker compose up -d
python orchestration/flows.py
streamlit run dashboard/app.py
```

### Environnement de production (cloud)
- **Dashboard** : Streamlit Cloud (https://...streamlit.app)
- **Base de données** : Neon PostgreSQL (eu-central-1)
- **Logs** : Dossier `logs/` avec rotation automatique

---

## 👤 Auteur

**Asma SEBAI**  
Ingénieur Data | ING2 S2  
📧 asmasebai2003@gmail.com  
🔗 [GitHub](https://github.com/asmasebai123) | [LinkedIn](https://linkedin.com/in/asmasebai)

---

## 📄 Licence

Ce projet est un projet académique réalisé dans le cadre du cours de Data Engineering.

---

## 🙏 Remerciements

- **CoinGecko** pour l'API publique gratuite
- **Neon** pour l'hébergement PostgreSQL gratuit
- **Streamlit** pour la plateforme de déploiement
- **Anthropic Claude** pour l'assistance technique

---

**⭐ N'oubliez pas de star le repo si ce projet vous a aidé !**