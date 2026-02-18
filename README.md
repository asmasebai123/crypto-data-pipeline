# Crypto Data Pipeline — End-to-End

Pipeline Data Engineering complet pour collecter, traiter et visualiser
les prix des cryptomonnaies en temps réel.

---

## Architecture
```
CoinGecko API ──(batch/10min)──┐
                                ├──► PostgreSQL ──► Transformations ──► Streamlit
Kafka Simulator ──(streaming)──┘         │              Pandas            Dashboard
                                         │
                                      Prefect
                                   (orchestration)
```

---

## Stack technologique

| Couche | Technologie | Justification |
|---|---|---|
| Ingestion Batch | Python + CoinGecko API | API gratuite, fiable, données structurées |
| Ingestion Streaming | Apache Kafka | Standard industrie pour flux temps réel |
| Stockage | PostgreSQL | Données structurées, requêtes analytiques simples |
| Transformation | Pandas | Volume modéré, développement rapide |
| Orchestration | Prefect | Dashboard intégré, plus simple qu'Airflow |
| Visualisation | Streamlit + Plotly | Déploiement rapide, interface interactive |
| Déploiement | Docker Compose | Reproductibilité garantie |

---

## Sources de données

- **Source 1 — Batch** : CoinGecko API publique
  - Endpoint : `/coins/markets`
  - Fréquence : toutes les 10 minutes
  - Cryptos : Bitcoin, Ethereum, Solana, XRP, BNB

- **Source 2 — Streaming** : Apache Kafka
  - Topic : `crypto_prices`
  - Simulation de variations de prix en temps réel
  - Intervalle : toutes les 5 secondes

---

## Transformations métier

1. **Nettoyage** — suppression valeurs nulles, doublons, prix aberrants
2. **Moyenne horaire** — prix moyen par crypto par heure
3. **Classement journalier** — ranking par performance 24h
4. **Détection alertes** — variation > ±5% déclenche une alerte
5. **Volume journalier** — volume total échangé par jour

---

## Structure du projet
```
crypto_data_project/
├── ingestion/
│   ├── main.py              # Ingestion batch CoinGecko
│   ├── database.py          # Connexion PostgreSQL + tables
│   ├── kafka_producer.py    # Producteur Kafka
│   ├── kafka_consumer.py    # Consommateur Kafka
│   └── fetch_history.py     # Historique réel CoinGecko
├── transformations/
│   ├── cleaning.py          # Nettoyage données
│   ├── aggregations.py      # Moyenne horaire + volume
│   ├── rankings.py          # Classement performance
│   ├── alerts.py            # Détection alertes ±5%
│   └── run_transforms.py    # Pipeline transformations
├── orchestration/
│   ├── flows.py             # Flows Prefect
│   └── scheduler.py         # Planification 10 minutes
├── dashboard/
│   └── app.py               # Dashboard Streamlit
├── tests/
│   └── test_transformations.py  # 18 tests unitaires
├── docker-compose.yml
├── requirements.txt
├── .env.example
└── README.md
```

---

## Installation et démarrage

### Prérequis
- Docker Desktop installé et démarré
- Python 3.10+
- Clé API CoinGecko gratuite : https://www.coingecko.com/en/api

### Étape 1 — Cloner le projet
```bash
git clone https://github.com/TON_PSEUDO/crypto-data-pipeline.git
cd crypto-data-pipeline
```

### Étape 2 — Configurer l'environnement
```bash
# Créer l'environnement virtuel
python -m venv venv
venv\Scripts\activate        # Windows
source venv/bin/activate     # Linux/Mac

# Installer les dépendances
pip install -r requirements.txt

# Configurer les variables
cp .env.example .env
# Editer .env et ajouter ta clé CoinGecko
```

### Étape 3 — Démarrer les services Docker
```bash
docker compose up -d
```

### Étape 4 — Initialiser la base de données
```bash
python ingestion/database.py
```

### Étape 5 — Récupérer les données historiques
```bash
python ingestion/fetch_history.py
```

### Étape 6 — Lancer le pipeline complet
```bash
python orchestration/flows.py
```

### Étape 7 — Lancer le dashboard
```bash
streamlit run dashboard/app.py
```

Dashboard accessible sur : **http://localhost:8501**

---

## Lancer les tests
```bash
pytest tests/ -v
```
Résultat attendu : **18 tests passés**

---

## Démarrage rapide (tout en un)
```bash
# Windows — double-cliquer sur start.bat
docker compose up -d
python orchestration/flows.py
streamlit run dashboard/app.py
```

---

## Logs
Les logs sont sauvegardés dans le dossier `logs/` :
- `logs/ingestion.log` — ingestion batch
- `logs/kafka_producer.log` — producteur Kafka
- `logs/kafka_consumer.log` — consommateur Kafka
- `logs/transformations.log` — transformations

---

## Difficultés rencontrées
- **SSL Certificate Error** : réseau universitaire avec proxy
  → Solution : `verify=False` + `urllib3.disable_warnings`
- **CoinGecko 403 Forbidden** : clé API requise depuis 2024
  → Solution : inscription gratuite sur coingecko.com
- **kafka-python incompatible Python 3.12**
  → Solution : migration vers `kafka-python-ng`

  ---
  ## 🌐 Dashboard en ligne

**URL publique** : https://crypto-data-pipeline-8mrtpexxnwydrp9dbehvt5.streamlit.app

Le dashboard est accessible 24/7 et se met à jour automatiquement.
