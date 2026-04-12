# 🚀 Guide : Lancer le Pipeline Complet

## Une Seule Commande pour Tout !

```bash
python run_pipeline.py
```

**Voilà !** Cette commande lance automatiquement :
- ✅ Kafka (avec Docker)
- ✅ Ingestion Batch (main.py)
- ✅ Kafka Producer (streaming)
- ✅ Kafka Consumer (streaming)
- ✅ Dashboard Streamlit

---

## Installation Rapide

### Prérequis

```bash
# Installer Docker Desktop
# https://www.docker.com/products/docker-desktop

# Vérifier Docker est installé
docker --version
```

### Installation des Dépendances

```bash
# Dans le dossier du projet
pip install -r requirements.txt

# Dépendances supplémentaires pour Kafka
pip install kafka-python
```

---

## Options de Lancement

### 1️⃣ Mode Complet (Recommandé)

```bash
python run_pipeline.py
```

Lance tout :
- Kafka + Zookeeper
- Batch Ingestion (chaque 10 min)
- Kafka Producer (chaque 5 sec)
- Kafka Consumer (continu)
- Dashboard Streamlit

**Parfait pour :** Apprendre et tester toutes les fonctionnalités

---

### 2️⃣ Mode Batch Seulement

```bash
python run_pipeline.py --mode batch
```

Lance seulement :
- Batch Ingestion (chaque 10 min)
- Dashboard Streamlit

**Parfait pour :** Développement rapide, sans Kafka

---

### 3️⃣ Mode Streaming Seulement

```bash
python run_pipeline.py --mode stream
```

Lance seulement :
- Kafka + Zookeeper
- Kafka Producer (chaque 5 sec)
- Kafka Consumer (continu)

**Parfait pour :** Tester le streaming sans ingestion API

---

### 4️⃣ Sans Dashboard

```bash
python run_pipeline.py --no-dashboard
```

Lance tout SAUF le dashboard Streamlit.

**Parfait pour :** Backend seulement (ingestion + Kafka)

---

### 5️⃣ Kafka Local (Sans Docker)

Si tu as Kafka installé localement :

```bash
python run_pipeline.py --local-kafka
```

**Prérequis :**
```bash
# Lancer Kafka localement AVANT

# Sur Linux/Mac :
bin/kafka-server-start.sh config/server.properties

# Sur Windows :
bin\windows\kafka-server-start.bat config\server.properties
```

---

## Exemples Pratiques

### Cas 1 : Apprendre Toutes les Fonctionnalités

```bash
# Terminal unique : Lance tout
python run_pipeline.py

# Résultat :
# - Données en temps réel (Kafka)
# - Données batch (API)
# - Dashboard avec les 2 sources
```

### Cas 2 : Développement Rapide

```bash
# Juste le batch, pas Kafka
python run_pipeline.py --mode batch

# Plus léger, plus rapide
```

### Cas 3 : Tester Kafka Séparément

```bash
# Terminal 1 : Backend (Kafka + Ingestion)
python run_pipeline.py --mode full --no-dashboard

# Terminal 2 : Dashboard
python -m streamlit run dashboard/app.py
```

### Cas 4 : Production (Neon Cloud)

```bash
# Tu as déjà main_cloud.py pour ça
python ingestion/main_cloud.py

# Ou configure main.py pour utiliser Neon
```

---

## Qu'est-ce qui se Lance ?

### 🐳 Docker Startup (30 sec)

```
Zookeeper :    Container zookeeper    Port 2181  ← Coordination Kafka
Kafka :        Container kafka        Port 9092  ← Messages
PostgreSQL :   Container postgres     Port 5432  ← Base de données
```

**Attendre que tout soit "healthy"** avant de continuer.

### 📥 Ingestion Batch (main.py)

```
Cycle 1 (00:00) :
  ├─ Appel API CoinGecko
  ├─ Récupère 5 cryptos
  ├─ Sauvegarde en raw_crypto_prices
  └─ Affiche les prix

Cycle 2 (00:10) :
  [Même chose]

Répété indéfiniment tous les 10 minutes
```

### 📤 Kafka Producer

```
Chaque 5 secondes :
  ├─ Choisir crypto aléatoire
  ├─ Simuler variation ±2%
  ├─ Envoyer vers topic "crypto_prices"
  └─ Log : "Envoyé → bitcoin: $42,500 (+0.12%)"

Continu indéfiniment
```

### 📥 Kafka Consumer

```
Continu, écoute le topic :
  ├─ Reçoit message Kafka
  ├─ Parse le JSON
  ├─ Sauvegarde en stream_crypto_prices
  └─ Log : "Sauvegardé : bitcoin"

Tous les messages sont traités instantanément
```

### 📊 Dashboard Streamlit

```
Auto-refresh toutes les 60 sec :
  ├─ Charge raw_crypto_prices (batch)
  ├─ Charge stream_crypto_prices (Kafka)
  ├─ Affiche graphiques Plotly
  └─ Affiche 5 onglets

Accessible sur http://localhost:8501
```

---

## Logs en Temps Réel

### Fichier Principal

```bash
# Tous les logs du pipeline
tail -f logs/pipeline.log

# Exemple de contenu :
2026-04-06 10:00:00 | INFO     | pipeline_master  | 🚀 Démarrage : Ingestion Batch
2026-04-06 10:00:05 | INFO     | kafka_producer   | 📤 Envoyé → bitcoin: $42,500.00
2026-04-06 10:00:05 | INFO     | kafka_consumer   | Reçu : bitcoin @ $42,500.00
2026-04-06 10:00:10 | INFO     | batch_ingestion  | 5 lignes sauvegardées en base
```

### Logs Spécifiques

```bash
# Batch uniquement
tail -f logs/ingestion.log

# Kafka Producer
tail -f logs/kafka_producer.log

# Kafka Consumer
tail -f logs/kafka_consumer.log

# Streamlit Cloud (si utilisé)
tail -f logs/cloud_ingestion.log
```

---

## Arrêter le Pipeline

### Méthode Simple

```bash
# Appuie sur CTRL + C dans le terminal
# Le script arrête proprement :
# - Tous les processus
# - Kafka et Docker
```

### Arrêt Manuel

Si quelque chose reste en arrière-plan :

```bash
# Arrêter Kafka
docker-compose down

# Vérifier les processus Python
ps aux | grep python

# Tuer un processus si nécessaire
kill -9 <PID>
```

---

## Dépannage

### ❌ Docker n'est pas installé

```
Erreur : Docker n'est pas installé

Solution :
  1. Installe Docker Desktop : https://www.docker.com/products/docker-desktop
  2. Redémarre ton ordinateur
  3. Relance le script
```

### ❌ Port 9092 déjà utilisé

```
Erreur : Address already in use

Solution :
  # Voir quel process utilise le port
  lsof -i :9092  (Linux/Mac)
  netstat -ano | findstr :9092  (Windows)

  # Tuer le process
  kill -9 <PID>

  # Ou arrêter Kafka
  docker-compose down
```

### ❌ Kafka ne démarre pas

```
Erreur : Kafka unhealthy

Solution :
  # Attendre plus longtemps (30 sec minimum)
  # Ou vérifier les logs Docker
  docker logs crypto_kafka

  # Redémarrer
  docker-compose restart kafka
```

### ❌ Dashboard Streamlit timeout

```
Erreur : Connection refused localhost:8501

Solution :
  # Le dashboard met ~20 sec à démarrer
  # Attendre et recharger la page

  # Ou vérifier les logs
  tail -f logs/pipeline.log | grep "Dashboard"
```

### ❌ Base de données erreur

```
Erreur : psycopg2.OperationalError: could not connect

Solution :
  # PostgreSQL n'est pas en cours d'exécution
  # Lancer avec Docker :
  docker-compose up -d postgres

  # Ou lancer localement :
  psql -U admin -d crypto_db
```

---

## Architecture Visuelle

### Flux Complet

```
                          ┌─────────────────┐
                          │  CoinGecko API  │
                          └────────┬────────┘
                                   │
                    ┌──────────────┼──────────────┐
                    │              │              │
                    ▼              ▼              ▼
            ┌───────────────┐ ┌──────────────┐  (optionnel)
            │  main.py      │ │kafka_producer│
            │  (batch)      │ │  (streaming) │
            │  10 min       │ │   5 sec      │
            └───────┬───────┘ └──────┬───────┘
                    │                │
                    │                ▼
                    │         ┌──────────────────┐
                    │         │ Kafka Topic      │
                    │         │ "crypto_prices"  │
                    │         └────────┬─────────┘
                    │                  │
                    │                  ▼
                    │         ┌──────────────────┐
                    │         │kafka_consumer    │
                    │         │  (consommer)     │
                    │         └────────┬─────────┘
                    │                  │
                    ▼                  ▼
            ┌─────────────────────────────────┐
            │    PostgreSQL Database          │
            │  ┌─────────────────────────┐   │
            │  │raw_crypto_prices (batch)│   │
            │  │stream_crypto_prices     │   │
            │  │ (streaming)             │   │
            │  └─────────────────────────┘   │
            └────────────────┬────────────────┘
                             │
                             ▼
            ┌─────────────────────────────────┐
            │   Dashboard Streamlit           │
            │   http://localhost:8501         │
            │  ┌─────────────────────────┐   │
            │  │ 5 Onglets               │   │
            │  │ - Vue d'ensemble        │   │
            │  │ - Analyse technique     │   │
            │  │ - Alertes & Volume      │   │
            │  │ - Streaming Kafka       │   │
            │  │ - Spark Analytics       │   │
            │  └─────────────────────────┘   │
            └─────────────────────────────────┘
```

---

## Performance & Ressources

### Consommation Ressources

```
Mode Batch :
  - RAM : ~200 MB
  - CPU : <5%
  - Bande passante : ~1 KB/min (API calls)

Mode Complet (Batch + Kafka) :
  - RAM : ~2 GB (Kafka + Postgres + Python)
  - CPU : ~10-15%
  - Bande passante : ~10 KB/min

Docker :
  - Disque : ~2 GB (images + volumes)
  - Réseau : localhost uniquement
```

### Optimisations

```bash
# Mode léger (développement)
python run_pipeline.py --mode batch --no-dashboard

# Mode full (test complet)
python run_pipeline.py

# Mode production (Neon Cloud)
python ingestion/main_cloud.py
```

---

## Intégration Continue (Futur)

Tu peux configurer le pipeline pour s'exécuter automatiquement :

### Avec Cron (Linux/Mac)

```bash
# Édite crontab
crontab -e

# Ajoute :
*/10 * * * * cd /chemin/projet && python run_pipeline.py --mode batch
```

### Avec Systemd (Linux)

```bash
# Crée un service
sudo nano /etc/systemd/system/crypto-pipeline.service

[Unit]
Description=Crypto Data Pipeline
After=network.target

[Service]
ExecStart=/usr/bin/python3 /chemin/projet/run_pipeline.py
Restart=always

[Install]
WantedBy=multi-user.target

# Activer et démarrer
sudo systemctl enable crypto-pipeline
sudo systemctl start crypto-pipeline
```

### Avec GitHub Actions (Cloud)

```yaml
# .github/workflows/pipeline.yml
name: Crypto Pipeline

on:
  schedule:
    - cron: '*/10 * * * *'  # Toutes les 10 min

jobs:
  pipeline:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Run Pipeline
        run: python run_pipeline.py --mode batch
```

---

## Résumé

| Commande | Mode | Utilisation |
|----------|------|------------|
| `python run_pipeline.py` | Complet | Apprendre tout |
| `python run_pipeline.py --mode batch` | Batch | Développement rapide |
| `python run_pipeline.py --mode stream` | Streaming | Tester Kafka |
| `python run_pipeline.py --no-dashboard` | Sans UI | Backend seulement |
| `python ingestion/main_cloud.py` | Cloud | Production Neon |

---

## Prochaines Étapes

### Si ça fonctionne ✅

1. Explore le dashboard sur http://localhost:8501
2. Vérifieles données en PostgreSQL :
   ```bash
   psql -U admin -d crypto_db -c "SELECT * FROM raw_crypto_prices LIMIT 5;"
   ```
3. Modifie les paramètres (fréquence, cryptos, etc.)

### Si ça ne fonctionne pas ❌

1. Vérifie les logs : `tail -f logs/pipeline.log`
2. Consulte la section "Dépannage" ci-dessus
3. Ouvre une issue sur GitHub

---

**Bon launch ! 🚀**
