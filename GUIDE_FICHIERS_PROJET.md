# 📁 Guide Complet des Fichiers du Projet

## 🎯 Vue d'ensemble du projet

**Crypto Data Pipeline** est un système **end-to-end** qui collecte, transforme et visualise les données de cryptomonnaies en temps réel.

---

## 📂 Structure du Projet

```
crypto_data_project/
├── 📁 ingestion/              → Collecte des données
├── 📁 dashboard/              → Interface Streamlit
├── 📁 logs/                   → Logs d'exécution
├── 📁 figures/                → Diagrammes (PNG)
├── 🔧 Configuration & Deployment
├── 📊 Scripts de données
└── 📚 Documentation
```

---

# 🔍 DÉTAIL DE CHAQUE FICHIER

## 1️⃣ DOSSIER: `ingestion/` (Collecte des données)

### **`database.py`** ⚙️
**Rôle:** Gère la connexion à la base de données Neon PostgreSQL
- ✅ Crée les tables si elles n'existent pas
- ✅ Gère l'URL de connexion (local vs cloud)
- ✅ Fournit les fonctions de connexion

**Quand on l'utilise:** À chaque démarrage du pipeline

---

### **`main_cloud.py`** 🌐
**Rôle:** LE CŒUR du pipeline - collecte continu 24/7
**C'est celui qui tourne sur Railway!**

**Ce qu'il fait:**
1. ✅ Appelle l'API CoinGecko toutes les 10 minutes
2. ✅ Récupère les prix de 5 cryptos (Bitcoin, Ethereum, XRP, BNB, Solana)
3. ✅ Insère les données dans Neon PostgreSQL
4. ✅ Lance les transformations SQL
5. ✅ Boucle infinie = tourne 24/7

**Données collectées:**
- Prix actuel, capitalisation, volume, variation 24h

**Transformations lancées:**
- Moyenne horaire
- Volume journalier
- Classement journalier
- Détection d'alertes (variations > 5%)

---

### **`fetch_history.py`** 📚
**Rôle:** Charge l'historique des 7 derniers jours
- Utilisé pour charger des données de test
- Pas exécuté automatiquement

---

### **`kafka_producer.py`** 📤
**Rôle:** Envoie des messages de prix via Kafka (local seulement)
- Simule un flux de données temps réel
- Envoie toutes les 5 secondes
- **N'est PAS utilisé en production (Railway)**

---

### **`kafka_consumer.py`** 📥
**Rôle:** Reçoit les messages Kafka et les stocke
- Consomme les messages du producer
- Stocke dans la table `stream_crypto_prices`
- **N'est PAS utilisé en production (Railway)**

---

### **`populate_data.py`** 🌱
**Rôle:** Génère des données de test synthétiques
- Crée des données fictives pour tester
- Pas utilisé en production

---

### **`migrate_to_neon.py`** 🔄
**Rôle:** Migre les données du PostgreSQL local vers Neon Cloud
- Utilisé une fois pour transférer les données
- Plus utile maintenant (données en production)

---

## 2️⃣ DOSSIER: `dashboard/` (Interface utilisateur)

### **`app.py`** 🎨
**Rôle:** LE DASHBOARD STREAMLIT - Interface visuelle
**C'est ce que le prof verra!**

**Structure:**
- 🎨 **CSS personnalisé** = Dark theme professionnel
- 📊 **5 onglets principaux:**

#### **Onglet 1: Vue d'ensemble**
- Cartes KPI (prix actuels, variations)
- Graphique évolution des prix
- Classement du jour

#### **Onglet 2: Analyse technique**
- Graphique Candlestick OHLC
- Heatmap de corrélation
- Pour les traders

#### **Onglet 3: Alertes & Volume**
- Détection des variations > 5%
- Graphique des volumes
- Anomalies détectées

#### **Onglet 4: Streaming Kafka**
- Flux temps réel (données anciennes pour démo)
- Distribution des variations
- Tableau des derniers événements

#### **Onglet 5: Spark Analytics**
- Volatilité de chaque crypto
- Part de marché (dominance)
- Analyses avancées via SQL

**Données affichées:**
- Querues SQL depuis Neon
- Cache 60 secondes (performance)
- Graphiques via Plotly

---

### **`config.py`** ⚙️
**Rôle:** Configuration de la base de données pour Streamlit Cloud
- ✅ Lit DATABASE_URL depuis `st.secrets`
- ✅ Crée la connexion SQLAlchemy
- ✅ Permet au dashboard de lire Neon

---

## 3️⃣ FICHIERS DE CONFIGURATION

### **`Procfile`** 🚀
**Rôle:** Dit à Railway comment démarrer l'application
```
worker: python start_railway.py
```
- Lance le pipeline sur Railway
- Exécuté automatiquement

---

### **`runtime.txt`** 📌
**Rôle:** Spécifie la version Python pour Railway
```
python-3.12.0
```
- Assure la compatibilité

---

### **`.env`** 🔐
**Rôle:** Variables d'environnement (secrets)
```
DATABASE_URL=postgresql://...  (Neon Cloud)
COINGECKO_API_KEY=...           (API key)
```
- ❌ **Ne jamais committer ce fichier!**
- ✅ Utilisé seulement localement

---

### **`.env.example`** 📖
**Rôle:** Template montrant la structure de `.env`
- Guide pour configurer les variables
- Montre les formats attendus

---

### **`docker-compose.yml`** 🐳
**Rôle:** Lance les services Docker localement
- Kafka + Zookeeper
- PostgreSQL (optionnel)
- ❌ **N'est PAS utilisé en production (Railway)**

---

### **`requirements.txt`** 📦
**Rôle:** Liste toutes les dépendances Python
```
psycopg2-binary          # PostgreSQL
streamlit                # Dashboard
pandas                   # Transformations
pyspark                  # Big data
requests                 # API calls
plotly                   # Graphiques
prefect                  # Orchestration
```

---

## 4️⃣ SCRIPTS DE DÉMARRAGE & DONNÉES

### **`start_railway.py`** 🚀 ⭐ IMPORTANT!
**Rôle:** Point d'entrée pour Railway - lance le pipeline continu
**C'est ce qui tourne 24/7 en production!**

```python
from ingestion.main_cloud import run_pipeline_loop
run_pipeline_loop(interval_minutes=10)  # Toutes les 10 minutes
```

**Fonctionnement:**
- Appelle `main_cloud.py`
- Tourne en boucle infinie
- Collecte les données continuellement

---

### **`run_pipeline.py`** 🎯
**Rôle:** Lance le pipeline complet localement
**Options:**
- `--mode batch` → Batch seulement
- `--mode stream` → Kafka seulement
- `--mode full` → Batch + Kafka + Dashboard

**Pour développement local seulement**

---

### **`load_massive_data.py`** 📥 ⭐ IMPORTANT!
**Rôle:** Charge 30 jours d'historique pour le dashboard

**Ce qu'il fait:**
1. ✅ Télécharge 30 jours de chaque crypto (CoinGecko)
2. ✅ Supprime les anciennes données
3. ✅ Insère 3600+ lignes dans Neon
4. ✅ Lance les transformations SQL

**À utiliser UNE SEULE FOIS pour remplir le dashboard**

---

### **`debug_env.py`** 🔍
**Rôle:** Teste la connexion à la base de données
- Vérifie que DATABASE_URL est correct
- Teste la connexion Neon
- Utile pour déboguer

---

### **`diagnostic_db_simple.py`** 📊
**Rôle:** Affiche des stats sur la base de données
- Compte les lignes par table
- Montre la dernière mise à jour
- Vérifie l'intégrité des données

---

## 5️⃣ DOCUMENTATION & PRÉSENTATION

### **`presentation.pptx`** 🎤
**Rôle:** Présentation visuelle du projet (14 slides)
**Contient:**
- Problématique
- Architecture complète (6 couches)
- Pipeline détaillé
- Dashboard
- Résultats & métriques
- Conclusion

---

### **`discours_presentation.docx`** 📝
**Rôle:** Script complet pour la présentation
**Inclut:**
- Discours pour chaque slide
- Timing des présentations
- Explications techniques
- Transitions naturelles

---

### **`FIX_RAILWAY_PIPELINE.md`** 🔧
**Rôle:** Documentation du fix appliqué
- Explique le problème (pipeline arrêté)
- Solution implémentée
- Comment ça marche maintenant

---

### **`GUIDE_COMPLET_PROJET.md`** 📚
**Rôle:** Documentation complète du projet
- Architecture
- Installation
- Utilisation
- Troubleshooting

---

### **`LANCER_PIPELINE.md`** 🚀
**Rôle:** Guide pour lancer le pipeline localement
- Étapes d'installation
- Configuration
- Commandes à exécuter

---

## 6️⃣ FICHIERS DE DONNÉES & LOGS

### **`logs/`** 📋
**Contient:**
- `pipeline.log` → Logs du pipeline
- `cloud_ingestion.log` → Logs de l'ingestion
- `ingestion.log` → Logs détaillés
- Utiles pour déboguer

---

### **`figures/`** 🖼️
**Contient:**
- Diagrammes PNG de l'architecture
- Graphiques de démonstration
- Images pour la documentation

---

### **`crypto_prices.json`** 📄
**Rôle:** Fichier de test avec données JSON
- Données de test locales
- Exemple de structure

---

## 🏗️ ARCHITECTURE COMPLÈTE

```
┌─────────────────────────────────────────────────────────────┐
│                   SOURCES DE DONNÉES                         │
├─────────────────────────────────────────────────────────────┤
│  CoinGecko API (Batch)    │    Kafka (Streaming - Local)    │
└──────────────┬──────────────────────────┬────────────────────┘
               │                          │
               ▼                          ▼
┌──────────────────────┐   ┌──────────────────────┐
│   main_cloud.py      │   │  kafka_producer.py   │
│   (Batch Ingestion)  │   │  kafka_consumer.py   │
└──────────┬───────────┘   └──────────┬───────────┘
           │                          │
           └──────────────┬───────────┘
                          │
                          ▼
            ┌─────────────────────────┐
            │  Neon PostgreSQL Cloud  │
            │  (5 tables principales) │
            └────────────┬────────────┘
                         │
        ┌────────────────┼────────────────┐
        │                │                │
        ▼                ▼                ▼
  Transform SQL    Transform SQL    Transform SQL
  (Hourly Avg)     (Daily Volume)   (Daily Ranking)
        │                │                │
        └────────────────┼────────────────┘
                         │
                         ▼
            ┌─────────────────────────┐
            │  Streamlit Cloud        │
            │  Dashboard (5 onglets)  │
            └─────────────────────────┘
```

---

## 📊 FLUX DE DONNÉES

### **LOCAL (pour développement):**
1. CoinGecko API → `main_cloud.py` → PostgreSQL local → Streamlit local
2. Kafka Producer → Kafka Consumer → PostgreSQL local → Streamlit local

### **PRODUCTION (Railway):**
1. CoinGecko API → `main_cloud.py` (Railway) → Neon Cloud → Streamlit Cloud
2. Kafka ❌ (Non utilisé en production)

---

## 🎯 QUI FAIT QUOI

| Composant | Rôle | Exécution |
|-----------|------|-----------|
| **main_cloud.py** | Collecte batch | Railway 24/7 |
| **start_railway.py** | Orchestre main_cloud | Railway 24/7 |
| **app.py** | Dashboard visuel | Streamlit Cloud 24/7 |
| **database.py** | Gère la DB | Partout (local + cloud) |
| **load_massive_data.py** | Charge historique | Manual une fois |
| **run_pipeline.py** | Lance local | Local seulement |
| **kafka_*.py** | Streaming | Local seulement |

---

## 💡 POINTS CLÉS À RETENIR

✅ **Production stable:**
- Railway lance `start_railway.py` automatiquement
- `main_cloud.py` tourne 24/7
- Collecte 5 cryptos toutes les 10 minutes
- ~720 lignes/jour = 5000+ lignes/semaine

✅ **Dashboard toujours à jour:**
- Streamlit Cloud rafraîchit chaque 60 secondes
- Affiche les 5 onglets d'analyse
- 3600+ lignes d'historique

✅ **Données sécurisées:**
- Neon PostgreSQL en production
- Variables d'environnement chiffrées
- Zéro données sensibles en clair

---

## 🚀 POUR PRÉSENTER AU PROF

**Structure recommandée:**

1. **`presentation.pptx`** → Montrez les slides
2. **VS Code → Explorer les fichiers** dans cet ordre:
   - `ingestion/main_cloud.py` (cœur du projet)
   - `start_railway.py` (orchestration)
   - `dashboard/app.py` (interface)
   - `dashboard/config.py` (configuration)
   - `Procfile` & `runtime.txt` (déploiement)
   - `requirements.txt` (dépendances)

3. **Dashboard Streamlit** → Montrez les données en direct

4. **Logs Railway** → Montrez que ça tourne 24/7

5. **`discours_presentation.docx`** → Lisez pour les explications

---

**Vous êtes prêt ! Bonne présentation! 🎉**
