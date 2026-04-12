# 📥 Guide Détaillé du Module Ingestion

## Table des Matières
1. [Vue d'ensemble](#vue-densemble)
2. [Structure du Dossier](#structure-du-dossier)
3. [Architecture Générale](#architecture-générale)
4. [database.py - Connexion & Stockage](#databasepy---connexion--stockage)
5. [main.py - Ingestion Batch](#mainpy---ingestion-batch)
6. [fetch_history.py - Récupération Historique](#fetch_historypy---récupération-historique)
7. [kafka_producer.py - Production Streaming](#kafka_producerpy---production-streaming)
8. [kafka_consumer.py - Consommation Streaming](#kafka_consumerpy---consommation-streaming)
9. [main_cloud.py - Ingestion Cloud](#main_cloudpy---ingestion-cloud)
10. [Flux de Données Complet](#flux-de-données-complet)
11. [Modes d'Exécution](#modes-dexécution)
12. [Gestion d'Erreurs](#gestion-derreurs)

---

## Vue d'Ensemble

### Qu'est-ce que l'Ingestion ?

L'**ingestion** est la première étape du pipeline : **collecter les données brutes** depuis des sources externes et les sauvegarder en base de données.

```
Source de Données (API CoinGecko)
           ↓
    [Ingestion]  ← C'est ici !
           ↓
    Base de Données (PostgreSQL)
           ↓
    Transformation
           ↓
    Visualisation
```

### Trois Modes d'Ingestion

| Mode | Fichier | Quand l'utiliser | Débit |
|------|---------|------------------|-------|
| **Batch** | `main.py` | Développement local, test | Périodique (chaque 10 min) |
| **Streaming** | `kafka_producer.py` + `kafka_consumer.py` | Temps réel, haute fréquence | Continu (chaque 5 sec) |
| **Cloud** | `main_cloud.py` | Production sur Streamlit Cloud | Périodique (chaque 10 min) |

---

## Structure du Dossier

```
ingestion/
├── __init__.py                    # (vide) rend le dossier importable
├── __pycache__/                   # Cache Python compilé (ignoré)
├── logs/                          # Dossier des fichiers log
│   ├── ingestion.log              # Logs de main.py
│   ├── kafka_producer.log         # Logs du producteur Kafka
│   ├── kafka_consumer.log         # Logs du consommateur Kafka
│   └── cloud_ingestion.log        # Logs de main_cloud.py
│
├── database.py                    # 🔑 Gestion connexions & tables (122 lignes)
├── main.py                        # 🔑 Ingestion batch principal (162 lignes)
├── fetch_history.py               # Récupération historique (178 lignes)
├── kafka_producer.py              # Producteur Kafka (94 lignes)
├── kafka_consumer.py              # Consommateur Kafka (60 lignes)
├── main_cloud.py                  # Ingestion Cloud Neon (262 lignes)
└── migrate_to_neon.py             # Migration locale → cloud (pour référence)
```

### Taille du Module
- **Total** : ~900 lignes de code Python
- **2 fichiers essentiels** : `database.py` + `main.py`
- **2 fichiers optionnels (streaming)** : Kafka producer/consumer
- **2 fichiers spécialisés** : Cloud + historique

---

## Architecture Générale

### Vue d'Ensemble de l'Ingestion

```
┌──────────────────────────────────────────────────────────┐
│         COUCHE INGESTION (ce dossier)                   │
└──────────────────────────────────────────────────────────┘

               INPUT (Sources externes)
                      ↓
    ┌─────────────────────────────────────┐
    │   CoinGecko API (gratuite)          │
    │   https://api.coingecko.com/api/v3/ │
    └─────────────────────────────────────┘
                      ↓
    ┌──────────────────────────────────────────────┐
    │  TROIS VOIES POSSIBLES                       │
    ├──────────────────────────────────────────────┤
    │ 1️⃣  BATCH (main.py)                         │
    │     ├─ Fetch API chaque 10 min              │
    │     ├─ Sauvegarde raw_crypto_prices         │
    │     └─ Logs fichier & console               │
    │                                               │
    │ 2️⃣  STREAMING (Kafka)                       │
    │     ├─ Producer : génère événements         │
    │     ├─ Kafka Topic : "crypto_prices"        │
    │     ├─ Consumer : écoute les événements     │
    │     └─ Sauvegarde stream_crypto_prices      │
    │                                               │
    │ 3️⃣  CLOUD (main_cloud.py)                   │
    │     ├─ Ingestion + Transformation           │
    │     ├─ Cible : Neon PostgreSQL Cloud        │
    │     └─ Exécution : toutes les 10 min        │
    └──────────────────────────────────────────────┘
                      ↓
    ┌─────────────────────────────────────┐
    │   PostgreSQL (local ou Neon Cloud)   │
    │   Tables : raw_crypto_prices,       │
    │            stream_crypto_prices     │
    └─────────────────────────────────────┘
                      ↓
               OUTPUT (Données en base)
```

### Flux de Requête HTTP Simplifié

```
Mon App (main.py)
    ↓
Crée requête HTTP vers CoinGecko
    ├─ URL: https://api.coingecko.com/api/v3/coins/markets
    ├─ Paramètres: ids=bitcoin,ethereum,... + headers
    └─ Timeout: 30 secondes
    ↓
Serveurs CoinGecko reçoivent
    ↓
Retournent JSON avec 20 cryptos
    ↓
Mon App reçoit la réponse
    ├─ Vérifie le status code (200 = OK, 429 = rate limit, 403 = erreur cle)
    ├─ Parse le JSON
    └─ Extrait les données pertinentes
    ↓
Sauvegarde en PostgreSQL
```

---

## database.py - Connexion & Stockage

### Rôle du Fichier

Ce fichier gère **tout ce qui concerne la base de données** :
- ✅ Établir les connexions PostgreSQL
- ✅ Créer les tables SQL
- ✅ Insérer les données
- ✅ Gérer les erreurs de connexion

### Contenu du Fichier (122 lignes)

#### Partie 1 : Connexion (`get_connection()`)

```python
def get_connection():
    return psycopg2.connect(
        host="localhost",          # Machine locale
        port=5432,                 # Port PostgreSQL standard
        dbname="crypto_db",        # Nom de la base
        user="admin",              # Utilisateur
        password="password123"     # Mot de passe
    )
```

**Explication** :
- `psycopg2` = driver PostgreSQL pour Python
- Paramètres = identifiants pour se connecter
- Retourne une **connexion active** qu'on peut utiliser pour des requêtes

**En production (Cloud)** :
```python
# main_cloud.py utilise :
NEON_URL = "postgresql://user:password@host:5432/db?sslmode=require"
psycopg2.connect(NEON_URL)
```

#### Partie 2 : Création des Tables (`create_tables()`)

**Table 1 : `raw_crypto_prices`**

```python
CREATE TABLE IF NOT EXISTS raw_crypto_prices (
    id              SERIAL PRIMARY KEY,           # Auto-increment
    coin_id         VARCHAR(50)     NOT NULL,    # Ex: "bitcoin"
    name            VARCHAR(100)    NOT NULL,    # Ex: "Bitcoin"
    symbol          VARCHAR(20)     NOT NULL,    # Ex: "BTC"
    current_price   NUMERIC(20, 8)  NOT NULL,   # Prix avec 8 décimales
    market_cap      NUMERIC(30, 2),             # Capitalisation boursière
    total_volume    NUMERIC(30, 2),             # Volume d'échange
    price_change_24h        NUMERIC(20, 8),    # Changement en $ sur 24h
    price_change_pct_24h    NUMERIC(10, 4),   # Changement en % sur 24h
    high_24h        NUMERIC(20, 8),            # Plus haut cours 24h
    low_24h         NUMERIC(20, 8),            # Plus bas cours 24h
    fetched_at      TIMESTAMP   DEFAULT NOW()  # Moment de la récupération
);
```

**Pourquoi ces types ?**

| Colonne | Type | Raison |
|---------|------|--------|
| `id` | SERIAL | Clé primaire auto-incrémentée |
| `coin_id` | VARCHAR(50) | Texte court, fixe (bitcoin, ethereum) |
| `current_price` | NUMERIC(20,8) | Précision financière (8 décimales) |
| `fetched_at` | TIMESTAMP | Horodatage précis |

**Table 2 : `stream_crypto_prices`**

```python
CREATE TABLE IF NOT EXISTS stream_crypto_prices (
    id              SERIAL PRIMARY KEY,
    coin_id         VARCHAR(50)     NOT NULL,
    price           NUMERIC(20, 8)  NOT NULL,
    variation_pct   NUMERIC(10, 4),           # Variation en %
    event_time      TIMESTAMP       NOT NULL,
    received_at     TIMESTAMP       DEFAULT NOW()
);
```

**Différence avec `raw_crypto_prices`** :
- Plus simple (moins de colonnes)
- Stocke les événements temps réel de Kafka
- `event_time` vs `fetched_at` (quando l'événement s'est produit vs quand reçu)

---

#### Partie 3 : Insertion des Données (`insert_raw_prices()`)

```python
def insert_raw_prices(data: list):
    """
    Paramètres :
        data : liste de dictionnaires (réponse JSON de l'API)
               Ex: [
                   {
                       "id": "bitcoin",
                       "name": "Bitcoin",
                       "current_price": 42500.00,
                       ...
                   },
                   ...
               ]

    Retour :
        int : nombre de lignes insérées
    """

    # Requête préparée (évite les injections SQL)
    sql = """
    INSERT INTO raw_crypto_prices
        (coin_id, name, symbol, current_price, market_cap,
         total_volume, price_change_24h, price_change_pct_24h,
         high_24h, low_24h, fetched_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Étape 1 : Établir la connexion
    conn = get_connection()
    cur = conn.cursor()

    # Étape 2 : Préparer les rows à insérer
    rows = []
    for coin in data:
        rows.append((
            coin.get('id'),                           # bitcoin
            coin.get('name'),                         # Bitcoin
            coin.get('symbol'),                       # BTC
            coin.get('current_price'),                # 42500.00
            coin.get('market_cap'),                   # 820000000000
            coin.get('total_volume'),                 # 25000000000
            coin.get('price_change_24h'),             # 500.00
            coin.get('price_change_percentage_24h'),  # 1.19
            coin.get('high_24h'),                     # 43200.00
            coin.get('low_24h'),                      # 42100.00
            datetime.utcnow()                         # 2026-04-06 10:15:30
        ))

    # Étape 3 : Exécuter l'insertion pour toutes les rows
    cur.executemany(sql, rows)  # executemany = plusieurs INSERT en une seule requête

    # Étape 4 : Valider la transaction
    conn.commit()

    # Étape 5 : Fermer la connexion
    cur.close()
    conn.close()

    return len(rows)
```

**Étapes Détaillées** :

1. **Préparation SQL** : `INSERT INTO ... VALUES (%s, %s, ...)`
   - Les `%s` sont des placeholders (évitent les injections SQL)
   - Exemple d'injection : si quelqu'un met "bitcoin'; DROP TABLE users; --"

2. **Création des tuples** :
   ```python
   rows = [
       ('bitcoin', 'Bitcoin', 'BTC', 42500.00, ...),
       ('ethereum', 'Ethereum', 'ETH', 2250.00, ...),
       ('ripple', 'Ripple', 'XRP', 0.55, ...),
   ]
   ```

3. **executemany()** : Exécute la même requête multiple fois
   ```python
   # Au lieu de faire :
   for coin in data:
       cur.execute(sql, (coin['id'], coin['name'], ...))  # Lent !

   # On fait :
   cur.executemany(sql, rows)  # Rapide ! Une seule requête
   ```

4. **commit()** : Valide les changements en base
   ```python
   # Sans commit(), les insertions ne sont pas sauvegardées
   conn.commit()  # Les données sont maintenant dans la base
   ```

#### Partie 4 : Insertion Stream (`insert_stream_price()`)

```python
def insert_stream_price(coin_id, price, variation_pct, event_time):
    """Insère UN prix provenant de Kafka"""
    sql = """
    INSERT INTO stream_crypto_prices
        (coin_id, price, variation_pct, event_time)
    VALUES (%s, %s, %s, %s)
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(sql, (coin_id, price, variation_pct, event_time))
    conn.commit()
    cur.close()
    conn.close()
```

**Différences avec `insert_raw_prices()** :
- UN seul prix au lieu de plusieurs
- Utilise `execute()` au lieu de `executemany()`
- Appelé continuellement par Kafka consumer

---

## main.py - Ingestion Batch

### Vue d'Ensemble

C'est le **script principal d'ingestion par batch** (récupération périodique).

```
Lancement : python ingestion/main.py
         ↓
Crée les tables (si n'existent pas)
         ↓
BOUCLE INFINIE :
  1️⃣  Appelle l'API CoinGecko
  2️⃣  Reçoit les prix
  3️⃣  Affiche à l'écran
  4️⃣  Sauvegarde en base
  5️⃣  Attends 10 minutes
  6️⃣  Recommence
```

### Étape 1 : Configuration du Logging (lignes 1-32)

```python
import sys
import os
import requests
import urllib3
import logging
import time
from datetime import datetime
from database import import_raw_prices, create_tables

# Fix encodage Windows (emojis dans logs)
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

# Dossier logs
os.makedirs("logs", exist_ok=True)

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(...),  # Affiche à l'écran
        logging.FileHandler("logs/ingestion.log")  # Écrit dans fichier
    ]
)
logger = logging.getLogger("batch_ingestion")
```

**Qu'est-ce que le Logging ?**

Au lieu d'écrire simplement :
```python
print("Erreur!")  # Simple mais pas structuré
```

On utilise le logging :
```python
logger.info("Appel API réussi")       # Info
logger.warning("Rate limit détecté")  # Avertissement
logger.error("Erreur base de données")  # Erreur
logger.critical("Système en panne")    # Critique
```

**Avantages** :
- ✅ Timestamps automatiques
- ✅ Niveaux de sévérité
- ✅ Fichier log permanent
- ✅ Console + fichier en même temps

**Exemple de log** :
```
2026-04-06 10:15:30 | INFO     | batch_ingestion | Tentative 1/3 - appel API CoinGecko
2026-04-06 10:15:31 | INFO     | batch_ingestion | OK - 20 cryptos recuperees
2026-04-06 10:15:32 | INFO     | batch_ingestion | 20 lignes sauvegardees en base
2026-04-06 10:15:32 | INFO     | batch_ingestion | Prochain cycle dans 9m 58s
```

### Étape 2 : Chargement de la Clé API (lignes 36-47)

```python
def load_api_key():
    """Charge la clé API CoinGecko depuis le fichier .env"""
    try:
        with open(".env") as f:
            for line in f:
                line = line.strip()
                if line.startswith("COINGECKO_API_KEY"):
                    return line.split("=", 1)[1].strip()
    except FileNotFoundError:
        pass
    return None
```

**Contenu du fichier `.env`** :
```
COINGECKO_API_KEY=your_actual_api_key_here_12345
```

**Pourquoi ?**
- Ne JAMAIS mettre les clés API en dur dans le code
- Si tu push sur GitHub, ta clé est compromise
- `.env` est dans `.gitignore` (pas commité)

**Chargement manuel** :
- Sans dépendance `python-dotenv` (1 seul import)
- Parcourt le fichier ligne par ligne
- Cherche `COINGECKO_API_KEY=`
- Extrait la valeur après le `=`

### Étape 3 : Appel API Avec Retry (lignes 51-110)

**Fonction Principale** :

```python
def fetch_crypto_prices(max_retries=3, wait_seconds=60):
    """
    Récupère les prix des cryptos depuis CoinGecko

    Paramètres :
        max_retries : nombre de tentatives (défaut 3)
        wait_seconds : attendre avant retry (défaut 60s)

    Retour :
        list : liste des cryptos avec leurs prix
        [] : liste vide si tout a échoué
    """

    # URL et paramètres de l'API
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",           # Devises : USD
        "ids": "bitcoin,ethereum,ripple,binancecoin,solana",  # 5 coins
        "order": "market_cap_desc",     # Trié par market cap décroissant
        "per_page": 10,                 # 10 résultats par page
        "page": 1,                      # Page 1
        "sparkline": False              # Sans sparkline (données inutiles)
    }

    # Headers HTTP
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json"
    }

    # Ajouter la clé API si disponible
    if API_KEY:
        headers["x-cg-demo-api-key"] = API_KEY
        logger.info("Cle API CoinGecko chargee")
    else:
        logger.warning("Aucune cle API - risque de blocage 403")

    # Boucle de retry
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Tentative {attempt}/{max_retries} - appel API CoinGecko")

            # Requête HTTP
            response = requests.get(
                url,
                params=params,
                headers=headers,
                timeout=30,         # Timeout 30 secondes
                verify=False        # Accepter certificats auto-signés
            )

            # Gestion des status codes
            if response.status_code == 429:
                # Rate limit — trop de requêtes
                logger.warning(f"Rate limit (429). Attente {wait_seconds}s...")
                time.sleep(wait_seconds)
                continue  # Essayer à nouveau

            if response.status_code == 403:
                # Accès refusé — clé invalide
                logger.error("Erreur 403 : acces refuse - verifie ta cle API")
                logger.error(f"Reponse : {response.text[:200]}")
                break  # Ne pas réessayer

            # Vérifier tout autre code d'erreur HTTP
            response.raise_for_status()

            # Parser le JSON
            data = response.json()
            logger.info(f"OK - {len(data)} cryptos recuperees")
            return data

        except requests.exceptions.SSLError as e:
            logger.error(f"Erreur SSL : {e}")
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Erreur connexion (tentative {attempt}) : {e}")
        except requests.exceptions.Timeout:
            logger.error(f"Timeout (tentative {attempt})")
        except requests.exceptions.HTTPError as e:
            logger.error(f"Erreur HTTP : {e}")

        # Attendre avant de réessayer
        if attempt < max_retries:
            logger.info("Retry dans 10 secondes...")
            time.sleep(10)

    # Si toutes les tentatives ont échoué
    logger.critical("ECHEC - toutes les tentatives ont echoue")
    return []
```

**Gestion des Erreurs** :

| Erreur | Cause | Action |
|--------|-------|--------|
| **429** | Rate limit | Attendre 60s et réessayer |
| **403** | Accès refusé | Clé API invalide, ne pas réessayer |
| **Timeout** | Pas de réponse | Réessayer après 10s |
| **ConnectionError** | Pas de connexion | Réessayer après 10s |
| **SSLError** | Certificat invalide | Réessayer après 10s |

**Retry Stratégie** :
```
Tentative 1 → TIMEOUT
  ↓ (attendre 10s)
Tentative 2 → TIMEOUT
  ↓ (attendre 10s)
Tentative 3 → TIMEOUT
  ↓ (attendre 60s si rate limit)
ÉCHEC final
```

### Étape 4 : Affichage à l'Écran (lignes 114-131)

```python
def display_prices(data):
    """Affiche les prix de manière lisible"""
    if not data:
        print("Aucune donnee.")
        return

    print("\n" + "=" * 65)
    print(f"  Crypto Prices - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 65)
    print(f"  {'Nom':<14} {'Prix USD':>12}  {'24h':>8}  {'Volume':>18}")
    print("-" * 65)

    for coin in data:
        name   = coin.get('name', '')
        price  = coin.get('current_price', 0) or 0
        change = coin.get('price_change_percentage_24h', 0) or 0
        volume = coin.get('total_volume', 0) or 0
        arrow  = "+" if change >= 0 else "-"
        print(f"  {name:<14} ${price:>12,.2f}  {arrow}{abs(change):>6.2f}%  ${volume:>16,.0f}")

    print("=" * 65 + "\n")
```

**Exemple de Sortie** :
```
=================================================================
  Crypto Prices - 2026-04-06 10:15:30
=================================================================
  Nom            Prix USD    24h       Volume
-----------------------------------------------------------------
  Bitcoin    $42,500.00   +2.50%  $25,000,000,000
  Ethereum    $2,250.00   -1.20%  $12,000,000,000
  Ripple          $0.55   -3.10%   $5,000,000,000
  Binance        $420.00   +0.85%   $3,000,000,000
  Solana         $185.00   +1.50%   $2,000,000,000
=================================================================
```

### Étape 5 : Boucle Principale (lignes 134-158)

```python
def run_batch_ingestion(interval_minutes=10, run_once=False):
    """
    Boucle principale du pipeline

    Paramètres :
        interval_minutes : intervalle entre cycles (défaut 10 min)
        run_once : si True, n'exécute qu'une fois (sinon infini)
    """
    logger.info("Demarrage ingestion batch")
    create_tables()  # Créer les tables si elles n'existent pas

    while True:
        start = datetime.now()  # Horodatage du cycle

        # 1️⃣  Appeler l'API
        data = fetch_crypto_prices()

        # 2️⃣  Si données reçues
        if data:
            # Afficher à l'écran
            display_prices(data)

            # Sauvegarder en base
            try:
                nb = insert_raw_prices(data)
                logger.info(f"{nb} lignes sauvegardees en base")
            except Exception as e:
                logger.error(f"Erreur sauvegarde DB : {e}")
        else:
            logger.warning("Aucune donnee recuperee ce cycle")

        # 3️⃣  Si run_once, arrêter après ce cycle
        if run_once:
            break

        # 4️⃣  Calculer le temps d'attente
        elapsed = (datetime.now() - start).seconds
        wait = max(0, interval_minutes * 60 - elapsed)

        # 5️⃣  Attendre et recommencer
        logger.info(f"Prochain cycle dans {wait // 60}m {wait % 60}s")
        time.sleep(wait)
```

**Timing Détaillé** :

```
Cycle 1 :
  10:00:00 → Appel API
  10:00:02 → Réception données
  10:00:03 → Affichage
  10:00:04 → Sauvegarde
  10:00:05 → Calcul temps attente : 10*60 - 5 = 595 secondes = 9m 55s
  10:00:05 → Début attente (sleep 595s)
  10:10:00 → Cycle 2 commence

Résultat : Un nouveau cycle toutes les 10 minutes exactement
```

### Entrée du Script (lignes 161-162)

```python
if __name__ == "__main__":
    run_batch_ingestion(interval_minutes=10, run_once=True)
```

**`run_once=True`** : Le script s'exécute UNE FOIS puis s'arrête
- Utile pour tester
- Sans cela, la boucle `while True` tourne indéfiniment

**Utilisation** :
```bash
# Exécuter une seule fois (test)
python ingestion/main.py

# Exécuter en continu (production local)
# Modifier run_once=False dans le code
```

---

## fetch_history.py - Récupération Historique

### Rôle

Récupérer l'**historique des 7 derniers jours** pour initialiser la base de données avec des données réelles.

```
Lancement : python ingestion/fetch_history.py
         ↓
Pour chaque crypto :
  ├─ Appel API : /market_chart?days=7
  ├─ Reçoit 168 points (7 jours × 24h)
  ├─ Calcule les variations
  └─ Sauvegarde en base
```

### Structure

#### Constantes (lignes 11-21)

```python
CRYPTOS = {
    "bitcoin":     "Bitcoin",
    "ethereum":    "Ethereum",
    "ripple":      "XRP",
    "binancecoin": "BNB",
    "solana":      "Solana",
}

SYMBOLS = {
    "bitcoin": "btc", "ethereum": "eth", ...
}
```

#### Fonction Principale : `fetch_market_chart()` (lignes 36-96)

```python
def fetch_market_chart(coin_id, days=7):
    """
    Récupère l'historique des prix pour une crypto

    Paramètres :
        coin_id : ex "bitcoin"
        days : nombre de jours (ex 7)

    Retour :
        list de tuples : [(timestamp, price, volume), ...]
    """

    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
    params = {
        "vs_currency": "usd",
        "days": days
        # PAS de 'interval' : plan gratuit uniquement
    }

    # Retry avec gestion erreurs (identique à main.py)
    for attempt in range(1, 4):
        try:
            r = requests.get(url, params=params, ...)

            # Gestion status codes
            if r.status_code == 429:
                print(f"  Rate limit — attente 60s...")
                time.sleep(60)
                continue

            if r.status_code == 401:
                print(f"  ERREUR 401 : cle API invalide ou expiree")
                return []

            r.raise_for_status()

            # Parser la réponse
            data = r.json()
            prices = data.get("prices", [])      # [[ts, price], [ts, price], ...]
            volumes = data.get("total_volumes", [])

            # Créer une map volume par timestamp
            vol_map = {v[0]: v[1] for v in volumes}

            # Construire la liste de résultats
            result = []
            for ts_ms, price in prices:
                # Convertir timestamp millisecondes → datetime UTC
                ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                vol = vol_map.get(ts_ms, 0)
                result.append((ts, price, vol))

            print(f"  OK — {len(result)} points pour {coin_id}")
            return result

        except Exception as e:
            print(f"  Erreur tentative {attempt} : {e}")
            if attempt < 3:
                time.sleep(10)

    return []
```

**Format de Réponse CoinGecko** :

```json
{
    "prices": [
        [1680684000000, 42500.00],    # [timestamp ms, prix]
        [1680687600000, 42510.50],
        [1680691200000, 42480.25],
        ...
    ],
    "total_volumes": [
        [1680684000000, 25000000000],  # [timestamp ms, volume]
        ...
    ]
}
```

**Conversion Timestamps** :

```python
ts_ms = 1680684000000  # Millisecondes
ts = ts_ms / 1000      # Secondes = 1680684000

datetime.fromtimestamp(1680684000, tz=timezone.utc)
# Résultat : 2023-04-05 08:00:00+00:00
```

#### Insertion Historique : `insert_historical()` (lignes 99-137)

```python
def insert_historical(coin_id, name, symbol, records):
    """
    Insère les données historiques en base

    Particularité : Calcule les variations sur 24h
    """

    # Préparation SQL
    sql = """
    INSERT INTO raw_crypto_prices
        (coin_id, name, symbol, current_price, market_cap,
         total_volume, price_change_24h, price_change_pct_24h,
         high_24h, low_24h, fetched_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING  # Si doublon, ignorer
    """

    # Extraire liste des prix pour retrouver prix 24h plus tôt
    prices_list = [r[1] for r in records]  # [42500, 42510.5, 42480.25, ...]

    # Construire les rows
    rows = []
    for i, (ts, price, volume) in enumerate(records):
        # Chercher le prix d'il y a 24h
        idx_24h = max(0, i - 24)  # -24h = -24 points (hourly)
        price_24h_ago = prices_list[idx_24h]

        # Calculer la variation
        change_24h = price - price_24h_ago
        change_pct = ((change_24h / price_24h_ago) * 100
                      if price_24h_ago > 0 else 0)

        rows.append((
            coin_id,
            name,
            symbol,
            round(price, 8),                    # Prix actuel
            0,                                  # Market cap (non dispo en historique)
            round(volume, 2),
            round(change_24h, 8),
            round(change_pct, 4),
            round(price * 1.01, 8),             # High (simule ±1%)
            round(price * 0.99, 8),             # Low (simule ±1%)
            ts.replace(tzinfo=None)             # Enlever timezone pour PostgreSQL
        ))

    # Insertion batch
    cur.executemany(sql, rows)
    conn.commit()
    return len(rows)
```

**Calcul de la Variation 24h** :

```
Données horaires (7 jours = 168 points) :
  [0]  09:00 : 42500.00  ← Point courant (index 0)
  [1]  10:00 : 42510.50
  ...
  [24] 09:00 (+1 jour) : 42600.00  ← Point 24h après

Pour calculer variation du point [24] :
  Prix actuel = 42600.00
  Prix 24h ago = prices_list[max(0, 24-24)] = prices_list[0] = 42500.00
  Change = 42600.00 - 42500.00 = 100.00 USD
  Change % = (100 / 42500) × 100 = 0.235%

Pour les premiers 24 points (< 24h) :
  On utilise max(0, i-24) → index 0
  Donc compare au tout premier prix
```

#### Script Principal (lignes 151-177)

```python
if __name__ == "__main__":
    print("=" * 55)
    print("  RECUPERATION HISTORIQUE REEL — COINGECKO")
    print("=" * 55)

    create_tables()
    clear_old_data()  # Supprimer les anciennes données

    print("\nRecuperation historique (7 jours)...")
    total = 0

    for coin_id, name in CRYPTOS.items():
        records = fetch_market_chart(coin_id, days=7)
        if records:
            nb = insert_historical(coin_id, name, SYMBOLS[coin_id], records)
            total += nb
            time.sleep(5)  # Pause obligatoire entre appels API

    print(f"  TERMINE ! {total} lignes reelles inserees")
```

**Résultat Typique** :
```
5 cryptos × 168 points = 840 lignes insérées
```

---

## kafka_producer.py - Production Streaming

### Rôle

**Générer des événements Kafka** (prix temps réel) en continu.

```
Lancement : python ingestion/kafka_producer.py
         ↓
BOUCLE INFINIE chaque 5 secondes :
  ├─ Choisir une crypto aléatoire
  ├─ Simuler une variation (±2%)
  ├─ Créer un message JSON
  └─ Envoyer vers Kafka topic "crypto_prices"
```

### Configuration Kafka

```python
TOPIC = "crypto_prices"
CRYPTOS = ["bitcoin", "ethereum", "ripple", "binancecoin", "solana"]

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],      # Kafka local
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5                                  # 5 tentatives en cas d'erreur
)
```

### Fonction Principale : `run_producer()` (lignes 54-90)

```python
def run_producer(interval_seconds=5):
    """
    Lance le producteur Kafka

    Paramètres :
        interval_seconds : intervalle entre messages (défaut 5s)
    """

    logger.info("🚀 Démarrage Kafka Producer")

    # Obtenir les prix réels actuels comme base
    base_prices = get_reference_prices()

    try:
        while True:
            # 1️⃣  Choisir une crypto aléatoire
            coin_id = random.choice(CRYPTOS)
            base_price = base_prices.get(coin_id, 100)

            # 2️⃣  Simuler une variation (±2%)
            price, variation = simulate_price(base_price)

            # 3️⃣  Créer le message
            message = {
                "coin_id": coin_id,
                "price": price,
                "variation_pct": variation,
                "event_time": datetime.utcnow().isoformat()
            }

            # 4️⃣  Envoyer au topic Kafka
            producer.send(TOPIC, value=message)
            logger.info(f"📤 Envoyé → {coin_id}: ${price:,.4f} ({variation:+.2f}%)")

            # 5️⃣  Mettre à jour le prix de base
            base_prices[coin_id] = price

            # 6️⃣  Attendre le prochain message
            time.sleep(interval_seconds)

    except KeyboardInterrupt:
        logger.info("⛔ Producteur arrêté")
    finally:
        producer.flush()   # Vider les messages en attente
        producer.close()
```

### Simulation de Prix : `simulate_price()` (lignes 47-50)

```python
def simulate_price(base_price):
    """
    Simule une variation aléatoire

    Retour :
        (new_price, variation_pct)
    """
    variation_pct = random.uniform(-2.0, 2.0)  # Entre -2% et +2%
    new_price = base_price * (1 + variation_pct / 100)
    return round(new_price, 8), round(variation_pct, 4)
```

**Exemple** :
```
Base : 42500.00
Variation : +1.3%
Nouveau : 42500 × 1.013 = 43052.50
```

### Format du Message Kafka

```json
{
    "coin_id": "bitcoin",
    "price": 42550.25,
    "variation_pct": 0.12,
    "event_time": "2026-04-06T10:15:30.123456"
}
```

---

## kafka_consumer.py - Consommation Streaming

### Rôle

**Lire les messages Kafka** et les sauvegarder en base de données.

```
Lancement : python ingestion/kafka_consumer.py
         ↓
Connecter au topic "crypto_prices"
         ↓
BOUCLE INFINIE :
  ├─ Écouter les messages Kafka
  ├─ Parser le JSON
  └─ Sauvegarder en table stream_crypto_prices
```

### Code Principal (lignes 21-56)

```python
def run_consumer():
    logger.info(" Démarrage Kafka Consumer")
    create_tables()

    # Connexion au topic Kafka
    consumer = KafkaConsumer(
        TOPIC,  # "crypto_prices"
        bootstrap_servers=["localhost:9092"],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",  # Lire depuis le début si nouveau groupe
        enable_auto_commit=True,       # Marquer messages comme traités
        group_id="crypto_consumer_group"
    )

    logger.info(f" En écoute sur le topic '{TOPIC}'...")

    try:
        # Boucle de consommation
        for message in consumer:
            # Extraire le message
            data = message.value  # Le JSON parsé

            coin_id = data.get("coin_id")
            price = data.get("price")
            variation_pct = data.get("variation_pct")
            event_time = datetime.fromisoformat(data.get("event_time"))

            # Log
            logger.info(f" Reçu : {coin_id} @ ${price:,.4f} ({variation_pct:+.2f}%)")

            # Sauvegarder en base
            try:
                insert_stream_price(coin_id, price, variation_pct, event_time)
                logger.info(f" Sauvegardé en base : {coin_id}")
            except Exception as e:
                logger.error(f"Erreur sauvegarde : {e}")

    except KeyboardInterrupt:
        logger.info(" Consommateur arrêté")
    finally:
        consumer.close()
```

### Configuration Kafka Consumer

| Paramètre | Valeur | Explication |
|-----------|--------|-------------|
| `auto_offset_reset` | "earliest" | Si premier accès, lire depuis le début |
| `enable_auto_commit` | True | Marquer les messages comme traités |
| `group_id` | "crypto_consumer_group" | Groupe de consommation (pour tracking) |

**Flux Détaillé** :

```
Kafka Topic : "crypto_prices"
  Message 1 : {"coin_id": "bitcoin", "price": 42500, ...}  ← Consumer lit
  Message 2 : {"coin_id": "ethereum", "price": 2250, ...}  ← Consumer lit
  Message 3 : {"coin_id": "ripple", "price": 0.55, ...}    ← Consumer lit
  Message 4 : (à venir)                                     ← Consumer attend

Consumer sauvegarde chaque message en table stream_crypto_prices
```

---

## main_cloud.py - Ingestion Cloud

### Rôle

**Ingestion + Transformation combinées** pour Streamlit Cloud / Neon.

```
Lancement : python ingestion/main_cloud.py
         ↓
BOUCLE INFINIE chaque 10 min :
  ├─ Appel API CoinGecko
  ├─ Insertion dans Neon PostgreSQL
  └─ Exécution transformations SQL directement en base
```

### Différence avec `main.py`

| Aspect | main.py | main_cloud.py |
|--------|---------|---------------|
| **Base de données** | Local `localhost:5432` | Cloud Neon (NEON_URL) |
| **Transformations** | Fait en Python (transformations/) | Fait en SQL direct |
| **Logs** | Console + fichier local | Console + fichier log |
| **Public** | Développeur local | Production Cloud |

### URL Neon (ligne 30)

```python
NEON_URL = "postgresql://neondb_owner:npg_bgGCEw9sofm6@ep-spring-paper-agif76oh-pooler.c-2.eu-central-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
```

**Décodage** :
```
postgresql://              # Protocole PostgreSQL
neondb_owner:npg_bgGCEw9sofm6   # user:password
@ep-spring-paper-agif76oh-pooler.c-2.eu-central-1.aws.neon.tech  # Host
/neondb                    # Nom base
?sslmode=require           # SSL obligatoire
&channel_binding=require   # Channel binding (sécurité)
```

### Ingestion (lignes 50-89)

```python
def fetch_crypto_prices():
    """Identique à main.py"""
    # Appel API + retry

def insert_to_neon(data):
    """Insère dans Neon au lieu de localhost"""
    conn = get_neon_conn()  # Connexion Neon
    # ... insertion identique ...
```

### Transformations SQL Directes (lignes 133-226)

Au lieu d'exécuter Python + Pandas, on execute du SQL directement dans PostgreSQL.

#### Transformation 1 : Moyenne Horaire

```python
cur.execute("""
    DELETE FROM transform_hourly_avg;
    INSERT INTO transform_hourly_avg
    SELECT
        coin_id,
        date_trunc('hour', fetched_at) as hour,
        ROUND(AVG(current_price)::numeric, 4) as avg_price,
        ROUND(MIN(current_price)::numeric, 4) as min_price,
        ROUND(MAX(current_price)::numeric, 4) as max_price,
        COUNT(*) as nb_records
    FROM raw_crypto_prices
    WHERE fetched_at >= NOW() - INTERVAL '7 days'
    GROUP BY coin_id, date_trunc('hour', fetched_at);
""")
```

**Avantages** :
- ✅ Pas de données transférées à Python
- ✅ Calculs en base (plus rapide)
- ✅ Pas d'allocation mémoire côté app

#### Transformation 2 : Classement

```python
cur.execute("""
    DELETE FROM transform_daily_ranking;
    INSERT INTO transform_daily_ranking
    SELECT
        day, rank, coin_id, name, current_price, price_change_pct_24h
    FROM (
        SELECT
            DATE(fetched_at) as day,
            coin_id, name, current_price, price_change_pct_24h,
            RANK() OVER (PARTITION BY DATE(fetched_at)
                         ORDER BY price_change_pct_24h DESC) as rank,
            ROW_NUMBER() OVER (PARTITION BY coin_id, DATE(fetched_at)
                               ORDER BY fetched_at DESC) as rn
        FROM raw_crypto_prices
        WHERE fetched_at >= NOW() - INTERVAL '7 days'
    ) sub
    WHERE rn = 1;  # Garder le dernier prix de chaque jour
""")
```

**Window Functions** :
- `RANK() OVER (... ORDER BY change DESC)` : Classer par variation
- `ROW_NUMBER() OVER (... ORDER BY fetched_at DESC)` : Numéroter par temps

---

## Flux de Données Complet

### Scénario 1 : Batch Local (main.py)

```
10:00:00
  ├─ Lancer main.py
  ├─ Appel API CoinGecko
  ├─ Reçoit JSON (20 cryptos)
  ├─ Affichage console
  ├─ Insertion PostgreSQL local
  ├─ Log : "20 lignes sauvegardees"
  ├─ Attendre 10 minutes
  └─ Cycle suivant

10:10:00 → Cycle 2
10:20:00 → Cycle 3
...
```

### Scénario 2 : Streaming Kafka

```
00:00:00 → Lancer kafka_producer.py
  ├─ Génère message Bitcoin (chaque 5s)
  ├─ Envoie vers topic "crypto_prices"
  └─ Continue indéfiniment

00:00:00 → Lancer kafka_consumer.py
  ├─ Écoute le topic
  ├─ Reçoit message Bitcoin
  ├─ Sauvegarde en stream_crypto_prices
  ├─ Attente prochain message
  └─ Continue indéfiniment

Résultat : Stream de prix en temps quasi-réel dans la BD
```

### Scénario 3 : Cloud Production (main_cloud.py)

```
Jour 1, 10:00:00
  ├─ Lancer main_cloud.py
  ├─ Appel API
  ├─ Insertion Neon
  ├─ Transformation SQL 1 (moyenne horaire)
  ├─ Transformation SQL 2 (classement)
  ├─ Transformation SQL 3 (alertes)
  ├─ Logs : "Transformations terminees"
  ├─ Attendre 10 minutes
  ├─ Cycle 2
  └─ Toutes les données sont dans Neon Cloud

Streamlit Cloud accède aux données :
  ├─ Requête SELECT * FROM raw_crypto_prices
  ├─ Requête SELECT * FROM transform_daily_ranking
  └─ Affiche le dashboard
```

---

## Modes d'Exécution

### Mode 1 : Développement Local (Recommandé pour Apprendre)

**Commande** :
```bash
cd /sessions/peaceful-sweet-wright/mnt/crypto_data_project
python ingestion/main.py
```

**Configuration** :
- Base locale PostgreSQL
- Exécution : Une fois (test)
- Données : Réelles (API CoinGecko)
- Logs : Console + fichier

**Sortie Attendue** :
```
2026-04-06 10:15:30 | INFO     | batch_ingestion | Demarrage ingestion batch
2026-04-06 10:15:30 | INFO     | batch_ingestion | Tentative 1/3 - appel API CoinGecko
2026-04-06 10:15:31 | INFO     | batch_ingestion | OK - 5 cryptos recuperees

=================================================================
  Crypto Prices - 2026-04-06 10:15:31
=================================================================
  Nom            Prix USD    24h       Volume
-----------------------------------------------------------------
  Bitcoin    $42,500.00   +2.50%  $25,000,000,000
  ...
```

### Mode 2 : Streaming Temps Réel (Avancé)

**Terminal 1 : Producer**
```bash
python ingestion/kafka_producer.py
```

**Terminal 2 : Consumer**
```bash
python ingestion/kafka_consumer.py
```

**Flux** :
- Producer génère 1 message/5s
- Consumer reçoit immédiatement
- Données dans `stream_crypto_prices`

### Mode 3 : Production Cloud (Streamlit Cloud)

**Où** : Sur serveur distant
**Commande** : `python ingestion/main_cloud.py`
**Fréquence** : Chaque 10 minutes
**Cible** : Neon PostgreSQL Cloud

---

## Gestion d'Erreurs

### Erreur 1 : Rate Limit (429)

**Cause** : Trop de requêtes à CoinGecko

**Gestion dans main.py** :
```python
if response.status_code == 429:
    logger.warning(f"Rate limit (429). Attente {wait_seconds}s...")
    time.sleep(wait_seconds)  # Attendre 60s
    continue                   # Réessayer
```

**Prévention** :
- ✅ Ajouter clé API (augmente limite)
- ✅ Augmenter `wait_seconds` (ex: 120)
- ✅ Espacer les appels

### Erreur 2 : Clé API Invalide (403)

**Cause** : `COINGECKO_API_KEY` dans .env est incorrect

**Logs** :
```
Erreur 403 : acces refuse - verifie ta cle API dans .env
Reponse : {"status":{"error_code":1000,"error_message":"invalid api key"}}
```

**Solution** :
```bash
# Aller sur https://www.coingecko.com/en/api
# Copier ta vraie clé
# Mettre dans .env : COINGECKO_API_KEY=xxxxx
```

### Erreur 3 : Connexion PostgreSQL Impossible

**Cause** : PostgreSQL ne tourne pas ou mauvais identifiants

**Logs** :
```
Erreur insertion raw : (psycopg2.OperationalError)
could not connect to server: No such file or directory
```

**Solution** :
```bash
# Vérifier PostgreSQL tourne
sudo systemctl status postgresql

# Ou vérifier identifiants
psql -h localhost -U admin -d crypto_db

# Vérifier dans database.py
get_connection() : vérifier host, port, user, password
```

### Erreur 4 : Timeout API

**Cause** : CoinGecko met trop de temps à répondre

**Logs** :
```
Timeout (tentative 1)
Retry dans 10 secondes...
```

**Gestion** :
- Automatique : 3 tentatives avec backoff
- Si tout échoue : cycle ignoré, prochain essai dans 10 min

### Erreur 5 : Table Déjà Existe

**Cause** : `CREATE TABLE IF NOT EXISTS` OK → code sûr

**Résultat** :
```
Tables créées : raw_crypto_prices, stream_crypto_prices
(ou déjà existantes, pas d'erreur)
```

---

## Résumé

### Fichiers Essentiels

| Fichier | Lignes | Rôle | Commande |
|---------|--------|------|----------|
| **database.py** | 122 | Connexion & tables | Importé par autres |
| **main.py** | 162 | Ingestion batch | `python ingestion/main.py` |
| **fetch_history.py** | 178 | Initialisation historique | `python ingestion/fetch_history.py` |
| **kafka_producer.py** | 94 | Génération streaming | `python ingestion/kafka_producer.py` |
| **kafka_consumer.py** | 60 | Consommation streaming | `python ingestion/kafka_consumer.py` |
| **main_cloud.py** | 262 | Production cloud | `python ingestion/main_cloud.py` |

### Flux de Données

```
CoinGecko API
    ↓
fetch_crypto_prices() [main.py]
    ↓
display_prices()        insert_raw_prices()
    ↓                       ↓
Console          PostgreSQL (raw_crypto_prices)
                 │
                 ├─ Transformation locale (transformations/)
                 └─ Ou transformation SQL (main_cloud.py)
                 │
                 └─ Dashboard Streamlit
```

### Points Clés

✅ **Ingestion = première étape** du pipeline
✅ **3 modes** : batch, streaming, cloud
✅ **Retry automatique** + gestion erreurs
✅ **Logs structurés** pour déboguer
✅ **SQL paramétrisé** pour sécurité
✅ **Timestamps** précis pour analyses temporelles
