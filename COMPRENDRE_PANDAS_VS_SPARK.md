# 🔄 Pandas vs Spark : Le Dual-Engine Expliqué

## 📊 Concept: L'Architecture Dual-Engine

**L'idée:** Utiliser le meilleur outil selon le VOLUME de données

```
Volume PETIT (< 50k lignes)  →  PANDAS  (rapide, simple, pas cher)
                                    ↓
                    Traitement sur UN ordinateur
                    Mémoire RAM suffisante
                    Idéal pour développement

Volume GROS (≥ 50k lignes)   →  SPARK   (distribué, scalable, puissant)
                                    ↓
                    Traitement sur PLUSIEURS ordinateurs
                    Partage des données
                    Idéal pour production massive
```

---

## 🐼 PANDAS : Le petit champion

### **Qu'est-ce que c'est?**
Une librairie Python pour traiter les données **sur UN seul ordinateur**

### **Avantages:**
✅ **Simple à apprendre** - Syntaxe Python native
✅ **Rapide pour petit volume** - Tout en mémoire RAM
✅ **Parfait pour analyse** - DataFrame interactif
✅ **Zéro config** - Installation basique

### **Inconvénients:**
❌ **Limité en mémoire** - Max ~100GB RAM
❌ **Pas distribué** - Un seul ordinateur
❌ **Lent sur gros volume** - Des millions de lignes = catastrophe

### **Exemple Pandas:**

```python
import pandas as pd

# Charger les données (petit fichier)
df = pd.read_csv("crypto_prices.csv")  # 10k lignes

# Transformer
df['prix_usd'] = df['prix'] * 1.1
df['variation_%'] = df['prix'].pct_change()

# Agréger
daily_avg = df.groupby('coin_id').agg({
    'prix': 'mean',
    'volume': 'sum'
})

# Résultat
print(daily_avg)
# Result: DataFrame avec 5 lignes (5 cryptos)
```

**Temps d'exécution:** < 1 seconde ⚡

---

## ⚡ SPARK : Le géant distribué

### **Qu'est-ce que c'est?**
Un framework pour traiter les données **sur PLUSIEURS ordinateurs en parallèle**

### **Avantages:**
✅ **Scalable infini** - Des milliards de lignes
✅ **Distribué** - Traitement parallèle sur cluster
✅ **Rapide sur gros volume** - 100x plus rapide que Pandas
✅ **Fault-tolerant** - Si un nœud crash, ça continue

### **Inconvénients:**
❌ **Complex** - Configuration, clusters, dépendances
❌ **Overhead** - Lent sur petit volume
❌ **Coûteux** - Nécessite plusieurs serveurs

### **Exemple Spark:**

```python
from pyspark.sql import SparkSession

# Créer une session Spark
spark = SparkSession.builder.appName("crypto").getOrCreate()

# Charger les données (fichier énorme)
df = spark.read.csv("crypto_prices_huge.csv", header=True)
# 10 MILLIARDS de lignes!

# Transformer (exécuté en parallèle sur cluster)
df = df.withColumn('prix_usd', df.prix * 1.1)
df = df.withColumn('variation_%',
    (df.prix - df.prix.lag(1)) / df.prix.lag(1) * 100)

# Agréger (distribué sur tous les nœuds)
daily_avg = df.groupBy('coin_id').agg({
    'prix': 'avg',
    'volume': 'sum'
})

# Résultat
daily_avg.show()
# Result: DataFrame avec 5 lignes
```

**Temps d'exécution:** ~10 secondes (mais traite 10 MILLIARDS de lignes!)

---

## 🏗️ COMPARAISON DIRECTE

| Aspect | Pandas | Spark |
|--------|--------|-------|
| **Volume max** | 100 GB | Infini |
| **Vitesse (1M lignes)** | 1 sec | 5 sec |
| **Vitesse (1B lignes)** | ❌ Crash | 100 sec |
| **Distribution** | ❌ Non | ✅ Oui |
| **Facilité** | ✅ Super facile | ❌ Complexe |
| **Mémoire** | RAM seul | Distribué |
| **Coût** | $0 | $100+/mois |
| **Idéal pour** | Dev, petit data | Production, big data |

---

## 🤖 COMMENT ÇA MARCHE DANS MON PROJET?

### **Le système choisit automatiquement:**

```python
def get_transformation_engine(nb_rows):
    """Choisit le bon moteur selon le volume"""

    if nb_rows < 50000:
        return "PANDAS"  # Petit volume
    else:
        return "SPARK"   # Gros volume

# Exemple:
df_count = get_row_count()  # Compte les lignes

if get_transformation_engine(df_count) == "PANDAS":
    # Lance les transformations Pandas
    df = pd.read_sql("SELECT * FROM raw_crypto_prices", conn)
    # Transforme avec Pandas
    result = transform_with_pandas(df)
else:
    # Lance les transformations Spark
    spark_df = spark.read.jdbc(url, table, properties)
    # Transforme avec Spark
    result = transform_with_spark(spark_df)

# Le code est le MÊME, juste l'exécution change!
```

**Le résultat est identique, juste la performance change!**

---

## 📝 EXEMPLE CONCRET: Calcul de volatilité

### **Avec Pandas (petit volume):**

```python
import pandas as pd

# 10k lignes = ok pour Pandas
df = pd.read_sql("SELECT * FROM raw_crypto_prices", conn)

# Calculer volatilité par crypto
volatility = df.groupby('coin_id')['current_price'].std()

print(volatility)
# bitcoin      2456.78
# ethereum      156.45
# ripple          0.12
# binancecoin    45.23
# solana        158.34
```

### **Avec Spark (gros volume):**

```python
from pyspark.sql.functions import stddev, col

# 10 MILLIARDS de lignes = Spark distribué sur 10 serveurs
spark_df = spark.read \
    .format("jdbc") \
    .option("url", neon_url) \
    .option("dbtable", "raw_crypto_prices") \
    .load()

# Même requête, Spark la distribue automatiquement!
volatility = spark_df.groupBy('coin_id') \
    .agg(stddev('current_price').alias('volatility'))

volatility.show()
# +-------------+------------------+
# |    coin_id   |    volatility     |
# +-------------+------------------+
# |    bitcoin   |      2456.78      |
# |   ethereum   |      156.45       |
# +-------------+------------------+
```

**Le résultat est IDENTIQUE, mais Spark l'a calculé sur 10 serveurs en parallèle!**

---

## 🔀 LE FLUX RÉEL DANS MON PROJET

```
┌──────────────────┐
│ Données brutes   │
│ 3633 lignes      │
│ (PostgreSQL)     │
└────────┬─────────┘
         │
         ▼
    COUNT(*)  ← Combien de lignes?
         │
    ┌────┴────────────────┐
    │                     │
    ▼                     ▼
 < 50k?              ≥ 50k?
    │                     │
    ▼                     ▼
┌─────────────┐      ┌──────────────┐
│   PANDAS    │      │    SPARK     │
├─────────────┤      ├──────────────┤
│ • Moyenne   │      │ • Moyenne    │
│ • Volume    │      │ • Volume     │
│ • Ranking   │      │ • Ranking    │
│ • Volatilité│      │ • Volatilité │
│ • Dominance │      │ • Dominance  │
└──────┬──────┘      └──────┬───────┘
       │                    │
       └────────┬───────────┘
                │
                ▼
    ┌─────────────────────┐
    │ Résultats identiques│
    │ (5 cryptos analyzés)│
    └─────────────────────┘
                │
                ▼
    ┌─────────────────────┐
    │  Dashboard Streamlit│
    │  (affiche les résult)
    └─────────────────────┘
```

---

## ❌ POURQUOI KAFKA NE FONCTIONNE PAS?

### **Le problème:**

Kafka est complexe à configurer en production cloud:

```
Local (développement):        Cloud (Production):
┌──────────────┐             ❌ Kafka nécessite:
│   Kafka      │             • Docker Zookeeper
│  (Docker)    │             • Docker Kafka
│              │             • Network persistant
│  Producer ──→ Consumer     • Beaucoup de ressources
│              │             • Coût élevé
└──────────────┘             • Configuration complex
```

### **Les solutions:**

**Option 1: Garder Kafka local (DEV seulement)**
```bash
# Sur votre ordinateur
docker-compose up kafka

# Puis:
python ingestion/kafka_producer.py  # Envoie des messages
python ingestion/kafka_consumer.py  # Reçoit des messages
```
✅ Marche localement
❌ Pas en production (Railway)

**Option 2: Service Kafka managé (coûteux)**
- Upstash: $29/mois
- Confluent Cloud: $99/mois
- RedPanda Cloud: $49/mois

**Option 3: Ne pas utiliser Kafka (ma recommandation)**
- ✅ Le batch (10 min) suffit amplement
- ✅ Zéro coût supplémentaire
- ✅ Plus simple et fiable

---

## 📊 MON CHOIX: Batch seulement

**Pourquoi?**

```
Batch (CoinGecko):
- Appel API simple
- Toutes les 10 minutes
- 5 cryptos par appel
- Données fiables
- Gratuit
- ~2800 lignes/jour

Streaming (Kafka):
- Complex à setup
- Nécessite cluster
- Coûteux
- Configuration difficile
- Pas d'avantage pour mon use case
```

**Pour la présentation au prof:**
> "J'ai implémenté Kafka en local pour montrer l'architecture.
> En production, le batch toutes les 10 minutes suffit.
> 2800 lignes/jour = 5000+ lignes/semaine = excellent pour l'analyse!"

---

## 🎯 POINTS CLÉS À RETENIR

### **Pandas:**
- Simple
- Parfait pour petit volume (< 50k lignes)
- Development friendly
- My default choice

### **Spark:**
- Complexe
- Nécessaire pour gros volume (> 50k lignes)
- Production ready
- Scalable infiniment

### **Mon projet:**
- ✅ Actuellement: 3633 lignes → utilise PANDAS
- ✅ Si: 1 million de lignes → basculerait à SPARK
- ✅ Le code ne change pas, juste l'exécution

### **Kafka:**
- ❌ Pas en production (trop complexe/coûteux)
- ✅ Local seulement (pour démo)
- ✅ Batch suffit amplement

---

## 💡 EXEMPLE: Quand utiliser Spark?

**Scénario réel:**

```
Jour 1: 5000 lignes      → PANDAS ✅ (1 sec)
Jour 10: 50k lignes      → PANDAS ✅ (2 sec)
Jour 100: 500k lignes    → PANDAS ❌ (30 sec, lent)
Jour 200: 1M lignes      → SPARK ✅ (5 sec, 100x plus rapide!)
```

**C'est pour ça que j'ai le dual-engine:**
- Pas besoin de Spark tant qu'on n'en a pas besoin
- Mais prêt si les données explosent

---

## 🚀 RÉSUMÉ POUR PRÉSENTER AU PROF

**Dites:**

> "J'ai implémenté une architecture **dual-engine**:
>
> **Pandas pour petit volume:**
> - Actuellement 3633 lignes
> - Traitement simple et rapide
> - Idéal pour développement
>
> **Spark pour gros volume:**
> - Implémenté mais pas utilisé actuellement
> - Si les données explosent (millions de lignes)
> - Prêt à scaler infiniment
>
> **Kafka:**
> - Implémenté localement pour la démo
> - En production, le batch suffit
> - 2800 lignes/jour = excellent pour l'analyse
>
> **Résultat:**
> - Flexible (adapte l'outil au volume)
> - Production-ready (peut scaler)
> - Cost-efficient (pas de Kafka coûteux)"

---

**Des questions sur Pandas ou Spark?** 👍
