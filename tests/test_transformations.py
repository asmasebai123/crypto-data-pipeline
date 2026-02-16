# tests/test_transformations.py
import sys
import os
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'transformations'))

from cleaning    import clean_raw_prices
from aggregations import compute_hourly_average, compute_daily_volume
from rankings     import compute_daily_ranking
from alerts       import detect_price_alerts


# ══════════════════════════════════════════════════════════════════
#  DONNEES DE TEST (réutilisées dans tous les tests)
# ══════════════════════════════════════════════════════════════════

def make_sample_df():
    """Crée un DataFrame de test réaliste."""
    now = datetime.now()
    return pd.DataFrame([
        {
            "coin_id": "bitcoin",    "name": "Bitcoin",
            "symbol": "btc",        "current_price": 95000.0,
            "market_cap": 1800000000000.0,
            "total_volume": 28000000000.0,
            "price_change_24h": 1200.0,
            "price_change_pct_24h": 1.28,
            "high_24h": 96000.0,    "low_24h": 93000.0,
            "fetched_at": now
        },
        {
            "coin_id": "ethereum",   "name": "Ethereum",
            "symbol": "eth",        "current_price": 3200.0,
            "market_cap": 385000000000.0,
            "total_volume": 12000000000.0,
            "price_change_24h": -50.0,
            "price_change_pct_24h": -1.54,
            "high_24h": 3300.0,     "low_24h": 3100.0,
            "fetched_at": now - timedelta(minutes=10)
        },
        {
            "coin_id": "solana",     "name": "Solana",
            "symbol": "sol",        "current_price": 185.0,
            "market_cap": 85000000000.0,
            "total_volume": 4000000000.0,
            "price_change_24h": 12.0,
            "price_change_pct_24h": 6.94,   # > 5% = alerte !
            "high_24h": 190.0,      "low_24h": 172.0,
            "fetched_at": now - timedelta(minutes=20)
        },
        {
            "coin_id": "ripple",     "name": "XRP",
            "symbol": "xrp",        "current_price": 0.55,
            "market_cap": 30000000000.0,
            "total_volume": 1500000000.0,
            "price_change_24h": -0.04,
            "price_change_pct_24h": -7.27,  # < -5% = alerte !
            "high_24h": 0.60,       "low_24h": 0.50,
            "fetched_at": now - timedelta(minutes=30)
        },
    ])


# ══════════════════════════════════════════════════════════════════
#  TEST 1 — Nettoyage des données
# ══════════════════════════════════════════════════════════════════

class TestCleaning:

    def test_supprime_lignes_prix_nul(self):
        """Les lignes avec current_price=None doivent être supprimées."""
        df = make_sample_df()
        # Ajouter une ligne avec prix nul
        ligne_nulle = df.iloc[0].copy()
        ligne_nulle["current_price"] = None
        df = pd.concat([df, pd.DataFrame([ligne_nulle])], ignore_index=True)

        result = clean_raw_prices(df)
        assert result["current_price"].isna().sum() == 0, \
            "Il ne doit rester aucun prix nul apres nettoyage"

    def test_supprime_prix_negatif(self):
        """Un prix <= 0 est aberrant et doit être supprimé."""
        df = make_sample_df()
        ligne_neg = df.iloc[0].copy()
        ligne_neg["current_price"] = -500.0
        df = pd.concat([df, pd.DataFrame([ligne_neg])], ignore_index=True)

        result = clean_raw_prices(df)
        assert (result["current_price"] > 0).all(), \
            "Tous les prix doivent etre positifs apres nettoyage"

    def test_supprime_doublons(self):
        """Les doublons (même coin + même timestamp) doivent être supprimés."""
        df = make_sample_df()
        df_double = pd.concat([df, df], ignore_index=True)

        result = clean_raw_prices(df_double)
        assert len(result) == len(df), \
            "Les doublons doivent etre supprimes"

    def test_dataframe_vide(self):
        """Un DataFrame vide ne doit pas planter."""
        df_vide = pd.DataFrame()
        result  = clean_raw_prices(df_vide)
        assert result.empty, "Un DataFrame vide doit rester vide"

    def test_types_corriges(self):
        """Les colonnes numériques doivent être du bon type après nettoyage."""
        df = make_sample_df()
        # Simuler des colonnes en string (comme elles arrivent parfois de la DB)
        df["current_price"] = df["current_price"].astype(str)
        df["total_volume"]  = df["total_volume"].astype(str)

        result = clean_raw_prices(result if False else clean_raw_prices(df))
        assert pd.api.types.is_float_dtype(result["current_price"]), \
            "current_price doit etre numerique"


# ══════════════════════════════════════════════════════════════════
#  TEST 2 — Calcul de la moyenne horaire
# ══════════════════════════════════════════════════════════════════

class TestAggregations:

    def test_moyenne_horaire_correcte(self):
        """La moyenne horaire doit être correctement calculée."""
        now = datetime.now().replace(minute=0, second=0, microsecond=0)
        df  = pd.DataFrame([
            {"coin_id": "bitcoin", "name": "Bitcoin",
             "current_price": 95000.0, "total_volume": 1000.0,
             "price_change_pct_24h": 1.0, "fetched_at": now},
            {"coin_id": "bitcoin", "name": "Bitcoin",
             "current_price": 96000.0, "total_volume": 2000.0,
             "price_change_pct_24h": 1.0,
             "fetched_at": now + timedelta(minutes=30)},
        ])

        result = compute_hourly_average(df)
        avg    = result[result["coin_id"] == "bitcoin"]["avg_price"].values[0]

        assert abs(avg - 95500.0) < 0.01, \
            f"Moyenne attendue : 95500.0, obtenue : {avg}"

    def test_volume_journalier_somme(self):
        """Le volume journalier doit être la somme des volumes du jour."""
        now = datetime.now()
        df  = pd.DataFrame([
            {"coin_id": "bitcoin", "name": "Bitcoin",
             "current_price": 95000.0, "total_volume": 10000.0,
             "price_change_pct_24h": 1.0, "fetched_at": now},
            {"coin_id": "bitcoin", "name": "Bitcoin",
             "current_price": 95500.0, "total_volume": 20000.0,
             "price_change_pct_24h": 1.0,
             "fetched_at": now + timedelta(hours=1)},
        ])

        result = compute_daily_volume(df)
        total  = result[result["coin_id"] == "bitcoin"]["total_volume"].values[0]

        assert total == 30000.0, \
            f"Volume total attendu : 30000.0, obtenu : {total}"

    def test_moyenne_dataframe_vide(self):
        """compute_hourly_average ne doit pas planter sur DataFrame vide."""
        result = compute_hourly_average(pd.DataFrame())
        assert result.empty


# ══════════════════════════════════════════════════════════════════
#  TEST 3 — Détection des alertes
# ══════════════════════════════════════════════════════════════════

class TestAlerts:

    def test_detecte_hausse_forte(self):
        """Une variation > +5% doit déclencher une alerte HAUSSE FORTE."""
        df     = make_sample_df()
        alerts = detect_price_alerts(df, threshold=5.0)

        solana_alert = alerts[alerts["coin_id"] == "solana"]
        assert len(solana_alert) == 1, \
            "Solana (+6.94%) doit declencher une alerte"
        assert solana_alert["alert_level"].values[0] == "HAUSSE FORTE"

    def test_detecte_baisse_forte(self):
        """Une variation < -5% doit déclencher une alerte BAISSE FORTE."""
        df     = make_sample_df()
        alerts = detect_price_alerts(df, threshold=5.0)

        xrp_alert = alerts[alerts["coin_id"] == "ripple"]
        assert len(xrp_alert) == 1, \
            "XRP (-7.27%) doit declencher une alerte"
        assert xrp_alert["alert_level"].values[0] == "BAISSE FORTE"

    def test_pas_alerte_sous_seuil(self):
        """Bitcoin (+1.28%) ne doit PAS déclencher d'alerte."""
        df     = make_sample_df()
        alerts = detect_price_alerts(df, threshold=5.0)

        btc_alert = alerts[alerts["coin_id"] == "bitcoin"]
        assert len(btc_alert) == 0, \
            "Bitcoin (+1.28%) ne doit pas declencher d'alerte"

    def test_nombre_alertes_correct(self):
        """Exactement 2 alertes attendues (Solana + XRP)."""
        df     = make_sample_df()
        alerts = detect_price_alerts(df, threshold=5.0)
        assert len(alerts) == 2, \
            f"2 alertes attendues, {len(alerts)} obtenues"

    def test_alerte_dataframe_vide(self):
        """detect_price_alerts ne doit pas planter sur DataFrame vide."""
        result = detect_price_alerts(pd.DataFrame(), threshold=5.0)
        assert result.empty


# ══════════════════════════════════════════════════════════════════
#  TEST 4 — Classement
# ══════════════════════════════════════════════════════════════════

class TestRankings:

    def test_rang_1_meilleure_performance(self):
        """Le rang 1 doit correspondre à la crypto avec la meilleure variation."""
        df     = make_sample_df()
        result = compute_daily_ranking(df)

        rang1 = result[result["rank"] == 1]["coin_id"].values[0]
        assert rang1 == "solana", \
            f"Solana (+6.94%) doit etre rang 1, obtenu : {rang1}"

    def test_tous_cryptos_classes(self):
        """Toutes les cryptos du dataset doivent apparaître dans le classement."""
        df     = make_sample_df()
        result = compute_daily_ranking(df)

        coins_entree  = set(df["coin_id"].unique())
        coins_sortie  = set(result["coin_id"].unique())
        assert coins_entree == coins_sortie, \
            "Toutes les cryptos doivent apparaitre dans le classement"


# ══════════════════════════════════════════════════════════════════
#  TEST 5 — Qualité des données
# ══════════════════════════════════════════════════════════════════

class TestDataQuality:

    def test_pas_de_valeurs_nulles_apres_nettoyage(self):
        """Après nettoyage, les colonnes critiques ne doivent avoir aucun null."""
        df = make_sample_df()
        # Injecter des nulls dans plusieurs colonnes
        df.loc[0, "total_volume"]         = None
        df.loc[1, "price_change_pct_24h"] = None
        df.loc[2, "market_cap"]           = None

        result = clean_raw_prices(df)
        for col in ["current_price", "total_volume", "price_change_pct_24h"]:
            assert result[col].isna().sum() == 0, \
                f"La colonne {col} contient encore des nulls apres nettoyage"

    def test_prix_toujours_positifs(self):
        """Tous les prix après nettoyage doivent être strictement positifs."""
        df     = make_sample_df()
        result = clean_raw_prices(df)
        assert (result["current_price"] > 0).all(), \
            "Tous les prix doivent etre > 0"

    def test_colonnes_obligatoires_presentes(self):
        """Les colonnes critiques doivent toujours être présentes."""
        df       = make_sample_df()
        result   = clean_raw_prices(df)
        required = ["coin_id", "current_price", "total_volume",
                    "price_change_pct_24h", "fetched_at"]
        for col in required:
            assert col in result.columns, \
                f"Colonne obligatoire manquante : {col}"