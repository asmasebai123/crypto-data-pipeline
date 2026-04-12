#!/usr/bin/env python3
"""
Script pour générer les diagrammes du rapport
Génère les images PNG utilisées dans le rapport LaTeX
"""

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import numpy as np

# Configuration
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.size'] = 10

# Couleurs
COLOR_SOURCE = '#E8F4F8'      # Bleu clair
COLOR_INGESTION = '#B3E5FC'   # Bleu
COLOR_STORAGE = '#81D4FA'     # Bleu plus foncé
COLOR_TRANSFORM = '#4FC3F7'   # Bleu encore plus foncé
COLOR_AUTOMATION = '#29B6F6'  # Bleu foncé
COLOR_DASHBOARD = '#0288D1'   # Bleu très foncé
COLOR_KAFKA = '#FFC107'       # Ambre
COLOR_SPARK = '#FF6F00'       # Orange

# ─────────────────────────────────────────────────────────
# DIAGRAMME 1 : Architecture Générale
# ─────────────────────────────────────────────────────────

def create_architecture_diagram():
    """Crée le diagramme d'architecture générale"""

    fig, ax = plt.subplots(1, 1, figsize=(14, 10))
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 10)
    ax.axis('off')

    # Titre
    ax.text(5, 9.5, 'Architecture du Crypto Data Pipeline',
            ha='center', va='top', fontsize=16, fontweight='bold')

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # COUCHE 1 : Sources de Données
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    # CoinGecko API
    box1 = FancyBboxPatch((3, 8), 4, 0.6,
                          boxstyle="round,pad=0.1",
                          edgecolor='black', facecolor=COLOR_SOURCE, linewidth=2)
    ax.add_patch(box1)
    ax.text(5, 8.3, 'CoinGecko API\n(Source de données)',
            ha='center', va='center', fontsize=10, fontweight='bold')

    # Flèche vers ingestion
    arrow1 = FancyArrowPatch((5, 8), (5, 7.3),
                            arrowstyle='->', mutation_scale=25,
                            linewidth=2, color='black')
    ax.add_patch(arrow1)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # COUCHE 2 : Ingestion (Deux chemins)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    # main.py (Batch)
    box2a = FancyBboxPatch((0.5, 6.5), 3, 0.7,
                           boxstyle="round,pad=0.1",
                           edgecolor='black', facecolor=COLOR_INGESTION, linewidth=2)
    ax.add_patch(box2a)
    ax.text(2, 6.85, 'main.py\n(Batch - 10 min)',
            ha='center', va='center', fontsize=9, fontweight='bold')

    # kafka_producer.py
    box2b = FancyBboxPatch((6.5, 6.5), 3, 0.7,
                           boxstyle="round,pad=0.1",
                           edgecolor='black', facecolor=COLOR_KAFKA, linewidth=2)
    ax.add_patch(box2b)
    ax.text(8, 6.85, 'kafka_producer.py\n(Streaming - 5 sec)',
            ha='center', va='center', fontsize=9, fontweight='bold')

    # Flèches depuis API
    arrow2a = FancyArrowPatch((4.2, 7.3), (2.5, 7.2),
                             arrowstyle='->', mutation_scale=20,
                             linewidth=1.5, color='black')
    ax.add_patch(arrow2a)

    arrow2b = FancyArrowPatch((5.8, 7.3), (7.5, 7.2),
                             arrowstyle='->', mutation_scale=20,
                             linewidth=1.5, color='black')
    ax.add_patch(arrow2b)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # COUCHE 3 : Kafka Topic (optionnel)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    box3 = FancyBboxPatch((6.5, 5.5), 3, 0.7,
                          boxstyle="round,pad=0.1",
                          edgecolor='#FF6F00', facecolor=COLOR_KAFKA,
                          linewidth=2, linestyle='--')
    ax.add_patch(box3)
    ax.text(8, 5.85, 'Kafka Topic\n"crypto_prices"',
            ha='center', va='center', fontsize=9, fontweight='bold')

    # Flèche du producer vers Kafka
    arrow3 = FancyArrowPatch((8, 6.5), (8, 6.2),
                            arrowstyle='->', mutation_scale=20,
                            linewidth=1.5, color='#FF6F00')
    ax.add_patch(arrow3)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # COUCHE 4 : Stockage (PostgreSQL)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    box4 = FancyBboxPatch((3, 4.3), 4, 1,
                          boxstyle="round,pad=0.1",
                          edgecolor='black', facecolor=COLOR_STORAGE, linewidth=2)
    ax.add_patch(box4)
    ax.text(5, 4.95, 'PostgreSQL Database',
            ha='center', va='center', fontsize=11, fontweight='bold')
    ax.text(5, 4.55, 'raw_crypto_prices | stream_crypto_prices',
            ha='center', va='center', fontsize=8, style='italic')

    # Flèches vers base de données
    arrow4a = FancyArrowPatch((2, 6.5), (3.5, 5.3),
                             arrowstyle='->', mutation_scale=20,
                             linewidth=1.5, color='black')
    ax.add_patch(arrow4a)

    arrow4b = FancyArrowPatch((8, 5.5), (6.5, 5.3),
                             arrowstyle='->', mutation_scale=20,
                             linewidth=1.5, color='#FF6F00')
    ax.add_patch(arrow4b)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # COUCHE 5 : Transformation & Analytics
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    # Transformations Pandas
    box5a = FancyBboxPatch((0.5, 2.8), 2.5, 0.8,
                           boxstyle="round,pad=0.1",
                           edgecolor='black', facecolor=COLOR_TRANSFORM, linewidth=1.5)
    ax.add_patch(box5a)
    ax.text(1.75, 3.2, 'Transformations\n(Pandas/Spark)',
            ha='center', va='center', fontsize=8, fontweight='bold')

    # Spark SQL Analytics
    box5b = FancyBboxPatch((3.5, 2.8), 2.5, 0.8,
                           boxstyle="round,pad=0.1",
                           edgecolor='black', facecolor=COLOR_SPARK, linewidth=1.5)
    ax.add_patch(box5b)
    ax.text(4.75, 3.2, 'Spark Analytics\n(Volatilité, Dominance)',
            ha='center', va='center', fontsize=8, fontweight='bold', color='white')

    # Prefect Orchestration
    box5c = FancyBboxPatch((6.5, 2.8), 2.5, 0.8,
                           boxstyle="round,pad=0.1",
                           edgecolor='black', facecolor=COLOR_AUTOMATION, linewidth=1.5)
    ax.add_patch(box5c)
    ax.text(7.75, 3.2, 'Prefect\n(Orchestration)',
            ha='center', va='center', fontsize=8, fontweight='bold', color='white')

    # Flèches depuis la base de données
    arrow5a = FancyArrowPatch((3.8, 4.3), (1.75, 3.6),
                             arrowstyle='->', mutation_scale=18,
                             linewidth=1.5, color='black')
    ax.add_patch(arrow5a)

    arrow5b = FancyArrowPatch((5, 4.3), (4.75, 3.6),
                             arrowstyle='->', mutation_scale=18,
                             linewidth=1.5, color='black')
    ax.add_patch(arrow5b)

    arrow5c = FancyArrowPatch((6.2, 4.3), (7.75, 3.6),
                             arrowstyle='->', mutation_scale=18,
                             linewidth=1.5, color='black')
    ax.add_patch(arrow5c)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # COUCHE 6 : Visualisation
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    box6 = FancyBboxPatch((3, 1.2), 4, 0.9,
                          boxstyle="round,pad=0.1",
                          edgecolor='black', facecolor=COLOR_DASHBOARD, linewidth=2)
    ax.add_patch(box6)
    ax.text(5, 1.85, 'Dashboard Streamlit',
            ha='center', va='center', fontsize=11, fontweight='bold', color='white')
    ax.text(5, 1.35, '5 Onglets : Vue d\'ensemble • Analyse technique • Alertes • Kafka • Spark',
            ha='center', va='center', fontsize=7, color='white')

    # Flèches vers visualisation
    arrow6a = FancyArrowPatch((1.75, 2.8), (3.8, 2.1),
                             arrowstyle='->', mutation_scale=18,
                             linewidth=1.5, color='black')
    ax.add_patch(arrow6a)

    arrow6b = FancyArrowPatch((4.75, 2.8), (4.8, 2.1),
                             arrowstyle='->', mutation_scale=18,
                             linewidth=1.5, color='black')
    ax.add_patch(arrow6b)

    arrow6c = FancyArrowPatch((7.75, 2.8), (6.2, 2.1),
                             arrowstyle='->', mutation_scale=18,
                             linewidth=1.5, color='black')
    ax.add_patch(arrow6c)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Légende
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    # Légende modes
    ax.text(0.5, 0.6, 'Batch (Périodique)', fontsize=8, color='gray')
    ax.text(0.5, 0.3, 'Streaming (Continu avec Kafka)', fontsize=8, color='#FF6F00')

    # Légende couches
    ax.text(5, 0.05, '← Flux de données (toutes les 10 min pour batch, continu pour streaming)',
            ha='center', fontsize=7, style='italic', color='gray')

    plt.tight_layout()
    plt.savefig('/sessions/peaceful-sweet-wright/mnt/crypto_data_project/figures/architecture_diagram.png',
                dpi=300, bbox_inches='tight', facecolor='white')
    print("✓ Diagramme d'architecture sauvegardé")
    plt.close()

# ─────────────────────────────────────────────────────────
# DIAGRAMME 2 : Flux de Données
# ─────────────────────────────────────────────────────────

def create_data_flow_diagram():
    """Crée le diagramme de flux de données"""

    fig, ax = plt.subplots(1, 1, figsize=(14, 8))
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 10)
    ax.axis('off')

    # Titre
    ax.text(5, 9.5, 'Flux de Données Détaillé',
            ha='center', va='top', fontsize=16, fontweight='bold')

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # BATCH PIPELINE (gauche)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    ax.text(1.5, 8.8, 'BATCH (main.py)', fontsize=12, fontweight='bold', color='#0288D1')

    # Étape 1
    box_b1 = FancyBboxPatch((0.2, 7.8), 2.6, 0.6,
                            boxstyle="round,pad=0.05",
                            edgecolor='black', facecolor='#B3E5FC', linewidth=1.5)
    ax.add_patch(box_b1)
    ax.text(1.5, 8.1, '1. Appel API\nCoinGecko',
            ha='center', va='center', fontsize=8)

    # Flèche
    arrow_b1 = FancyArrowPatch((1.5, 7.8), (1.5, 7.3),
                              arrowstyle='->', mutation_scale=15, linewidth=1, color='black')
    ax.add_patch(arrow_b1)

    # Étape 2
    box_b2 = FancyBboxPatch((0.2, 6.7), 2.6, 0.6,
                            boxstyle="round,pad=0.05",
                            edgecolor='black', facecolor='#B3E5FC', linewidth=1.5)
    ax.add_patch(box_b2)
    ax.text(1.5, 7.0, '2. Parse JSON\n(20 cryptos)',
            ha='center', va='center', fontsize=8)

    # Flèche
    arrow_b2 = FancyArrowPatch((1.5, 6.7), (1.5, 6.2),
                              arrowstyle='->', mutation_scale=15, linewidth=1, color='black')
    ax.add_patch(arrow_b2)

    # Étape 3
    box_b3 = FancyBboxPatch((0.2, 5.6), 2.6, 0.6,
                            boxstyle="round,pad=0.05",
                            edgecolor='black', facecolor='#81D4FA', linewidth=1.5)
    ax.add_patch(box_b3)
    ax.text(1.5, 5.9, '3. Sauvegarde\nraw_crypto_prices',
            ha='center', va='center', fontsize=8)

    # Flèche
    arrow_b3 = FancyArrowPatch((1.5, 5.6), (1.5, 5.1),
                              arrowstyle='->', mutation_scale=15, linewidth=1, color='black')
    ax.add_patch(arrow_b3)

    # Étape 4
    box_b4 = FancyBboxPatch((0.2, 4.5), 2.6, 0.6,
                            boxstyle="round,pad=0.05",
                            edgecolor='black', facecolor='#4FC3F7', linewidth=1.5)
    ax.add_patch(box_b4)
    ax.text(1.5, 4.8, '4. Attendre 10 min\nProchain cycle',
            ha='center', va='center', fontsize=8)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # STREAMING PIPELINE (droite)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    ax.text(8.5, 8.8, 'STREAMING (Kafka)', fontsize=12, fontweight='bold', color='#FF6F00')

    # Étape 1
    box_s1 = FancyBboxPatch((7.2, 7.8), 2.6, 0.6,
                            boxstyle="round,pad=0.05",
                            edgecolor='black', facecolor='#FFC107', linewidth=1.5)
    ax.add_patch(box_s1)
    ax.text(8.5, 8.1, '1. Producer\nGénère prix aléatoire',
            ha='center', va='center', fontsize=8)

    # Flèche
    arrow_s1 = FancyArrowPatch((8.5, 7.8), (8.5, 7.3),
                              arrowstyle='->', mutation_scale=15, linewidth=1, color='#FF6F00')
    ax.add_patch(arrow_s1)

    # Étape 2
    box_s2 = FancyBboxPatch((7.2, 6.7), 2.6, 0.6,
                            boxstyle="round,pad=0.05",
                            edgecolor='#FF6F00', facecolor='#FFF3E0', linewidth=1.5)
    ax.add_patch(box_s2)
    ax.text(8.5, 7.0, '2. Kafka Topic\n"crypto_prices"',
            ha='center', va='center', fontsize=8)

    # Flèche
    arrow_s2 = FancyArrowPatch((8.5, 6.7), (8.5, 6.2),
                              arrowstyle='->', mutation_scale=15, linewidth=1, color='#FF6F00')
    ax.add_patch(arrow_s2)

    # Étape 3
    box_s3 = FancyBboxPatch((7.2, 5.6), 2.6, 0.6,
                            boxstyle="round,pad=0.05",
                            edgecolor='black', facecolor='#FFC107', linewidth=1.5)
    ax.add_patch(box_s3)
    ax.text(8.5, 5.9, '3. Consumer\nLit le topic',
            ha='center', va='center', fontsize=8)

    # Flèche
    arrow_s3 = FancyArrowPatch((8.5, 5.6), (8.5, 5.1),
                              arrowstyle='->', mutation_scale=15, linewidth=1, color='#FF6F00')
    ax.add_patch(arrow_s3)

    # Étape 4
    box_s4 = FancyBboxPatch((7.2, 4.5), 2.6, 0.6,
                            boxstyle="round,pad=0.05",
                            edgecolor='black', facecolor='#FF6F00', linewidth=1.5)
    ax.add_patch(box_s4)
    ax.text(8.5, 4.8, '4. Sauvegarde\nstream_crypto_prices',
            ha='center', va='center', fontsize=8, color='white')

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Base de données (au centre, en bas)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    box_db = FancyBboxPatch((2.5, 2.5), 5, 1.2,
                            boxstyle="round,pad=0.1",
                            edgecolor='black', facecolor='#81D4FA', linewidth=2)
    ax.add_patch(box_db)
    ax.text(5, 3.4, 'PostgreSQL Database',
            ha='center', va='center', fontsize=11, fontweight='bold')
    ax.text(5, 2.95, 'raw_crypto_prices (Batch) + stream_crypto_prices (Streaming)',
            ha='center', va='center', fontsize=8, style='italic')

    # Flèches vers DB
    arrow_db1 = FancyArrowPatch((1.5, 4.5), (3.5, 3.7),
                               arrowstyle='->', mutation_scale=18, linewidth=1.5, color='black')
    ax.add_patch(arrow_db1)

    arrow_db2 = FancyArrowPatch((8.5, 4.5), (6.5, 3.7),
                               arrowstyle='->', mutation_scale=18, linewidth=1.5, color='#FF6F00')
    ax.add_patch(arrow_db2)

    # Dashboard
    box_dash = FancyBboxPatch((2.5, 0.8), 5, 1,
                              boxstyle="round,pad=0.1",
                              edgecolor='black', facecolor='#0288D1', linewidth=2)
    ax.add_patch(box_dash)
    ax.text(5, 1.5, 'Dashboard Streamlit',
            ha='center', va='center', fontsize=11, fontweight='bold', color='white')
    ax.text(5, 1.0, 'http://localhost:8501',
            ha='center', va='center', fontsize=8, color='white', style='italic')

    # Flèche DB vers Dashboard
    arrow_final = FancyArrowPatch((5, 2.5), (5, 1.8),
                                 arrowstyle='->', mutation_scale=20, linewidth=2, color='black')
    ax.add_patch(arrow_final)

    # Légende timing
    ax.text(1.5, 4.0, 'Tous les\n10 min', fontsize=7, color='gray', ha='center', style='italic')
    ax.text(8.5, 4.0, 'Chaque\n5 sec', fontsize=7, color='#FF6F00', ha='center', style='italic')

    plt.tight_layout()
    plt.savefig('/sessions/peaceful-sweet-wright/mnt/crypto_data_project/figures/data_flow_diagram.png',
                dpi=300, bbox_inches='tight', facecolor='white')
    print("✓ Diagramme de flux sauvegardé")
    plt.close()

# ─────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    import os

    # Créer dossier figures
    os.makedirs('/sessions/peaceful-sweet-wright/mnt/crypto_data_project/figures', exist_ok=True)

    print("🎨 Génération des diagrammes...")
    create_architecture_diagram()
    create_data_flow_diagram()
    print("\n✅ Tous les diagrammes ont été générés !")
    print("   📁 Dossier : /sessions/peaceful-sweet-wright/mnt/crypto_data_project/figures/")
