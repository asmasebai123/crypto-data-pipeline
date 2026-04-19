const { Document, Packer, Paragraph, TextRun, HeadingLevel, AlignmentType, PageBreak } = require('docx');
const fs = require('fs');

const doc = new Document({
  styles: {
    default: {
      document: {
        run: { font: "Calibri", size: 24 }
      }
    },
    paragraphStyles: [
      {
        id: "Heading1",
        name: "Heading 1",
        basedOn: "Normal",
        next: "Normal",
        run: { size: 32, bold: true, font: "Calibri", color: "065A82" },
        paragraph: { spacing: { before: 240, after: 120 }, outlineLevel: 0 }
      },
      {
        id: "Heading2",
        name: "Heading 2",
        basedOn: "Normal",
        next: "Normal",
        run: { size: 28, bold: true, font: "Calibri", color: "1C7293" },
        paragraph: { spacing: { before: 180, after: 100 }, outlineLevel: 1 }
      }
    ]
  },
  sections: [{
    properties: {
      page: {
        margin: { top: 1440, right: 1440, bottom: 1440, left: 1440 }
      }
    },
    children: [
      // ===== INTRODUCTION =====
      new Paragraph({
        heading: HeadingLevel.HEADING_1,
        children: [new TextRun("DISCOURS - CRYPTO DATA PIPELINE")]
      }),

      new Paragraph({
        spacing: { after: 240 },
        children: [new TextRun("Présentation du Projet Data Engineering End-to-End")]
      }),

      new Paragraph({
        heading: HeadingLevel.HEADING_2,
        children: [new TextRun("INTRODUCTION (30 secondes)")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("Madame, Monsieur le Professeur, chers camarades,")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("Je vous présente aujourd'hui un projet ambitieux : un pipeline Data Engineering complet pour l'analyse des données de cryptomonnaies. C'est un projet qui démontre la maîtrise end-to-end de la chaîne de valeur des données, de l'ingestion jusqu'à la visualisation en production.")]
      }),

      new Paragraph({
        spacing: { after: 240 },
        children: [new TextRun("Tout le pipeline fonctionne 24/7 en cloud et est accessible en ligne. Nous allons voir ensemble comment cela a été réalisé.")]
      }),

      // ===== SLIDE 2: PROBLÉMATIQUE =====
      new Paragraph({
        heading: HeadingLevel.HEADING_2,
        children: [new TextRun("SLIDE 2: CONTEXTE ET PROBLÉMATIQUE (1 minute)")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("Commençons par la problématique qui a motivé ce projet.")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("Le marché des cryptomonnaies est un marché fascinant mais extrêmement volatil. Des milliards de dollars s'échangent chaque jour, 24 heures sur 24. Les prix fluctuent constamment, et les investisseurs ont besoin d'outils d'analyse en temps réel pour prendre des décisions éclairées.")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("Mais le défi ne s'arrête pas là. Nous devons intégrer des données provenant de sources très différentes :")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("• Des APIs REST publiques pour les données batch (CoinGecko)")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("• Des flux temps réel via Kafka pour la transmission de masse")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("Une fois collectées, ces données doivent être transformées, stockées dans une base relationnelle, puis rendues accessibles à travers un dashboard interactif et professionnel.")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("C'est exactement ce que nous avons construit. Et pour y parvenir, nous avons défini quatre objectifs clairs :")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("1. Un pipeline ETL complet et automatisé")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("2. Une architecture dual-engine capable de traiter à la fois de petits et grands volumes de données")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("3. Un dashboard production-ready avec des visualisations professionnelles")]
      }),

      new Paragraph({
        spacing: { after: 240 },
        children: [new TextRun("4. Un déploiement cloud 24/7 sans nécessiter de maintenance")]
      }),

      // ===== SLIDE 3: ARCHITECTURE =====
      new Paragraph({
        heading: HeadingLevel.HEADING_2,
        children: [new TextRun("SLIDE 3: ARCHITECTURE GLOBALE (1 minute 30)")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("Voici maintenant l'architecture globale de notre système. Elle est organisée en 6 couches bien définies, et c'est justement ce qui en fait sa force.")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("Commençons par le bas :")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("LA COUCHE API : Tout commence avec l'API REST publique de CoinGecko. C'est notre source principale de données pour les informations sur les cryptomonnaies. Nous utilisons aussi Apache Kafka pour simuler des flux temps réel de haute vélocité.")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("LA COUCHE INGESTION : Ici, nous avons deux chemins : un chemin batch qui appelle l'API CoinGecko toutes les 10 minutes, et un chemin streaming qui reçoit les données de Kafka en temps réel.")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("LA COUCHE STOCKAGE : Les données vont dans Neon PostgreSQL, une base de données cloud. C'est ici que tout est persisté. Nous avons plusieurs tables pour les données brutes, les données transformées, et les analyses.")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("LA COUCHE TRANSFORMATION : C'est le cœur du projet. Nous avons implémenté une architecture dual-engine : Pandas pour les petits volumes (développement rapide), et PySpark pour les gros volumes (scalabilité production). Le système choisit automatiquement le meilleur moteur selon le volume.")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("LA COUCHE ORCHESTRATION : Prefect orchestre tout le pipeline, et Railway fait tourner le code 24/7 en cloud sans qu'on ait besoin de faire quoi que ce soit.")]
      }),

      new Paragraph({
        spacing: { after: 240 },
        children: [new TextRun("LA COUCHE VISUALISATION : Enfin, Streamlit Cloud expose un dashboard interactif accessible publiquement. C'est ce que vos utilisateurs finaux verront.")]
      }),

      // ===== SLIDE 4: INGESTION BATCH =====
      new Paragraph({
        heading: HeadingLevel.HEADING_2,
        children: [new TextRun("SLIDE 4: INGESTION BATCH (45 secondes)")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("Parlons d'abord de l'ingestion batch. Toutes les 10 minutes, notre système appelle l'API CoinGecko et demande les données actuelles pour 5 cryptomonnaies : Bitcoin, Ethereum, Solana, XRP et BNB.")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("Chaque réponse nous donne le prix actuel, la capitalisation boursière, le volume d'échange 24 heures, et les variations en pourcentage.")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("CoinGecko a été choisi parce que c'est l'une des sources les plus fiables du marché, avec une API gratuite bien documentée. La fréquence de 10 minutes respecte les limites du plan gratuit (30 appels/minute) tout en maintenant une granularité suffisante pour l'analyse.")]
      }),

      new Paragraph({
        spacing: { after: 240 },
        children: [new TextRun("Vous voyez sur la droite un exemple de réponse JSON. C'est simple et structuré.")]
      }),

      // ===== SLIDE 5: INGESTION STREAMING =====
      new Paragraph({
        heading: HeadingLevel.HEADING_2,
        children: [new TextRun("SLIDE 5: INGESTION STREAMING KAFKA (45 secondes)")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("Maintenant, Kafka. C'est un système de messaging distribué, le standard industrie pour gérer des flux de données en temps réel.")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("Dans notre système, nous avons un topic Kafka appelé 'crypto_prices'. Un producteur envoie des messages toutes les 5 secondes avec le prix actuel et la variation de chaque crypto. Un consommateur reçoit ces messages et les stocke.")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("Pourquoi Kafka ? Parce que c'est l'architecture qu'on trouve dans la majorité des systèmes data en production. C'est hautement scalable et peut gérer des millions de messages par seconde. Dans notre cas, nous simulons les données, mais en production, on pourrait connecter directement aux exchanges cryptographiques via WebSocket.")]
      }),

      new Paragraph({
        spacing: { after: 240 },
        children: [new TextRun("Cela démontre l'architecture production-ready que nous avons construite.")]
      }),

      // ===== SLIDE 6: STOCKAGE =====
      new Paragraph({
        heading: HeadingLevel.HEADING_2,
        children: [new TextRun("SLIDE 6: STOCKAGE POSTGRESQL NEON (1 minute)")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("Toutes les données, qu'elles viennent de batch ou streaming, vont dans Neon PostgreSQL Cloud.")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("Nous avons plusieurs tables :")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("• 'raw_crypto_prices' : les données brutes de l'API batch")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("• 'stream_crypto_prices' : les données du flux Kafka")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("• Des tables transformées pour les moyennes horaires, les classements, les alertes")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("• Des tables spécialisées pour les analyses Spark SQL avancées")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("Pourquoi Neon ? C'est un PostgreSQL serverless gratuit, hautement disponible, avec sauvegarde automatique. Nous n'avons pas à gérer de serveur. C'est la combinaison parfaite pour un projet de démonstration, mais aussi pour la production.")]
      }),

      new Paragraph({
        spacing: { after: 240 },
        children: [new TextRun("Les données persistent ici de façon sécurisée et accessible.")]
      }),

      // ===== SLIDE 7: TRANSFORMATION =====
      new Paragraph({
        heading: HeadingLevel.HEADING_2,
        children: [new TextRun("SLIDE 7: TRANSFORMATION DUAL-ENGINE (2 minutes)")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("Voici l'innovation clé de ce projet : l'architecture dual-engine.")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("Le problème classique en data engineering : comment traiter efficacement TOUS les volumes ? Les petits volumes ? Les grands volumes ?")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("Notre réponse : deux moteurs, deux approches.")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("PANDAS : quand les données sont en nombre limité (moins de 50 000 lignes), Pandas est plus rapide, plus simple, et plus agréable à développer. C'est du Python pur, facile à déboguer.")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("PYSPARK : quand les données explosent (50 000 lignes ou plus), nous basculons sur Spark. Spark peut traiter des milliards de lignes distribuées sur un cluster. C'est production-ready.")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("Et la beauté ? Le système choisit AUTOMATIQUEMENT le bon moteur selon le volume de données. Pas de changement de code.")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("Nous avons implémenté 5 transformations :")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("1. NETTOYAGE : suppression des valeurs nulles, doublons, et anomalies")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("2. MOYENNE HORAIRE : agrégation du prix moyen, min et max par heure")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("3. CLASSEMENT JOURNALIER : rangement des cryptos par performance 24h. Là, Spark brille vraiment avec ses Window Functions.")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("4. DÉTECTION D'ALERTES : quand la variation dépasse ±5%, on génère une alerte")]
      }),

      new Paragraph({
        spacing: { after: 240 },
        children: [new TextRun("5. VOLUME JOURNALIER : somme des volumes échangés par jour")]
      }),

      // ===== SLIDE 8: ORCHESTRATION =====
      new Paragraph({
        heading: HeadingLevel.HEADING_2,
        children: [new TextRun("SLIDE 8: ORCHESTRATION PREFECT + RAILWAY (1 minute 30)")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("Comment s'assurer que tout cela tourne correctement, sans intervention humaine, 24 heures par jour ?")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("C'est le rôle de Prefect et Railway.")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("PREFECT : c'est un framework d'orchestration moderne. Nous définissons un 'flow' qui exécute les étapes dans l'ordre :")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("1. Créer les tables PostgreSQL")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("2. Appeler l'API CoinGecko")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("3. Sauvegarder les données brutes")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("4. Exécuter les transformations (Pandas ou Spark)")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("5. Générer les analyses avancées")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("6. Rafraîchir le dashboard")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("Prefect gère les dépendances, les retries automatiques en cas d'erreur, et l'logging.")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("RAILWAY : c'est la plateforme cloud qui fait tourner ce pipeline 24/7. Railway lance le code, le relance automatiquement en cas de crash, et tout cela coûte pratiquement rien : 5 dollars de crédit par mois, suffisant pour nos besoins.")]
      }),

      new Paragraph({
        spacing: { after: 240 },
        children: [new TextRun("Résultat : un pipeline qui s'exécute toutes les 10 minutes, jour et nuit, sans que nous ayons à faire quoi que ce soit.")]
      }),

      // ===== SLIDE 9: DASHBOARD =====
      new Paragraph({
        heading: HeadingLevel.HEADING_2,
        children: [new TextRun("SLIDE 9: DASHBOARD STREAMLIT (1 minute 30)")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("Alors, où finissent les données ? Dans un dashboard. Et pas n'importe quel dashboard.")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("Nous avons construit une interface avec 5 onglets différents, chacun offrant une perspective unique sur les données :")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("ONGLET 1 - VUE D'ENSEMBLE : les prix actuels en cartes colorées, un graphique montrant l'évolution, et le classement des meilleures performances du jour")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("ONGLET 2 - ANALYSE TECHNIQUE : pour les traders. Candlestick OHLC, heatmap de corrélation entre les cryptos")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("ONGLET 3 - ALERTES & VOLUME : affiche les cryptos qui ont des variations importantes, et les volumes d'échange")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("ONGLET 4 - STREAMING KAFKA : pour voir les flux temps réel, les prix s'actualisent toutes les 5 secondes")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("ONGLET 5 - SPARK ANALYTICS : analyses avancées calculées via SQL : la volatilité de chaque crypto, et sa part dans le marché global")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("Le dashboard a un design moderne avec un dark theme professionnel. C'est accessible 24/7 sur internet. Et il se rafraîchit automatiquement toutes les 60 secondes.")]
      }),

      new Paragraph({
        spacing: { after: 240 },
        children: [new TextRun("C'est du niveau production.")]
      }),

      // ===== SLIDE 10: RÉSULTATS =====
      new Paragraph({
        heading: HeadingLevel.HEADING_2,
        children: [new TextRun("SLIDE 10: RÉSULTATS ATTENDUS VS RÉALITÉ (1 minute)")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("Parlons de ce que nous avions prévu, et de ce que nous avons réellement livré.")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("ATTENDUS :", { bold: true })]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("• Un pipeline ETL complet")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("• Environ 5 000 lignes de données par jour")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("• Une architecture dual-engine Pandas/Spark")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("• 5 transformations métier")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("• Un dashboard interactif")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("• Orchestration et exécution automatiques")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("• Déploiement cloud")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("RÉALITÉ :", { bold: true })]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("• Pipeline complet et fonctionnel ✓")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("• 5 050 lignes effectivement stockées ✓")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("• Architecture dual-engine opérationnelle ✓")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("• 5 transformations + 3 analyses Spark SQL avancées ✓")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("• Dashboard version 2.1 avec 5 onglets ✓")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("• Prefect + Railway fonctionnant 24/7 ✓")]
      }),

      new Paragraph({
        spacing: { after: 240 },
        children: [new TextRun("• Neon + Streamlit Cloud en production ✓")]
      }),

      // ===== SLIDE 11: MÉTRIQUES =====
      new Paragraph({
        heading: HeadingLevel.HEADING_2,
        children: [new TextRun("SLIDE 11: DONNÉES DE SORTIE (45 secondes)")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("Voici les chiffres clés du projet :")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("• 5 cryptomonnaies suivies")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("• ~5 000 lignes collectées par jour")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("• Fréquence batch : 10 minutes")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("• Fréquence streaming : 5 secondes")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("• 8 tables PostgreSQL créées")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("• 5 transformations métier")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("• 3 analyses Spark SQL avancées")]
      }),

      new Paragraph({
        spacing: { after: 240 },
        children: [new TextRun("• Uptime : 24/7")]
      }),

      // ===== SLIDE 12: FONCTIONNALITÉS =====
      new Paragraph({
        heading: HeadingLevel.HEADING_2,
        children: [new TextRun("SLIDE 12: FONCTIONNALITÉS DÉLIVRÉES (1 minute)")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("Récapitulons ce que ce projet offre :")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("1. PIPELINE AUTOMATISÉ : Une fois lancé, il s'exécute tous les jours sans intervention humaine, 24 heures sur 24.")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("2. DUAL-ENGINE : Flex automatiquement entre Pandas (efficace) et Spark (scalable) selon le volume.")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("3. DASHBOARD INTERACTIF : Interface utilisateur moderne avec 5 onglets thématiques, dark theme professionnel.")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("4. ANALYSES AVANCÉES : Volatilité, dominance marché, corrélation entre assets. Tout calculé via SQL, pas besoin de code complexe.")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("5. CLOUD DEPLOYMENT : Entièrement hébergé. Railway para l'orchestration, Neon pour la base de données, Streamlit Cloud pour le dashboard. Tout gratuit ou quasi-gratuit.")]
      }),

      new Paragraph({
        spacing: { after: 240 },
        children: [new TextRun("6. ALERTES TEMPS RÉEL : Détection automatique des mouvements de prix importants.")]
      }),

      // ===== SLIDE 13: TECH STACK =====
      new Paragraph({
        heading: HeadingLevel.HEADING_2,
        children: [new TextRun("SLIDE 13: STACK TECHNOLOGIQUE (30 secondes)")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("Pour ceux intéressés par les détails techniques, voici les technologies utilisées :")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("INGESTION : Python, API REST CoinGecko, Kafka")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("STOCKAGE : PostgreSQL 15, Neon Cloud")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("TRANSFORMATION : Pandas 2.2.3, PySpark 3.5.1, SQL")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("ORCHESTRATION : Prefect 3.x, Railway")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("VISUALISATION : Streamlit 1.42, Plotly 5.24, Streamlit Cloud")]
      }),

      new Paragraph({
        spacing: { after: 240 },
        children: [new TextRun("INFRASTRUCTURE : Docker, Python 3.12, GitHub")]
      }),

      // ===== CONCLUSION =====
      new Paragraph({
        heading: HeadingLevel.HEADING_2,
        children: [new TextRun("SLIDE 14: CONCLUSION (1 minute 30)")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("En conclusion, ce projet démontre une maîtrise complète de la chaîne de valeur Data Engineering.")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("Nous avons ingéré des données depuis plusieurs sources. Nous les avons transformées en utilisant deux moteurs différents pour maximiser l'efficacité. Nous les avons stockées de façon sécurisée et scalable. Et nous les avons exposées par un interface moderne et accessible.")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("Ce qui est remarquable, c'est que le TOUT fonctionne automatiquement en cloud, sans intervention humaine. C'est cela, la production-ready.")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("Les compétences développées incluent :")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("• Ingestion batch et streaming")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("• Stockage relationnel cloud")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("• Transformations Pandas ET PySpark")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("• Spark SQL, Window Functions")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("• Orchestration avec Prefect")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("• Déploiement cloud (Railway, Neon, Streamlit)")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("• Visualisation interactive professionnel")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("Le dashboard est actuellement en ligne et fonctionne parfaitement. Si vous êtes intéressés, vous pouvez le visiter pour voir les données en direct.")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("")]
      }),

      new Paragraph({
        spacing: { after: 120 },
        children: [new TextRun("Merci de votre attention. Je suis prête pour vos questions.")]
      })
    ]
  }]
});

Packer.toBuffer(doc).then(buffer => {
  fs.writeFileSync("discours_presentation.docx", buffer);
  console.log("✅ Discours créé: discours_presentation.docx");
});
