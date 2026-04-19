const PptxGenJS = require("pptxgenjs");

// Initialiser la présentation
const pres = new PptxGenJS();
pres.defineLayout({ name: "LAYOUT1", width: 10, height: 7.5 });
pres.defineLayout({ name: "LAYOUT2", width: 10, height: 7.5 });

// Couleurs (Ocean Gradient palette)
const colors = {
  deepBlue: "065A82",
  teal: "1C7293",
  midnight: "21295C",
  lightBg: "F5F5F5",
  white: "FFFFFF",
  accent: "00D4FF",
  text: "333333",
};

// Helper pour titres
function addTitleSlide(pres, title, subtitle) {
  const slide = pres.addSlide();
  slide.background = { color: colors.deepBlue };

  slide.addText(title, {
    x: 0.5, y: 2.5, w: 9, h: 1.5,
    fontSize: 54, bold: true, color: colors.white,
    align: "center", fontFace: "Calibri"
  });

  slide.addText(subtitle, {
    x: 0.5, y: 4.2, w: 9, h: 1,
    fontSize: 24, color: colors.accent,
    align: "center", fontFace: "Calibri"
  });

  slide.addText("Asma Sebai | ING2 - Semestre 2 | Avril 2026", {
    x: 0.5, y: 6.8, w: 9, h: 0.5,
    fontSize: 12, color: colors.lightBg,
    align: "center", italic: true
  });
}

function addContentSlide(pres, title, content) {
  const slide = pres.addSlide();
  slide.background = { color: colors.white };

  // Titre avec barre colorée
  slide.addShape(pres.ShapeType.rect, {
    x: 0, y: 0, w: 10, h: 0.8,
    fill: { color: colors.deepBlue }
  });

  slide.addText(title, {
    x: 0.5, y: 0.15, w: 9, h: 0.5,
    fontSize: 36, bold: true, color: colors.white,
    fontFace: "Calibri"
  });

  return slide;
}

// ============ SLIDE 1: PAGE DE TITRE ============
addTitleSlide(pres, "Crypto Data Pipeline", "Architecture Data Engineering End-to-End");

// ============ SLIDE 2: PROBLÉMATIQUE ============
const slide2 = addContentSlide(pres, "Contexte et Problématique");

slide2.addText("Le Problème", {
  x: 0.5, y: 1.2, w: 4.5, h: 0.4,
  fontSize: 20, bold: true, color: colors.deepBlue,
  fontFace: "Calibri"
});

const problems = [
  "📊 Marché crypto volatil 24h/24 ($billions/jour)",
  "⚠️ Besoin d'analyse en temps réel",
  "🔄 Intégration de sources hétérogènes (API, Kafka)",
  "💾 Stockage et transformation complexe",
  "📈 Visualisation accessible aux décideurs"
];

let yPos = 1.7;
problems.forEach((problem) => {
  slide2.addText(problem, {
    x: 0.8, y: yPos, w: 8.7, h: 0.35,
    fontSize: 14, color: colors.text,
    fontFace: "Calibri"
  });
  yPos += 0.45;
});

slide2.addShape(pres.ShapeType.rect, {
  x: 5.2, y: 1.1, w: 4.3, h: 3,
  fill: { color: colors.teal },
  line: { color: colors.deepBlue, width: 2 }
});

slide2.addText("OBJECTIF", {
  x: 5.4, y: 1.3, w: 3.9, h: 0.3,
  fontSize: 16, bold: true, color: colors.white,
  align: "center", fontFace: "Calibri"
});

const objectives = [
  "✓ Pipeline ETL complet",
  "✓ Dual-Engine (Pandas/Spark)",
  "✓ Dashboard production-ready",
  "✓ Déploiement cloud 24/7",
  "✓ Analyses temps réel"
];

let objY = 1.8;
objectives.forEach((obj) => {
  slide2.addText(obj, {
    x: 5.4, y: objY, w: 3.9, h: 0.3,
    fontSize: 13, color: colors.white,
    fontFace: "Calibri"
  });
  objY += 0.35;
});

// ============ SLIDE 3: ARCHITECTURE GLOBALE ============
const slide3 = addContentSlide(pres, "Architecture Globale du Pipeline");

const layers = [
  { name: "API", y: 1.2, color: colors.accent },
  { name: "INGESTION", y: 1.9, color: colors.teal },
  { name: "STOCKAGE", y: 2.6, color: colors.deepBlue },
  { name: "TRANSFORMATION", y: 3.3, color: colors.teal },
  { name: "ORCHESTRATION", y: 4.0, color: colors.accent },
  { name: "VISUALISATION", y: 4.7, color: colors.deepBlue }
];

layers.forEach((layer) => {
  slide3.addShape(pres.ShapeType.rect, {
    x: 0.5, y: layer.y, w: 9, h: 0.5,
    fill: { color: layer.color }
  });

  slide3.addText(layer.name, {
    x: 0.7, y: layer.y + 0.08, w: 8.6, h: 0.35,
    fontSize: 14, bold: true, color: colors.white,
    align: "left", fontFace: "Calibri"
  });
});

// Flèches entre les couches
layers.slice(0, -1).forEach((_, i) => {
  slide3.addShape(pres.ShapeType.triangle, {
    x: 4.8, y: layers[i].y + 0.55, w: 0.4, h: 0.3,
    fill: { color: colors.deepBlue },
    rotate: 180
  });
});

// Descriptions
slide3.addText("CoinGecko + Kafka", {
  x: 0.7, y: 1.3, w: 2, h: 0.3,
  fontSize: 11, color: colors.text, italic: true
});

slide3.addText("Batch + Streaming", {
  x: 0.7, y: 2.0, w: 2, h: 0.3,
  fontSize: 11, color: colors.text, italic: true
});

slide3.addText("Neon PostgreSQL Cloud", {
  x: 0.7, y: 2.7, w: 2, h: 0.3,
  fontSize: 11, color: colors.text, italic: true
});

slide3.addText("Pandas + PySpark", {
  x: 0.7, y: 3.4, w: 2, h: 0.3,
  fontSize: 11, color: colors.text, italic: true
});

slide3.addText("Prefect + Railway", {
  x: 0.7, y: 4.1, w: 2, h: 0.3,
  fontSize: 11, color: colors.text, italic: true
});

slide3.addText("Streamlit Cloud", {
  x: 0.7, y: 4.8, w: 2, h: 0.3,
  fontSize: 11, color: colors.text, italic: true
});

// ============ SLIDE 4: INGESTION BATCH ============
const slide4 = addContentSlide(pres, "Étape 1: Ingestion Batch (CoinGecko API)");

const batchContent = [
  "🔗 Source: API REST publique CoinGecko",
  "⏱️ Fréquence: Toutes les 10 minutes",
  "📝 Format: JSON",
  "💾 Données: Prix, Volume, Variation 24h, Market Cap",
  "🪙 Cryptos: Bitcoin, Ethereum, Solana, XRP, BNB"
];

let bY = 1.4;
batchContent.forEach((item) => {
  slide4.addText(item, {
    x: 0.8, y: bY, w: 4.2, h: 0.35,
    fontSize: 13, color: colors.text,
    fontFace: "Calibri"
  });
  bY += 0.45;
});

// Code exemple
slide4.addShape(pres.ShapeType.rect, {
  x: 5.2, y: 1.2, w: 4.3, h: 3.8,
  fill: { color: "#F0F0F0" },
  line: { color: colors.deepBlue, width: 1 }
});

const codeExample = `GET /api/v3/coins/markets
?ids=bitcoin,ethereum
&vs_currency=usd
&order=market_cap_desc

Response:
{
  "id": "bitcoin",
  "current_price": 70970.00,
  "market_cap": 1.2T,
  "volume_24h": 29.6B,
  "price_change_24h": -2.84%
}`;

slide4.addText(codeExample, {
  x: 5.4, y: 1.4, w: 3.9, h: 3.4,
  fontSize: 9, color: colors.text,
  fontFace: "Consolas"
});

// ============ SLIDE 5: INGESTION STREAMING ============
const slide5 = addContentSlide(pres, "Étape 2: Ingestion Streaming (Kafka)");

slide5.addText("Architecture Kafka", {
  x: 0.5, y: 1.2, w: 9, h: 0.3,
  fontSize: 16, bold: true, color: colors.deepBlue,
  fontFace: "Calibri"
});

// Kafka flow diagram
const kafkaBoxes = [
  { label: "Producer", x: 0.8, color: colors.teal },
  { label: "Topic\ncrypto_prices", x: 3.5, color: colors.accent },
  { label: "Consumer", x: 6.2, color: colors.teal }
];

kafkaBoxes.forEach((box) => {
  slide5.addShape(pres.ShapeType.rect, {
    x: box.x, y: 1.8, w: 2, h: 1,
    fill: { color: box.color },
    line: { color: colors.deepBlue, width: 2 }
  });

  slide5.addText(box.label, {
    x: box.x + 0.1, y: 2.05, w: 1.8, h: 0.5,
    fontSize: 12, bold: true, color: colors.white,
    align: "center", fontFace: "Calibri"
  });
});

// Flèches
slide5.addShape(pres.ShapeType.triangle, {
  x: 2.95, y: 2.25, w: 0.4, h: 0.3,
  fill: { color: colors.deepBlue }
});

slide5.addShape(pres.ShapeType.triangle, {
  x: 5.65, y: 2.25, w: 0.4, h: 0.3,
  fill: { color: colors.deepBlue }
});

const kafkaDetails = [
  "✓ Flux temps réel 5 secondes",
  "✓ Données simulées (volumes croissants)",
  "✓ Infrastructure scalable pour production",
  "✓ Permet ajout WebSocket futures"
];

let kY = 3.2;
kafkaDetails.forEach((detail) => {
  slide5.addText(detail, {
    x: 0.8, y: kY, w: 8.2, h: 0.35,
    fontSize: 13, color: colors.text,
    fontFace: "Calibri"
  });
  kY += 0.45;
});

// ============ SLIDE 6: STOCKAGE ============
const slide6 = addContentSlide(pres, "Étape 3: Stockage (PostgreSQL Neon Cloud)");

const dbTables = [
  { name: "raw_crypto_prices", desc: "Données brutes batch" },
  { name: "stream_crypto_prices", desc: "Données streaming temps réel" },
  { name: "transform_hourly_avg", desc: "Moyennes horaires" },
  { name: "transform_daily_ranking", desc: "Classements journaliers" },
  { name: "spark_volatility", desc: "Analyses volatilité (Spark SQL)" }
];

let dbY = 1.4;
dbTables.forEach((table) => {
  slide6.addShape(pres.ShapeType.rect, {
    x: 0.8, y: dbY, w: 3.2, h: 0.35,
    fill: { color: colors.teal }
  });

  slide6.addText(table.name, {
    x: 1, y: dbY + 0.05, w: 2.8, h: 0.25,
    fontSize: 11, bold: true, color: colors.white,
    fontFace: "Calibri"
  });

  slide6.addText(table.desc, {
    x: 4.2, y: dbY + 0.05, w: 5.3, h: 0.25,
    fontSize: 11, color: colors.text,
    fontFace: "Calibri"
  });

  dbY += 0.45;
});

// Avantages Neon
slide6.addShape(pres.ShapeType.rect, {
  x: 0.8, y: 4.8, w: 8.9, h: 1.5,
  fill: { color: "#E8F4F8" },
  line: { color: colors.teal, width: 1 }
});

slide6.addText("Avantages Neon Cloud", {
  x: 1, y: 4.95, w: 8.5, h: 0.3,
  fontSize: 13, bold: true, color: colors.deepBlue,
  fontFace: "Calibri"
});

slide6.addText("✓ Gratuit  ✓ Scalable  ✓ Serverless  ✓ Haute disponibilité  ✓ Sauvegarde automatique", {
  x: 1, y: 5.35, w: 8.5, h: 0.8,
  fontSize: 11, color: colors.text,
  fontFace: "Calibri"
});

// ============ SLIDE 7: TRANSFORMATION DUAL-ENGINE ============
const slide7 = addContentSlide(pres, "Étape 4: Transformation (Dual-Engine Architecture)");

// Pandas vs Spark
const engines = [
  {
    name: "🐼 PANDAS",
    x: 0.8,
    details: ["< 50k lignes", "Développement rapide", "Efficace RAM", "Parfait dev local"]
  },
  {
    name: "⚡ PYSPARK",
    x: 5.2,
    details: ["≥ 50k lignes", "Scalable distribué", "Big Data ready", "Production-grade"]
  }
];

engines.forEach((engine) => {
  slide7.addShape(pres.ShapeType.rect, {
    x: engine.x, y: 1.3, w: 4.2, h: 0.5,
    fill: { color: engine.name.includes("PANDAS") ? colors.teal : colors.accent }
  });

  slide7.addText(engine.name, {
    x: engine.x + 0.2, y: 1.35, w: 3.8, h: 0.4,
    fontSize: 16, bold: true, color: colors.white,
    align: "center", fontFace: "Calibri"
  });

  let dY = 2;
  engine.details.forEach((detail) => {
    slide7.addText(detail, {
      x: engine.x + 0.3, y: dY, w: 3.6, h: 0.3,
      fontSize: 11, color: colors.text,
      fontFace: "Calibri"
    });
    dY += 0.4;
  });
});

// Transformations liste
slide7.addText("5 Transformations Implémentées", {
  x: 0.5, y: 4.5, w: 9, h: 0.3,
  fontSize: 14, bold: true, color: colors.deepBlue,
  fontFace: "Calibri"
});

const transformations = [
  "1️⃣ Nettoyage (suppression nulls, doublons, anomalies)",
  "2️⃣ Moyenne horaire (agrégation par crypto/heure)",
  "3️⃣ Classement journalier (Window Functions Spark)",
  "4️⃣ Détection alertes (seuils ±5%)",
  "5️⃣ Volume journalier (somme par crypto/jour)"
];

let tY = 5;
transformations.forEach((tr) => {
  slide7.addText(tr, {
    x: 0.8, y: tY, w: 8.7, h: 0.25,
    fontSize: 11, color: colors.text,
    fontFace: "Calibri"
  });
  tY += 0.3;
});

// ============ SLIDE 8: ORCHESTRATION ============
const slide8 = addContentSlide(pres, "Étape 5: Orchestration (Prefect + Railway)");

slide8.addShape(pres.ShapeType.rect, {
  x: 0.8, y: 1.3, w: 4.2, h: 4.8,
  fill: { color: "#F9F9F9" },
  line: { color: colors.teal, width: 2 }
});

slide8.addText("Prefect Flow", {
  x: 1, y: 1.5, w: 3.8, h: 0.3,
  fontSize: 14, bold: true, color: colors.deepBlue,
  fontFace: "Calibri"
});

const flowSteps = [
  "1. Créer tables PostgreSQL",
  "2. Appeler API CoinGecko",
  "3. Sauvegarder brutes",
  "4. Charger + Nettoyer",
  "5. Transformation (Pandas/Spark)",
  "6. Analyses SQL avancées",
  "7. Rafraîchir dashboard"
];

let fY = 2;
flowSteps.forEach((step) => {
  slide8.addText(step, {
    x: 1.1, y: fY, w: 3.6, h: 0.25,
    fontSize: 10, color: colors.text,
    fontFace: "Calibri"
  });
  fY += 0.35;
});

slide8.addShape(pres.ShapeType.rect, {
  x: 5.2, y: 1.3, w: 4.2, h: 4.8,
  fill: { color: "#E8F4F8" },
  line: { color: colors.accent, width: 2 }
});

slide8.addText("Railway Deployment", {
  x: 5.4, y: 1.5, w: 3.8, h: 0.3,
  fontSize: 14, bold: true, color: colors.deepBlue,
  fontFace: "Calibri"
});

const railwayPoints = [
  "✓ Pipeline tourne 24/7",
  "✓ Exécution toutes les 10min",
  "✓ Gratuit ($5/mois crédit)",
  "✓ Logs en temps réel",
  "✓ Auto-restart en erreur",
  "✓ Scalable production",
  "✓ Zéro maintenance"
];

let rY = 2;
railwayPoints.forEach((point) => {
  slide8.addText(point, {
    x: 5.4, y: rY, w: 3.8, h: 0.25,
    fontSize: 10, color: colors.text,
    fontFace: "Calibri"
  });
  rY += 0.35;
});

// ============ SLIDE 9: DASHBOARD ============
const slide9 = addContentSlide(pres, "Étape 6: Visualisation (Streamlit Cloud)");

slide9.addText("5 Onglets Interactifs", {
  x: 0.5, y: 1.2, w: 9, h: 0.3,
  fontSize: 16, bold: true, color: colors.deepBlue,
  fontFace: "Calibri"
});

const tabs = [
  { emoji: "📊", name: "Vue d'ensemble", desc: "KPI cards + Graphiques prix + Classement" },
  { emoji: "📈", name: "Analyse technique", desc: "Chandelier OHLC + Heatmap corrélation" },
  { emoji: "⚠️", name: "Alertes & Volume", desc: "Volatilité + Volume journalier" },
  { emoji: "⚡", name: "Streaming Kafka", desc: "Flux temps réel + Distribution" },
  { emoji: "🔧", name: "Spark Analytics", desc: "Volatilité + Dominance marché" }
];

let tabY = 1.7;
tabs.forEach((tab) => {
  slide9.addShape(pres.ShapeType.rect, {
    x: 0.8, y: tabY, w: 8.9, h: 0.6,
    fill: { color: colors.teal }
  });

  slide9.addText(tab.emoji + " " + tab.name, {
    x: 1, y: tabY + 0.08, w: 2.5, h: 0.45,
    fontSize: 12, bold: true, color: colors.white,
    fontFace: "Calibri"
  });

  slide9.addText(tab.desc, {
    x: 3.6, y: tabY + 0.08, w: 5.2, h: 0.45,
    fontSize: 11, color: colors.white,
    fontFace: "Calibri"
  });

  tabY += 0.75;
});

slide9.addShape(pres.ShapeType.rect, {
  x: 0.8, y: 5.8, w: 8.9, h: 0.8,
  fill: { color: colors.accent }
});

slide9.addText("🌐 Déployé sur Streamlit Cloud | 24/7 | Accessible publiquement | Auto-refresh 60s", {
  x: 1, y: 5.95, w: 8.7, h: 0.5,
  fontSize: 12, bold: true, color: colors.white,
  align: "center", fontFace: "Calibri"
});

// ============ SLIDE 10: RÉSULTATS ATTENDUS ============
const slide10 = addContentSlide(pres, "Résultats Attendus vs Réalité");

slide10.addShape(pres.ShapeType.rect, {
  x: 0.5, y: 1.2, w: 4.5, h: 5,
  fill: { color: "#E8F4F8" },
  line: { color: colors.teal, width: 2 }
});

slide10.addText("📊 ATTENDUS", {
  x: 0.7, y: 1.35, w: 4.1, h: 0.3,
  fontSize: 14, bold: true, color: colors.deepBlue,
  fontFace: "Calibri"
});

const expected = [
  "✓ Pipeline ETL",
  "✓ ~5,000 lignes/jour",
  "✓ Dual-Engine Pandas/Spark",
  "✓ 5 transformations",
  "✓ Dashboard interactive",
  "✓ Orchestration auto",
  "✓ Cloud deployment"
];

let expY = 1.8;
expected.forEach((exp) => {
  slide10.addText(exp, {
    x: 0.9, y: expY, w: 3.9, h: 0.28,
    fontSize: 11, color: colors.text,
    fontFace: "Calibri"
  });
  expY += 0.35;
});

slide10.addShape(pres.ShapeType.rect, {
  x: 5.2, y: 1.2, w: 4.3, h: 5,
  fill: { color: "#E8F8E8" },
  line: { color: colors.accent, width: 2 }
});

slide10.addText("✅ RÉALITÉ", {
  x: 5.4, y: 1.35, w: 3.9, h: 0.3,
  fontSize: 14, bold: true, color: colors.deepBlue,
  fontFace: "Calibri"
});

const reality = [
  "✅ Pipeline complet",
  "✅ 5,050 lignes stockées",
  "✅ Dual-Engine opérationnel",
  "✅ 5 transformations + 3 analyses Spark SQL",
  "✅ Dashboard 5 onglets (v2.1)",
  "✅ Prefect + Railway 24/7",
  "✅ Neon + Streamlit Cloud"
];

let realY = 1.8;
reality.forEach((r) => {
  slide10.addText(r, {
    x: 5.4, y: realY, w: 3.9, h: 0.28,
    fontSize: 11, color: colors.text,
    fontFace: "Calibri"
  });
  realY += 0.35;
});

// ============ SLIDE 11: MÉTRIQUES DE SORTIE ============
const slide11 = addContentSlide(pres, "Données de Sortie & Métriques");

const metrics = [
  { label: "Cryptos suivies", value: "5", icon: "🪙" },
  { label: "Lignes/jour", value: "~5,000", icon: "📊" },
  { label: "Fréquence batch", value: "10 min", icon: "⏱️" },
  { label: "Fréquence streaming", value: "5 sec", icon: "⚡" },
  { label: "Tables créées", value: "8", icon: "🗄️" },
  { label: "Transformations", value: "5", icon: "🔄" },
  { label: "Analyses Spark SQL", value: "3", icon: "🔧" },
  { label: "Uptime", value: "24/7", icon: "✅" }
];

let metricY = 1.3;
let col = 0;

metrics.forEach((metric, idx) => {
  const x = col === 0 ? 0.8 : 5.2;
  const y = 1.3 + (idx % 4) * 0.9;

  slide11.addShape(pres.ShapeType.rect, {
    x: x, y: y, w: 4.2, h: 0.75,
    fill: { color: colors.teal }
  });

  slide11.addText(metric.icon + " " + metric.label, {
    x: x + 0.2, y: y + 0.05, w: 3.8, h: 0.3,
    fontSize: 11, color: colors.white,
    fontFace: "Calibri"
  });

  slide11.addText(metric.value, {
    x: x + 0.2, y: y + 0.35, w: 3.8, h: 0.3,
    fontSize: 16, bold: true, color: colors.white,
    fontFace: "Calibri"
  });

  if ((idx + 1) % 4 === 0) {
    col = 1 - col;
  }
});

// ============ SLIDE 12: FONCTIONNALITÉS ============
const slide12 = addContentSlide(pres, "Fonctionnalités Délivrées");

const features = [
  { title: "Pipeline automatisé", desc: "Exécution 24/7 sans intervention", icon: "🤖" },
  { title: "Dual-Engine", desc: "Pandas pour petits volumes, Spark pour grands", icon: "⚙️" },
  { title: "Dashboard interactif", desc: "5 onglets, dark theme, responsive", icon: "📱" },
  { title: "Analyses avancées", desc: "Volatilité, dominance, corrélation", icon: "📈" },
  { title: "Cloud deployment", desc: "Railway + Neon + Streamlit (gratuit)", icon: "☁️" },
  { title: "Alertes temps réel", desc: "Seuils configurables, notifications", icon: "🔔" }
];

let featY = 1.3;
features.forEach((feat) => {
  slide12.addShape(pres.ShapeType.rect, {
    x: 0.8, y: featY, w: 0.5, h: 0.5,
    fill: { color: colors.accent }
  });

  slide12.addText(feat.icon, {
    x: 0.95, y: featY + 0.08, w: 0.2, h: 0.35,
    fontSize: 20, align: "center", fontFace: "Calibri"
  });

  slide12.addText(feat.title, {
    x: 1.5, y: featY + 0.05, w: 3.5, h: 0.25,
    fontSize: 12, bold: true, color: colors.deepBlue,
    fontFace: "Calibri"
  });

  slide12.addText(feat.desc, {
    x: 1.5, y: featY + 0.3, w: 3.5, h: 0.2,
    fontSize: 10, color: colors.text,
    fontFace: "Calibri", italic: true
  });

  featY += 0.8;
});

// ============ SLIDE 13: TECH STACK ============
const slide13 = addContentSlide(pres, "Stack Technologique");

const techStack = [
  { category: "Ingestion", techs: "Python • CoinGecko API • Kafka", y: 1.5 },
  { category: "Stockage", techs: "PostgreSQL • Neon Cloud", y: 2.3 },
  { category: "Transformation", techs: "Pandas • PySpark 3.5 • SQL", y: 3.1 },
  { category: "Orchestration", techs: "Prefect 3.x • Railway", y: 3.9 },
  { category: "Visualisation", techs: "Streamlit 1.42 • Plotly 5.24 • Streamlit Cloud", y: 4.7 },
  { category: "Infra", techs: "Docker • Python 3.12 • GitHub", y: 5.5 }
];

techStack.forEach((stack) => {
  slide13.addText(stack.category, {
    x: 0.8, y: stack.y, w: 2, h: 0.3,
    fontSize: 12, bold: true, color: colors.deepBlue,
    fontFace: "Calibri"
  });

  slide13.addShape(pres.ShapeType.rect, {
    x: 3, y: stack.y - 0.05, w: 6.7, h: 0.4,
    fill: { color: colors.teal }
  });

  slide13.addText(stack.techs, {
    x: 3.2, y: stack.y, w: 6.3, h: 0.3,
    fontSize: 11, color: colors.white,
    fontFace: "Calibri"
  });
});

// ============ SLIDE 14: CONCLUSIONS ============
addTitleSlide(pres, "Conclusion", "Pipeline Data Engineering Production-Ready");

const slideLast = pres.slides[pres.slides.length - 1];

slideLast.addText("Accomplissements", {
  x: 0.5, y: 1.5, w: 9, h: 0.3,
  fontSize: 18, bold: true, color: colors.white,
  fontFace: "Calibri"
});

const accomplishments = [
  "✅ Architecture ETL complète end-to-end",
  "✅ Dual-Engine avec sélection automatique du moteur",
  "✅ Pipeline 24/7 sur Railway (gratuit)",
  "✅ Dashboard production-ready avec 5 onglets",
  "✅ Analyses avancées via Spark SQL",
  "✅ Déploiement cloud sans maintenance"
];

let accY = 2.1;
accomplishments.forEach((acc) => {
  slideLast.addText(acc, {
    x: 0.8, y: accY, w: 8.7, h: 0.3,
    fontSize: 13, color: colors.white,
    fontFace: "Calibri"
  });
  accY += 0.45;
});

slideLast.addText("Démonstration en direct disponible", {
  x: 0.5, y: 6.2, w: 9, h: 0.4,
  fontSize: 14, italic: true, bold: true, color: colors.accent,
  align: "center", fontFace: "Calibri"
});

slideLast.addText("Dashboard Cloud: crypto-data-pipeline-8mrtpexxnwydrp9dbehvt5.streamlit.app", {
  x: 0.5, y: 6.65, w: 9, h: 0.3,
  fontSize: 11, color: colors.lightBg,
  align: "center", fontFace: "Calibri"
});

// Sauvegarder
pres.save({ path: "presentation.pptx" });
console.log("✅ Présentation créée: presentation.pptx");
