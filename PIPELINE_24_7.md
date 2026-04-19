# Garder le Pipeline 24/7 ⏰

## Le Problème
Quand vous arrêtez le pipeline local, le dashboard cloud cesse de recevoir des **NOUVELLES données**. Les **anciennes données** restent dans Neon Cloud, mais sans mise à jour continue, le dashboard n'affiche rien.

---

## Solution 1️⃣: Héberger le Pipeline en Cloud ⭐ MEILLEURE

Le pipeline tourne **24/7 gratuitement** sur une plateforme cloud.

### Option A: Railway.app (Recommandé)

1. **Inscription:**
   - Allez sur https://railway.app
   - Connectez votre GitHub

2. **Déployer le projet:**
   ```bash
   git push origin main  # Push votre repo
   ```
   Railway détecte automatiquement `requirements.txt` et lance:
   ```bash
   python run_pipeline.py --mode batch --no-kafka
   ```

3. **Configurer les variables d'environnement:**
   - Dans Railway Dashboard → Votre projet → Variables
   - Ajoutez: `DATABASE_URL` = votre URL Neon

4. **C'est tout!** ✅
   - Pipeline tourne 24/7
   - Nouvelles données envoyées à Neon automatiquement
   - Dashboard toujours à jour

**Gratuit jusqu'à 500 heures/mois**

---

### Option B: Render.com

Même chose que Railway, interface légèrement différente:
1. https://render.com
2. Nouveau "Web Service"
3. Connecter GitHub
4. Build: `pip install -r requirements.txt`
5. Start: `python run_pipeline.py --mode batch --no-kafka`
6. Gratuit 750 heures/mois

---

### Option C: PythonAnywhere (Plus facile, moins flexible)

1. https://www.pythonanywhere.com
2. Compte gratuit
3. Upload votre projet
4. Créer une tâche programmée (scheduled task)
   - Commande: `python /home/user/crypto_data_project/run_pipeline.py --mode batch --no-kafka`
   - Fréquence: Toutes les 10 minutes
5. Elle tourne automatiquement ✅

---

## Solution 2️⃣: Dashboard affiche l'historique complet

Déjà **fait pour vous!** ✅

Dans le dashboard, il y a maintenant une option: **"📊 Historique complet"**

Quand vous sélectionnez cette option:
- Le dashboard affiche **TOUTES les données** collectées
- Même si le pipeline s'arrête, vous voyez l'historique entier
- Les anciennes données restent visibles

**Pour accéder:**
1. Allez sur https://crypto-data-pipeline-8mrtpexxnwydrp9dbehvt5.streamlit.app
2. En haut à gauche, sélectionnez "Période" → **"📊 Historique complet"**
3. Tous les anciens prix s'affichent!

---

## Solution 3️⃣: Cron Job (Linux/Mac uniquement)

Si votre PC tourne 24/7:

```bash
# Éditer crontab
crontab -e

# Ajouter cette ligne (relance le pipeline toutes les 10 minutes)
*/10 * * * * cd /chemin/vers/crypto_data_project && python run_pipeline.py --mode batch --no-kafka >> logs/cron.log 2>&1
```

---

## Comparaison

| Solution | Coût | Effort | Disponibilité | Recommandé |
|----------|------|--------|---------------|-----------|
| **Railway** | Gratuit (500h/mois) | Facile (GitHub) | 24/7 ✅ | ⭐⭐⭐ |
| **Render** | Gratuit (750h/mois) | Facile | 24/7 ✅ | ⭐⭐⭐ |
| **PythonAnywhere** | Gratuit | Moyen | 24/7 ✅ | ⭐⭐ |
| **Cron Job** | Gratuit | Simple | Dépend de votre PC | ⭐ |
| **Historique complet** | Gratuit | Déjà fait! | Affiche anciennes données | ⭐⭐ |

---

## ✨ Recommandation

**Je recommande Railway.app** car:
1. ✅ Déploiement en 2 clics (GitHub)
2. ✅ Pipeline tourne 24/7 automatiquement
3. ✅ Gratuit jusqu'à 500 heures/mois (largement suffisant)
4. ✅ Logs en temps réel dans le dashboard
5. ✅ Données toujours à jour
6. ✅ Scalable si vous voulez ajouter plus de features

---

## Étapes pour Railway (5 minutes)

### 1. Créer un compte
```bash
https://railway.app → Sign up with GitHub
```

### 2. Créer un nouveau projet
```
Dashboard → New Project → Deploy from GitHub
```

### 3. Sélectionner votre repo
```
crypto-data-pipeline → Deploy
```

### 4. Ajouter DATABASE_URL
```
Variables → Add Variable
Key: DATABASE_URL
Value: postgresql://... (votre URL Neon)
```

### 5. Configurer le démarrage
```
Settings → Start Command:
python run_pipeline.py --mode batch --no-kafka
```

### Voilà! ✅
- Railway lance votre pipeline
- Il tourne 24/7
- Nouvelles données toutes les 10 minutes
- Dashboard toujours à jour 🎉

---

## Dépannage

### "Connection refused"
- Vérifiez que `DATABASE_URL` est bien définie dans Railway
- Vérifiez que votre URL Neon est correcte

### "No new data appearing"
- Vérifiez les logs: Railway Dashboard → Logs
- Assurez-vous que `run_pipeline.py` exécute en mode batch (sans Kafka)

### Pipeline s'arrête après quelques minutes
- Railway peut redémarrer les services
- Ajouter une boucle infinie avec retry dans le pipeline (optionnel)

---

## Questions?

Contactez le support Railway pour toute question sur l'hébergement! 🚀
